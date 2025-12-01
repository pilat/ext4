package ext4fs

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// parseXattrName parses an extended attribute name into its namespace index and short name.
// Extended attribute names use prefixes like "user.", "trusted.", etc.
// Returns the namespace index, the name without prefix, and any parsing error.
func parseXattrName(name string) (uint8, string, error) {
	prefixes := []struct {
		prefix string
		index  uint8
	}{
		{"user.", XattrIndexUser},
		{"security.", XattrIndexSecurity},
		{"trusted.", XattrIndexTrusted},
		{"system.posix_acl_access", XattrIndexPosixACLAccess},
		{"system.posix_acl_default", XattrIndexPosixACLDefault},
		{"system.", XattrIndexSystem},
	}

	for _, p := range prefixes {
		if strings.HasPrefix(name, p.prefix) {
			shortName := strings.TrimPrefix(name, p.prefix)
			// Special case for POSIX ACLs - name is empty
			if p.index == XattrIndexPosixACLAccess || p.index == XattrIndexPosixACLDefault {
				shortName = ""
			}
			return p.index, shortName, nil
		}
	}

	return 0, "", fmt.Errorf("unknown xattr namespace in: %s", name)
}

// xattrIndexToPrefix converts an extended attribute namespace index to its string prefix.
// Used when listing or displaying extended attribute names to users.
// Returns the full prefix including the dot (e.g., "user.", "trusted.").
func xattrIndexToPrefix(index uint8) string {
	switch index {
	case XattrIndexUser:
		return "user."
	case XattrIndexSecurity:
		return "security."
	case XattrIndexTrusted:
		return "trusted."
	case XattrIndexPosixACLAccess:
		return "system.posix_acl_access"
	case XattrIndexPosixACLDefault:
		return "system.posix_acl_default"
	case XattrIndexSystem:
		return "system."
	default:
		return fmt.Sprintf("unknown(%d).", index)
	}
}

// overwriteFile replaces the content of an existing file with new content.
// Frees the old blocks and allocates new ones as needed for the new content size.
// Updates the inode metadata while preserving the inode number.
// Returns the inode number (same as input) on success.
func (b *Builder) overwriteFile(inodeNum uint32, content []byte, mode, uid, gid uint16) (uint32, error) {
	// Read existing inode to get its blocks
	oldInode, err := b.readInode(inodeNum)
	if err != nil {
		return 0, fmt.Errorf("failed to read inode for overwrite: %w", err)
	}

	// Free the xattr block if present
	if oldInode.FileACLLo != 0 {
		if err := b.freeBlock(oldInode.FileACLLo); err != nil {
			return 0, fmt.Errorf("failed to free xattr block during overwrite: %w", err)
		}
	}

	// Free the old blocks
	oldBlocks, err := b.getInodeBlocks(oldInode)
	if err != nil {
		return 0, fmt.Errorf("failed to get old inode blocks during overwrite: %w", err)
	}
	for _, blk := range oldBlocks {
		if err := b.freeBlock(blk); err != nil {
			return 0, fmt.Errorf("failed to free old block %d during overwrite: %w", blk, err)
		}
	}

	// If the old inode had an extent tree (depth > 0), free the index blocks too
	if (oldInode.Flags & InodeFlagExtents) != 0 {
		depth := binary.LittleEndian.Uint16(oldInode.Block[6:8])
		if depth > 0 {
			entries := binary.LittleEndian.Uint16(oldInode.Block[2:4])
			for i := uint16(0); i < entries && i < 4; i++ {
				off := 12 + i*12
				leafBlock := binary.LittleEndian.Uint32(oldInode.Block[off+4:])
				if err := b.freeBlock(leafBlock); err != nil {
					return 0, fmt.Errorf("failed to free extent leaf block %d during overwrite: %w", leafBlock, err)
				}
			}
		}
	}

	size := uint64(len(content))
	blocksNeeded := uint32((size + BlockSize - 1) / BlockSize)
	if blocksNeeded == 0 {
		blocksNeeded = 1
	}

	inode := b.makeFileInode(mode, uid, gid, size)

	blocks, err := b.allocateBlocks(blocksNeeded)
	if err != nil {
		return 0, err
	}

	if blocksNeeded == 1 {
		b.setExtent(&inode, 0, blocks[0], 1)
	} else {
		if err := b.setExtentMultiple(&inode, blocks); err != nil {
			return 0, err
		}
	}
	inode.BlocksLo = blocksNeeded * (BlockSize / 512)

	for i, blk := range blocks {
		block := make([]byte, BlockSize)
		start := uint64(i) * BlockSize
		end := start + BlockSize
		if end > size {
			end = size
		}
		if start < size {
			copy(block, content[start:end])
		}
		if _, err := b.disk.WriteAt(block, int64(b.layout.BlockOffset(blk))); err != nil {
			return 0, fmt.Errorf("failed to write file block %d: %w", blk, err)
		}
	}

	if err := b.writeInode(inodeNum, &inode); err != nil {
		return 0, err
	}

	return inodeNum, nil
}

// writeXattrBlock writes extended attribute entries to a dedicated block.
// Extended attributes are stored in a special format with hash-based ordering
// for efficient lookup. The block is referenced from the inode's FileACLLo field.
func (b *Builder) writeXattrBlock(blockNum uint32, entries []XattrEntry) error {
	block := make([]byte, BlockSize)

	entriesOffset := XattrHeaderSize
	valuesEnd := BlockSize

	// Sort entries for consistent ordering (by index, then name)
	sortedEntries := make([]XattrEntry, len(entries))
	copy(sortedEntries, entries)
	for i := 0; i < len(sortedEntries)-1; i++ {
		for j := i + 1; j < len(sortedEntries); j++ {
			if sortedEntries[i].NameIndex > sortedEntries[j].NameIndex ||
				(sortedEntries[i].NameIndex == sortedEntries[j].NameIndex &&
					sortedEntries[i].Name > sortedEntries[j].Name) {
				sortedEntries[i], sortedEntries[j] = sortedEntries[j], sortedEntries[i]
			}
		}
	}

	// Collect entry hashes for block hash calculation
	entryHashes := make([]uint32, 0, len(sortedEntries))

	for _, entry := range sortedEntries {
		nameLen := len(entry.Name)
		entrySize := XattrEntryHeaderSize + nameLen
		if entrySize%4 != 0 {
			entrySize += 4 - (entrySize % 4)
		}

		valueSize := len(entry.Value)
		valueSizeAligned := valueSize
		if valueSizeAligned%4 != 0 {
			valueSizeAligned += 4 - (valueSizeAligned % 4)
		}

		// Check space: entries grow up, values grow down
		if entriesOffset+entrySize > valuesEnd-valueSizeAligned {
			return fmt.Errorf("xattr block full: cannot fit %s", entry.Name)
		}

		// Write value at end of block (growing downward)
		valuesEnd -= valueSizeAligned
		copy(block[valuesEnd:], entry.Value)

		// Calculate entry hash
		entryHash := xattrEntryHash(entry.NameIndex, entry.Name, entry.Value)
		entryHashes = append(entryHashes, entryHash)

		// Write entry header
		block[entriesOffset] = uint8(nameLen)
		block[entriesOffset+1] = entry.NameIndex
		binary.LittleEndian.PutUint16(block[entriesOffset+2:], uint16(valuesEnd))
		binary.LittleEndian.PutUint32(block[entriesOffset+4:], 0) // value_inum (unused)
		binary.LittleEndian.PutUint32(block[entriesOffset+8:], uint32(valueSize))
		binary.LittleEndian.PutUint32(block[entriesOffset+12:], entryHash)

		// Write name
		copy(block[entriesOffset+XattrEntryHeaderSize:], entry.Name)

		entriesOffset += entrySize
	}

	// Calculate block hash
	blockHash := xattrBlockHash(entryHashes)

	// Write header
	binary.LittleEndian.PutUint32(block[0:4], XattrMagic)
	binary.LittleEndian.PutUint32(block[4:8], 1)           // refcount
	binary.LittleEndian.PutUint32(block[8:12], 1)          // blocks
	binary.LittleEndian.PutUint32(block[12:16], blockHash) // hash
	// checksum at 16:20, reserved at 20:32 - leave as zero

	if _, err := b.disk.WriteAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
		return fmt.Errorf("failed to write xattr block %d: %w", blockNum, err)
	}
	return nil
}

// readXattrBlock reads extended attribute entries from a dedicated block.
// Parses the special xattr block format to extract name-value pairs.
// Returns a slice of XattrEntry structures for further processing.
func (b *Builder) readXattrBlock(blockNum uint32) ([]XattrEntry, error) {
	block := make([]byte, BlockSize)
	if _, err := b.disk.ReadAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
		return nil, fmt.Errorf("failed to read xattr block %d: %w", blockNum, err)
	}

	magic := binary.LittleEndian.Uint32(block[0:4])
	if magic != XattrMagic {
		return nil, nil
	}

	var entries []XattrEntry
	offset := XattrHeaderSize

	for offset+XattrEntryHeaderSize <= BlockSize {
		nameLen := int(block[offset])
		if nameLen == 0 {
			break
		}

		nameIndex := block[offset+1]
		valueOffs := binary.LittleEndian.Uint16(block[offset+2 : offset+4])
		valueSize := binary.LittleEndian.Uint32(block[offset+8 : offset+12])

		// entryHash at offset+12:offset+16 - we don't need it for reading

		if offset+XattrEntryHeaderSize+nameLen > BlockSize {
			break
		}

		name := string(block[offset+XattrEntryHeaderSize : offset+XattrEntryHeaderSize+nameLen])

		var value []byte
		if valueSize > 0 && int(valueOffs)+int(valueSize) <= BlockSize {
			value = make([]byte, valueSize)
			copy(value, block[valueOffs:int(valueOffs)+int(valueSize)])
		}

		entries = append(entries, XattrEntry{
			NameIndex: nameIndex,
			Name:      name,
			Value:     value,
		})

		// Entries are 4-byte aligned
		entrySize := XattrEntryHeaderSize + nameLen
		if entrySize%4 != 0 {
			entrySize += 4 - (entrySize % 4)
		}
		offset += entrySize
	}

	return entries, nil
}

// xattrEntryHash calculates a hash for an extended attribute entry.
// The hash is used for ordering entries in the xattr block for efficient lookup.
// Combines the namespace index, name, and value into a single hash value.
func xattrEntryHash(nameIndex uint8, name string, value []byte) uint32 {
	const nameHashShift = 5
	const valueHashShift = 16
	const xattrRound = 3
	const xattrPadBits = 2

	// First compute name hash
	hash := uint32(0)
	for _, c := range []byte(name) {
		hash = (hash << nameHashShift) ^ (hash >> (32 - nameHashShift)) ^ uint32(c)
	}

	// Continue (not XOR!) with value hash
	if len(value) > 0 {
		paddedLen := (len(value) + xattrRound) >> xattrPadBits
		for i := 0; i < paddedLen; i++ {
			var word uint32
			offset := i * 4
			for j := 0; j < 4 && offset+j < len(value); j++ {
				word |= uint32(value[offset+j]) << (j * 8)
			}
			hash = (hash << valueHashShift) ^ (hash >> (32 - valueHashShift)) ^ word
		}
	}

	return hash
}

// xattrBlockHash calculates a verification hash for the entire xattr block.
// Used to detect corruption or tampering of extended attribute data.
// The hash is stored in the xattr block header for integrity checking.
func xattrBlockHash(entryHashes []uint32) uint32 {
	const blockHashShift = 16

	hash := uint32(0)
	for _, entryHash := range entryHashes {
		if entryHash == 0 {
			return 0
		}
		hash = (hash << blockHashShift) ^ (hash >> (32 - blockHashShift)) ^ entryHash
	}
	return hash
}

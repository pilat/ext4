package ext4fs

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// makeDirectoryInode creates a new inode structure configured for a directory.
// Directory inodes have specific mode flags and initial link count of 2
// (accounting for "." and ".." entries). Timestamps are set to the filesystem creation time.
func (b *builder) makeDirectoryInode(mode, uid, gid uint16) inode {
	inode := inode{
		Mode:       s_IFDIR | mode,
		UID:        uid,
		GID:        gid,
		LinksCount: 2,
		Flags:      inodeFlagExtents,
		Atime:      b.layout.CreatedAt,
		Ctime:      b.layout.CreatedAt,
		Mtime:      b.layout.CreatedAt,
		Crtime:     b.layout.CreatedAt,
		ExtraIsize: 32,
	}
	b.initExtentHeader(&inode)

	return inode
}

// makeFileInode creates a new inode structure configured for a regular file.
// The inode is initialized with the specified size, ownership, and permissions.
// Regular files use extent trees for block mapping and have appropriate mode flags.
func (b *builder) makeFileInode(mode, uid, gid uint16, size uint64) inode {
	inode := inode{
		Mode:       s_IFREG | mode,
		UID:        uid,
		GID:        gid,
		SizeLo:     uint32(size & 0xFFFFFFFF),
		SizeHi:     uint32(size >> 32),
		LinksCount: 1,
		Flags:      inodeFlagExtents,
		Atime:      b.layout.CreatedAt,
		Ctime:      b.layout.CreatedAt,
		Mtime:      b.layout.CreatedAt,
		Crtime:     b.layout.CreatedAt,
		ExtraIsize: 32,
	}
	b.initExtentHeader(&inode)

	return inode
}

// initExtentHeader initializes the extent tree header in an inode's block array.
// The extent header is stored in the first 12 bytes of the inode's block field
// and contains metadata about the extent tree structure and depth.
func (b *builder) initExtentHeader(inode *inode) {
	for i := range inode.Block {
		inode.Block[i] = 0
	}

	binary.LittleEndian.PutUint16(inode.Block[0:2], extentMagic)
	binary.LittleEndian.PutUint16(inode.Block[2:4], 0)  // entries
	binary.LittleEndian.PutUint16(inode.Block[4:6], 4)  // max entries
	binary.LittleEndian.PutUint16(inode.Block[6:8], 0)  // depth
	binary.LittleEndian.PutUint32(inode.Block[8:12], 0) // generation
}

// setExtent sets a single extent mapping in an inode's extent tree.
// Maps a contiguous range of logical blocks to physical blocks on disk.
// Used for files that fit in a single extent or as part of a larger extent tree.
func (b *builder) setExtent(inode *inode, logicalBlock, physicalBlock uint32, length uint16) {
	binary.LittleEndian.PutUint16(inode.Block[2:4], 1)
	binary.LittleEndian.PutUint32(inode.Block[12:16], logicalBlock)
	binary.LittleEndian.PutUint16(inode.Block[16:18], length)
	binary.LittleEndian.PutUint16(inode.Block[18:20], 0)
	binary.LittleEndian.PutUint32(inode.Block[20:24], physicalBlock)
}

// setExtentMultiple handles allocation of non-contiguous blocks by creating multiple extents.
// For small numbers of blocks, creates individual extents. For larger allocations,
// may create an indexed extent tree structure. Blocks should be physically contiguous
// within each extent but may be sparse across extents.
func (b *builder) setExtentMultiple(inode *inode, blocks []uint32) error {
	if len(blocks) == 0 {
		return nil
	}

	// Build list of contiguous extents
	type extent struct {
		logical  uint32
		physical uint32
		length   uint16
	}

	var extents []extent

	currentExtent := extent{
		logical:  0,
		physical: blocks[0],
		length:   1,
	}

	for i := 1; i < len(blocks); i++ {
		// Check if contiguous with current extent
		if blocks[i] == currentExtent.physical+uint32(currentExtent.length) && currentExtent.length < 32768 {
			currentExtent.length++
		} else {
			extents = append(extents, currentExtent)
			currentExtent = extent{
				logical:  uint32(i),
				physical: blocks[i],
				length:   1,
			}
		}
	}

	extents = append(extents, currentExtent)

	// If fits in inode (max 4 extents), write directly
	if len(extents) <= 4 {
		binary.LittleEndian.PutUint16(inode.Block[2:4], uint16(len(extents)))

		for i, ext := range extents {
			off := 12 + i*12
			binary.LittleEndian.PutUint32(inode.Block[off:], ext.logical)
			binary.LittleEndian.PutUint16(inode.Block[off+4:], ext.length)
			binary.LittleEndian.PutUint16(inode.Block[off+6:], 0)
			binary.LittleEndian.PutUint32(inode.Block[off+8:], ext.physical)
		}

		return nil
	}

	// Need extent tree - allocate leaf block
	leafBlock, err := b.allocateBlock()
	if err != nil {
		return err
	}

	leaf := make([]byte, blockSize)

	// Write extent header for leaf
	binary.LittleEndian.PutUint16(leaf[0:2], extentMagic)
	binary.LittleEndian.PutUint16(leaf[2:4], uint16(len(extents)))
	binary.LittleEndian.PutUint16(leaf[4:6], (blockSize-12)/12) // max entries
	binary.LittleEndian.PutUint16(leaf[6:8], 0)                 // depth 0

	// Write extents to leaf
	for i, ext := range extents {
		off := 12 + i*12
		binary.LittleEndian.PutUint32(leaf[off:], ext.logical)
		binary.LittleEndian.PutUint16(leaf[off+4:], ext.length)
		binary.LittleEndian.PutUint16(leaf[off+6:], 0)
		binary.LittleEndian.PutUint32(leaf[off+8:], ext.physical)
	}

	if err := b.disk.writeAt(leaf, int64(b.layout.BlockOffset(leafBlock))); err != nil {
		return fmt.Errorf("failed to write extent leaf block: %w", err)
	}

	// Update inode to be index node
	for i := range inode.Block {
		inode.Block[i] = 0
	}

	binary.LittleEndian.PutUint16(inode.Block[0:2], extentMagic)
	binary.LittleEndian.PutUint16(inode.Block[2:4], 1) // 1 index entry
	binary.LittleEndian.PutUint16(inode.Block[4:6], 4) // max entries
	binary.LittleEndian.PutUint16(inode.Block[6:8], 1) // depth 1

	// Write index entry pointing to leaf
	binary.LittleEndian.PutUint32(inode.Block[12:16], 0)         // first logical block
	binary.LittleEndian.PutUint32(inode.Block[16:20], leafBlock) // leaf block lo
	binary.LittleEndian.PutUint16(inode.Block[20:22], 0)         // leaf block hi

	// Account for the leaf block in inode's block count
	inode.BlocksLo += blockSize / 512

	return nil
}

// writeInode writes an inode structure to its designated location in the inode table.
// Inodes are stored in inode tables within their respective block groups.
// The inode number determines which group and offset within the table to use.
func (b *builder) writeInode(inodeNum uint32, inode *inode) error {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, inode); err != nil {
		return fmt.Errorf("failed to encode inode %d: %w", inodeNum, err)
	}

	if err := b.disk.writeAt(buf.Bytes(), int64(b.layout.InodeOffset(inodeNum))); err != nil {
		return fmt.Errorf("failed to write inode %d: %w", inodeNum, err)
	}

	return nil
}

// readInode reads an inode structure from its location in the inode table.
// Returns a pointer to the inode data, which can be used for reading file metadata
// or modifying inode attributes. The inode number determines the group and offset.
func (b *builder) readInode(inodeNum uint32) (*inode, error) {
	buf := make([]byte, inodeSize)
	if err := b.disk.readAt(buf, int64(b.layout.InodeOffset(inodeNum))); err != nil {
		return nil, fmt.Errorf("failed to read inode %d: %w", inodeNum, err)
	}

	inode := &inode{}
	if err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, inode); err != nil {
		return nil, fmt.Errorf("failed to decode inode %d: %w", inodeNum, err)
	}

	return inode, nil
}

// incrementLinkCount increases the hard link count for the specified inode.
// This is called when a directory entry is added that references the inode,
// ensuring the link count accurately reflects the number of directory references.
func (b *builder) incrementLinkCount(inodeNum uint32) error {
	inode, err := b.readInode(inodeNum)
	if err != nil {
		return fmt.Errorf("failed to read inode for link count increment: %w", err)
	}

	inode.LinksCount++
	if err := b.writeInode(inodeNum, inode); err != nil {
		return fmt.Errorf("failed to write inode after incrementing link count: %w", err)
	}

	return nil
}

// decrementLinkCount decreases the hard link count for the specified inode.
// Returns the new link count after decrementing.
// This is called when a directory entry referencing the inode is removed.
func (b *builder) decrementLinkCount(inodeNum uint32) (uint16, error) {
	inode, err := b.readInode(inodeNum)
	if err != nil {
		return 0, fmt.Errorf("failed to read inode for link count decrement: %w", err)
	}

	if inode.LinksCount > 0 {
		inode.LinksCount--
	}

	if err := b.writeInode(inodeNum, inode); err != nil {
		return 0, fmt.Errorf("failed to write inode after decrementing link count: %w", err)
	}

	return inode.LinksCount, nil
}

// addBlockToInode adds a new block to a directory inode's extent tree.
// Used when a directory grows beyond its current block allocation.
// May convert from simple extents to indexed extents for large directories.
func (b *builder) addBlockToInode(inodeNum, newBlock uint32) error {
	inode, err := b.readInode(inodeNum)
	if err != nil {
		return fmt.Errorf("failed to read inode for block addition: %w", err)
	}

	entries := binary.LittleEndian.Uint16(inode.Block[2:4])
	maxEntries := binary.LittleEndian.Uint16(inode.Block[4:6])
	depth := binary.LittleEndian.Uint16(inode.Block[6:8])

	if depth != 0 {
		return b.addBlockToIndexedInode(inodeNum, newBlock)
	}

	if entries == 0 {
		binary.LittleEndian.PutUint16(inode.Block[2:4], 1)
		binary.LittleEndian.PutUint32(inode.Block[12:], 0)
		binary.LittleEndian.PutUint16(inode.Block[16:], 1)
		binary.LittleEndian.PutUint16(inode.Block[18:], 0)
		binary.LittleEndian.PutUint32(inode.Block[20:], newBlock)

		if err := b.writeInode(inodeNum, inode); err != nil {
			return fmt.Errorf("failed to write inode after initializing extent: %w", err)
		}

		return nil
	}

	lastOff := 12 + (entries-1)*12
	lastLogical := binary.LittleEndian.Uint32(inode.Block[lastOff:])
	lastLen := binary.LittleEndian.Uint16(inode.Block[lastOff+4:])
	lastStart := binary.LittleEndian.Uint32(inode.Block[lastOff+8:])

	if lastStart+uint32(lastLen) == newBlock && lastLen < 32768 {
		binary.LittleEndian.PutUint16(inode.Block[lastOff+4:], lastLen+1)

		if err := b.writeInode(inodeNum, inode); err != nil {
			return fmt.Errorf("failed to write inode after extending extent: %w", err)
		}

		return nil
	}

	if entries >= maxEntries {
		return b.convertToIndexedExtents(inodeNum, newBlock)
	}

	newOff := 12 + entries*12
	nextLogical := lastLogical + uint32(lastLen)

	binary.LittleEndian.PutUint32(inode.Block[newOff:], nextLogical)
	binary.LittleEndian.PutUint16(inode.Block[newOff+4:], 1)
	binary.LittleEndian.PutUint16(inode.Block[newOff+6:], 0)
	binary.LittleEndian.PutUint32(inode.Block[newOff+8:], newBlock)

	binary.LittleEndian.PutUint16(inode.Block[2:4], entries+1)

	if err := b.writeInode(inodeNum, inode); err != nil {
		return fmt.Errorf("failed to write inode after adding extent entry: %w", err)
	}

	return nil
}

// convertToIndexedExtents converts a simple extent inode to use indexed extents.
// Creates an extent index block to manage multiple extents efficiently.
// Required when a file or directory exceeds the capacity of inline extent storage.
func (b *builder) convertToIndexedExtents(inodeNum, newBlock uint32) error {
	inode, err := b.readInode(inodeNum)
	if err != nil {
		return fmt.Errorf("failed to read inode for extent conversion: %w", err)
	}

	entries := binary.LittleEndian.Uint16(inode.Block[2:4])

	leafBlock, err := b.allocateBlock()
	if err != nil {
		return err
	}

	leaf := make([]byte, blockSize)
	binary.LittleEndian.PutUint16(leaf[0:], extentMagic)
	binary.LittleEndian.PutUint16(leaf[2:], entries+1)
	binary.LittleEndian.PutUint16(leaf[4:], (blockSize-12)/12)
	binary.LittleEndian.PutUint16(leaf[6:], 0)

	copy(leaf[12:], inode.Block[12:12+entries*12])

	lastOff := 12 + (entries-1)*12
	lastLogical := binary.LittleEndian.Uint32(leaf[lastOff:])
	lastLen := binary.LittleEndian.Uint16(leaf[lastOff+4:])
	nextLogical := lastLogical + uint32(lastLen)

	newOff := 12 + entries*12
	binary.LittleEndian.PutUint32(leaf[newOff:], nextLogical)
	binary.LittleEndian.PutUint16(leaf[newOff+4:], 1)
	binary.LittleEndian.PutUint16(leaf[newOff+6:], 0)
	binary.LittleEndian.PutUint32(leaf[newOff+8:], newBlock)

	if err := b.disk.writeAt(leaf, int64(b.layout.BlockOffset(leafBlock))); err != nil {
		return fmt.Errorf("failed to write extent leaf block: %w", err)
	}

	for i := range inode.Block {
		inode.Block[i] = 0
	}

	binary.LittleEndian.PutUint16(inode.Block[0:], extentMagic)
	binary.LittleEndian.PutUint16(inode.Block[2:], 1)
	binary.LittleEndian.PutUint16(inode.Block[4:], 4)
	binary.LittleEndian.PutUint16(inode.Block[6:], 1)

	binary.LittleEndian.PutUint32(inode.Block[12:], 0)
	binary.LittleEndian.PutUint32(inode.Block[16:], leafBlock)
	binary.LittleEndian.PutUint16(inode.Block[20:], 0)

	inode.BlocksLo += blockSize / 512

	if err := b.writeInode(inodeNum, inode); err != nil {
		return fmt.Errorf("failed to write inode after converting to indexed extents: %w", err)
	}

	return nil
}

// addBlockToIndexedInode adds a new block to an inode that uses indexed extents.
// Updates the extent index structure to include the new extent mapping.
// Handles the complexity of maintaining sorted extent indices.
func (b *builder) addBlockToIndexedInode(inodeNum, newBlock uint32) error {
	inode, err := b.readInode(inodeNum)
	if err != nil {
		return fmt.Errorf("failed to read indexed inode: %w", err)
	}

	leafBlock := binary.LittleEndian.Uint32(inode.Block[16:])

	leaf := make([]byte, blockSize)
	if err := b.disk.readAt(leaf, int64(b.layout.BlockOffset(leafBlock))); err != nil {
		return fmt.Errorf("failed to read extent leaf block: %w", err)
	}

	entries := binary.LittleEndian.Uint16(leaf[2:4])
	maxEntries := binary.LittleEndian.Uint16(leaf[4:6])

	lastOff := 12 + (entries-1)*12
	lastLogical := binary.LittleEndian.Uint32(leaf[lastOff:])
	lastLen := binary.LittleEndian.Uint16(leaf[lastOff+4:])
	lastStart := binary.LittleEndian.Uint32(leaf[lastOff+8:])

	if lastStart+uint32(lastLen) == newBlock && lastLen < 32768 {
		binary.LittleEndian.PutUint16(leaf[lastOff+4:], lastLen+1)

		if err := b.disk.writeAt(leaf, int64(b.layout.BlockOffset(leafBlock))); err != nil {
			return fmt.Errorf("failed to write updated extent leaf: %w", err)
		}

		return nil
	}

	if entries >= maxEntries {
		return fmt.Errorf("extent tree depth > 1 not implemented")
	}

	nextLogical := lastLogical + uint32(lastLen)
	newOff := 12 + entries*12

	binary.LittleEndian.PutUint32(leaf[newOff:], nextLogical)
	binary.LittleEndian.PutUint16(leaf[newOff+4:], 1)
	binary.LittleEndian.PutUint16(leaf[newOff+6:], 0)
	binary.LittleEndian.PutUint32(leaf[newOff+8:], newBlock)

	binary.LittleEndian.PutUint16(leaf[2:4], entries+1)

	if err := b.disk.writeAt(leaf, int64(b.layout.BlockOffset(leafBlock))); err != nil {
		return fmt.Errorf("failed to write new extent leaf: %w", err)
	}

	return nil
}

// getInodeBlocks extracts all block numbers referenced by an inode's extent tree.
// Parses the extent structures to return a complete list of data blocks allocated
// to the file or directory. Supports both simple extents and complex extent trees.
func (b *builder) getInodeBlocks(inode *inode) ([]uint32, error) {
	if (inode.Flags & inodeFlagExtents) == 0 {
		return nil, nil
	}

	entries := binary.LittleEndian.Uint16(inode.Block[2:4])
	depth := binary.LittleEndian.Uint16(inode.Block[6:8])

	if entries == 0 {
		return nil, nil
	}

	var blocks []uint32

	if depth == 0 {
		for i := uint16(0); i < entries && i < 4; i++ {
			off := 12 + i*12
			length := binary.LittleEndian.Uint16(inode.Block[off+4:])
			startLo := binary.LittleEndian.Uint32(inode.Block[off+8:])

			for j := uint16(0); j < length; j++ {
				blocks = append(blocks, startLo+uint32(j))
			}
		}
	} else {
		for i := uint16(0); i < entries && i < 4; i++ {
			off := 12 + i*12
			leafBlock := binary.LittleEndian.Uint32(inode.Block[off+4:])

			leafData := make([]byte, blockSize)
			if err := b.disk.readAt(leafData, int64(b.layout.BlockOffset(leafBlock))); err != nil {
				return nil, fmt.Errorf("failed to read extent leaf block %d: %w", leafBlock, err)
			}

			leafEntries := binary.LittleEndian.Uint16(leafData[2:4])
			for j := uint16(0); j < leafEntries; j++ {
				extOff := 12 + j*12
				length := binary.LittleEndian.Uint16(leafData[extOff+4:])
				startLo := binary.LittleEndian.Uint32(leafData[extOff+8:])

				for k := uint16(0); k < length; k++ {
					blocks = append(blocks, startLo+uint32(k))
				}
			}
		}
	}

	return blocks, nil
}

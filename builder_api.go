// Package ext4fs provides a pure Go implementation for creating ext4 filesystem images.
// It allows building ext4 filesystems programmatically without external dependencies,
// suitable for creating disk images for virtual machines, containers, or embedded systems.
//
// The main entry point is Ext4ImageBuilder in disk.go, which provides a high-level API
// for creating and managing ext4 images. The internal Builder handles the low-level
// filesystem construction details.
package ext4fs

import (
	"encoding/binary"
	"fmt"
)

// ============================================================================
// Public API
// ============================================================================

// CreateDirectory creates a new directory with the specified name under the given parent directory.
// It allocates a new inode and data block, initializes the directory with "." and ".." entries,
// and adds the new directory entry to the parent. Returns the inode number of the created directory.
func (b *Builder) CreateDirectory(parentInode uint32, name string, mode, uid, gid uint16) (uint32, error) {
	if err := validateName(name); err != nil {
		return 0, fmt.Errorf("invalid directory name: %w", err)
	}

	inodeNum, err := b.allocateInode()
	if err != nil {
		return 0, err
	}

	dataBlock, err := b.allocateBlock()
	if err != nil {
		return 0, err
	}

	inode := b.makeDirectoryInode(mode, uid, gid)
	inode.LinksCount = 2
	inode.SizeLo = BlockSize
	inode.BlocksLo = BlockSize / 512
	b.setExtent(&inode, 0, dataBlock, 1)

	if err := b.writeInode(inodeNum, &inode); err != nil {
		return 0, err
	}

	entries := []DirEntry{
		{Inode: inodeNum, Type: FTDir, Name: []byte(".")},
		{Inode: parentInode, Type: FTDir, Name: []byte("..")},
	}
	if err := b.writeDirBlock(dataBlock, entries); err != nil {
		return 0, err
	}

	if err := b.addDirEntry(parentInode, DirEntry{
		Inode: inodeNum,
		Type:  FTDir,
		Name:  []byte(name),
	}); err != nil {
		return 0, err
	}

	if err := b.incrementLinkCount(parentInode); err != nil {
		return 0, err
	}

	// Track directory in correct group
	group := (inodeNum - 1) / InodesPerGroup
	b.usedDirsPerGroup[group]++

	if b.debug {
		fmt.Printf("✓ Created directory: %s (inode %d)\n", name, inodeNum)
	}

	return inodeNum, nil
}

// CreateFile creates a new regular file with the specified content under the given parent directory.
// If a file with the same name already exists, it overwrites the existing file.
// The file content is written across one or more allocated blocks using extent mapping.
// Returns the inode number of the created or overwritten file.
func (b *Builder) CreateFile(parentInode uint32, name string, content []byte, mode, uid, gid uint16) (uint32, error) {
	if err := validateName(name); err != nil {
		return 0, fmt.Errorf("invalid file name: %w", err)
	}

	existingInode, err := b.findEntry(parentInode, name)
	if err != nil {
		return 0, fmt.Errorf("failed to check for existing file: %w", err)
	}
	if existingInode != 0 {
		return b.overwriteFile(existingInode, content, mode, uid, gid)
	}

	inodeNum, err := b.allocateInode()
	if err != nil {
		return 0, err
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

	// Write content
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

	if err := b.addDirEntry(parentInode, DirEntry{
		Inode: inodeNum,
		Type:  FTRegFile,
		Name:  []byte(name),
	}); err != nil {
		return 0, err
	}

	if b.debug {
		fmt.Printf("✓ Created file: %s (inode %d, size %d)\n", name, inodeNum, size)
	}

	return inodeNum, nil
}

// CreateSymlink creates a symbolic link pointing to the specified target path.
// For targets <= 60 bytes, the target is stored directly in the inode's block array (fast symlink).
// For longer targets, a separate data block is allocated to store the target path.
// Returns the inode number of the created symlink.
func (b *Builder) CreateSymlink(parentInode uint32, name, target string, uid, gid uint16) (uint32, error) {
	if err := validateName(name); err != nil {
		return 0, fmt.Errorf("invalid symlink name: %w", err)
	}

	if len(target) == 0 {
		return 0, fmt.Errorf("symlink target cannot be empty")
	}

	if len(target) > 4096 {
		return 0, fmt.Errorf("symlink target too long: %d > 4096", len(target))
	}

	inodeNum, err := b.allocateInode()
	if err != nil {
		return 0, err
	}

	inode := Inode{
		Mode:       S_IFLNK | 0777,
		UID:        uid,
		GID:        gid,
		SizeLo:     uint32(len(target)),
		LinksCount: 1,
		Atime:      b.layout.CreatedAt,
		Ctime:      b.layout.CreatedAt,
		Mtime:      b.layout.CreatedAt,
		Crtime:     b.layout.CreatedAt,
		ExtraIsize: 32,
	}

	// Fast symlink: target stored in inode.Block (up to 60 bytes)
	if len(target) <= 60 {
		copy(inode.Block[:], target)
		inode.Flags = 0
		inode.BlocksLo = 0
	} else {
		inode.Flags = InodeFlagExtents
		dataBlock, err := b.allocateBlock()
		if err != nil {
			return 0, err
		}
		b.initExtentHeader(&inode)
		b.setExtent(&inode, 0, dataBlock, 1)
		inode.BlocksLo = BlockSize / 512

		block := make([]byte, BlockSize)
		copy(block, target)
		if _, err := b.disk.WriteAt(block, int64(b.layout.BlockOffset(dataBlock))); err != nil {
			return 0, fmt.Errorf("failed to write symlink target block: %w", err)
		}
	}

	if err := b.writeInode(inodeNum, &inode); err != nil {
		return 0, err
	}

	if err := b.addDirEntry(parentInode, DirEntry{
		Inode: inodeNum,
		Type:  FTSymlink,
		Name:  []byte(name),
	}); err != nil {
		return 0, err
	}

	if b.debug {
		fmt.Printf("✓ Created symlink: %s -> %s\n", name, target)
	}

	return inodeNum, nil
}

// SetXattr sets an extended attribute (xattr) on the specified inode.
// Extended attributes are name-value pairs that provide additional metadata
// beyond standard file attributes. Names use namespace prefixes like "user.",
// "trusted.", "security.", or "system.". If the xattr already exists, its value is updated.
func (b *Builder) SetXattr(inodeNum uint32, name string, value []byte) error {
	if len(name) == 0 {
		return fmt.Errorf("xattr name cannot be empty")
	}

	nameIndex, shortName, err := parseXattrName(name)
	if err != nil {
		return err
	}

	if len(shortName) > 255 {
		return fmt.Errorf("xattr name too long: %d > 255", len(shortName))
	}

	inode, err := b.readInode(inodeNum)
	if err != nil {
		return fmt.Errorf("failed to read inode for xattr: %w", err)
	}

	var xattrBlock uint32
	var entries []XattrEntry

	if inode.FileACLLo != 0 {
		xattrBlock = inode.FileACLLo
		var err error
		entries, err = b.readXattrBlock(xattrBlock)
		if err != nil {
			return fmt.Errorf("failed to read existing xattr block: %w", err)
		}
	} else {
		var err error
		xattrBlock, err = b.allocateBlock()
		if err != nil {
			return err
		}
		inode.FileACLLo = xattrBlock
		inode.BlocksLo += BlockSize / 512
	}

	// Update existing or add new entry
	found := false
	for i, e := range entries {
		if e.NameIndex == nameIndex && e.Name == shortName {
			entries[i].Value = value
			found = true
			break
		}
	}
	if !found {
		entries = append(entries, XattrEntry{
			NameIndex: nameIndex,
			Name:      shortName,
			Value:     value,
		})
	}

	if err := b.writeXattrBlock(xattrBlock, entries); err != nil {
		return err
	}

	if err := b.writeInode(inodeNum, inode); err != nil {
		return fmt.Errorf("failed to write inode after xattr update: %w", err)
	}

	if b.debug {
		fmt.Printf("✓ Set xattr %s on inode %d (%d bytes)\n", name, inodeNum, len(value))
	}

	return nil
}

// GetXattr retrieves the value of an extended attribute from the specified inode.
// Returns the attribute value as a byte slice, or an error if the attribute doesn't exist.
// This method is primarily used for testing and validation purposes.
func (b *Builder) GetXattr(inodeNum uint32, name string) ([]byte, error) {
	nameIndex, shortName, err := parseXattrName(name)
	if err != nil {
		return nil, err
	}

	inode, err := b.readInode(inodeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read inode for xattr: %w", err)
	}
	if inode.FileACLLo == 0 {
		return nil, fmt.Errorf("no xattrs on inode %d", inodeNum)
	}

	entries, err := b.readXattrBlock(inode.FileACLLo)
	if err != nil {
		return nil, fmt.Errorf("failed to read xattr block: %w", err)
	}
	for _, e := range entries {
		if e.NameIndex == nameIndex && e.Name == shortName {
			return e.Value, nil
		}
	}

	return nil, fmt.Errorf("xattr %s not found", name)
}

// ListXattrs returns a list of all extended attribute names associated with the specified inode.
// The returned names include their namespace prefixes (e.g., "user.attr", "trusted.security").
// Returns an empty slice if the inode has no extended attributes.
func (b *Builder) ListXattrs(inodeNum uint32) ([]string, error) {
	inode, err := b.readInode(inodeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read inode for xattr listing: %w", err)
	}
	if inode.FileACLLo == 0 {
		return nil, nil
	}

	entries, err := b.readXattrBlock(inode.FileACLLo)
	if err != nil {
		return nil, fmt.Errorf("failed to read xattr block: %w", err)
	}
	names := make([]string, 0, len(entries))

	for _, e := range entries {
		prefix := xattrIndexToPrefix(e.NameIndex)
		names = append(names, prefix+e.Name)
	}

	return names, nil
}

// RemoveXattr removes an extended attribute from the specified inode.
// If the attribute doesn't exist, no error is returned (idempotent operation).
// The xattr block may be deallocated if it becomes empty after removal.
func (b *Builder) RemoveXattr(inodeNum uint32, name string) error {
	nameIndex, shortName, err := parseXattrName(name)
	if err != nil {
		return err
	}

	inode, err := b.readInode(inodeNum)
	if err != nil {
		return fmt.Errorf("failed to read inode for xattr removal: %w", err)
	}
	if inode.FileACLLo == 0 {
		return fmt.Errorf("no xattrs on inode %d", inodeNum)
	}

	entries, err := b.readXattrBlock(inode.FileACLLo)
	if err != nil {
		return fmt.Errorf("failed to read xattr block: %w", err)
	}
	newEntries := make([]XattrEntry, 0, len(entries))
	found := false

	for _, e := range entries {
		if e.NameIndex == nameIndex && e.Name == shortName {
			found = true
			continue
		}
		newEntries = append(newEntries, e)
	}

	if !found {
		return fmt.Errorf("xattr %s not found", name)
	}

	if len(newEntries) == 0 {
		// Free the xattr block
		if err := b.freeBlock(inode.FileACLLo); err != nil {
			return fmt.Errorf("failed to free xattr block during removal: %w", err)
		}
		inode.FileACLLo = 0
		inode.BlocksLo -= BlockSize / 512
	} else {
		if err := b.writeXattrBlock(inode.FileACLLo, newEntries); err != nil {
			return err
		}
	}

	if err := b.writeInode(inodeNum, inode); err != nil {
		return fmt.Errorf("failed to write inode after xattr removal: %w", err)
	}
	return nil
}

// FinalizeMetadata updates all filesystem metadata to reflect the final state.
// This includes recalculating block and inode usage statistics per group,
// updating group descriptors with accurate counts, and ensuring the superblock
// reflects the current filesystem state. Must be called after all file operations.
func (b *Builder) FinalizeMetadata() error {
	// Calculate per-group statistics
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		// Count used blocks in this group (accounting for freed blocks)
		usedBlocks := b.nextBlockPerGroup[g] - gl.GroupStart - b.freedBlocksPerGroup[g]
		freeBlocks := gl.BlocksInGroup - usedBlocks

		// Calculate inode usage for this group
		groupStartInode := g*InodesPerGroup + 1
		groupEndInode := groupStartInode + InodesPerGroup

		var usedInodes uint16
		var highestUsedInode uint32

		if b.nextInode > groupStartInode {
			if b.nextInode >= groupEndInode {
				usedInodes = uint16(InodesPerGroup)
				highestUsedInode = InodesPerGroup
			} else {
				usedInodes = uint16(b.nextInode - groupStartInode)
				highestUsedInode = b.nextInode - groupStartInode
			}
		}

		// For group 0, account for reserved inodes
		if g == 0 {
			if highestUsedInode < FirstNonResInode-1 {
				highestUsedInode = FirstNonResInode - 1
			}
			if usedInodes < uint16(FirstNonResInode-1) {
				usedInodes = uint16(FirstNonResInode - 1)
			}
		}

		freeInodes := uint16(InodesPerGroup) - usedInodes
		// ItableUnused = inodes from highest used to end of table
		itableUnused := uint16(InodesPerGroup - highestUsedInode)

		// Read current group descriptor
		gdOffset := b.layout.BlockOffset(b.layout.GetGroupLayout(0).GDTStart) + uint64(g*32)
		gdBuf := make([]byte, 32)
		if _, err := b.disk.ReadAt(gdBuf, int64(gdOffset)); err != nil {
			return fmt.Errorf("failed to read group descriptor for group %d: %w", g, err)
		}

		// Update fields
		binary.LittleEndian.PutUint16(gdBuf[12:14], uint16(freeBlocks))
		binary.LittleEndian.PutUint16(gdBuf[14:16], freeInodes)
		// UsedDirsCount for THIS group
		binary.LittleEndian.PutUint16(gdBuf[16:18], b.usedDirsPerGroup[g])
		// Flags at offset 18 - don't set BGInodeZeroed without metadata_csum
		binary.LittleEndian.PutUint16(gdBuf[18:20], 0)
		// ItableUnusedLo at offset 28
		binary.LittleEndian.PutUint16(gdBuf[28:30], itableUnused)

		if _, err := b.disk.WriteAt(gdBuf, int64(gdOffset)); err != nil {
			return fmt.Errorf("failed to write group descriptor for group %d: %w", g, err)
		}

		// Update backup GDTs
		for bg := uint32(1); bg < b.layout.GroupCount; bg++ {
			if isSparseGroup(bg) {
				backupGl := b.layout.GetGroupLayout(bg)
				backupOffset := b.layout.BlockOffset(backupGl.GDTStart) + uint64(g*32)
				if _, err := b.disk.WriteAt(gdBuf, int64(backupOffset)); err != nil {
					return fmt.Errorf("failed to write backup group descriptor for group %d: %w", bg, err)
				}
			}
		}
	}

	// Calculate totals for superblock
	var totalFreeBlocks uint32
	var totalFreeInodes uint32
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		usedBlocks := b.nextBlockPerGroup[g] - gl.GroupStart - b.freedBlocksPerGroup[g]
		totalFreeBlocks += gl.BlocksInGroup - usedBlocks
	}
	totalFreeInodes = b.layout.TotalInodes() - (b.nextInode - 1)

	// Update primary superblock
	sbOffset := b.layout.PartitionStart + SuperblockOffset
	sbBuf := make([]byte, 1024)
	if _, err := b.disk.ReadAt(sbBuf, int64(sbOffset)); err != nil {
		return fmt.Errorf("failed to read primary superblock: %w", err)
	}

	binary.LittleEndian.PutUint32(sbBuf[0x0C:0x10], totalFreeBlocks)
	binary.LittleEndian.PutUint32(sbBuf[0x10:0x14], totalFreeInodes)

	if _, err := b.disk.WriteAt(sbBuf, int64(sbOffset)); err != nil {
		return fmt.Errorf("failed to write primary superblock: %w", err)
	}

	// Update backup superblocks
	for g := uint32(1); g < b.layout.GroupCount; g++ {
		if isSparseGroup(g) {
			gl := b.layout.GetGroupLayout(g)
			backupSbOffset := b.layout.BlockOffset(gl.SuperblockBlock)
			if _, err := b.disk.ReadAt(sbBuf, int64(backupSbOffset)); err != nil {
				return fmt.Errorf("failed to read backup superblock for group %d: %w", g, err)
			}
			binary.LittleEndian.PutUint32(sbBuf[0x0C:0x10], totalFreeBlocks)
			binary.LittleEndian.PutUint32(sbBuf[0x10:0x14], totalFreeInodes)
			if _, err := b.disk.WriteAt(sbBuf, int64(backupSbOffset)); err != nil {
				return fmt.Errorf("failed to write backup superblock for group %d: %w", g, err)
			}
		}
	}

	if b.debug {
		fmt.Printf("✓ Metadata finalized: %d free blocks, %d free inodes\n",
			totalFreeBlocks, totalFreeInodes)
	}
	return nil
}

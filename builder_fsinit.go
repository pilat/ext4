package ext4fs

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// writeSuperblock writes the ext4 superblock to offset 1024 bytes on disk.
// The superblock contains global filesystem parameters including block size,
// inode count, feature flags, and creation timestamp. It serves as the
// filesystem's "header" containing essential metadata.
func (b *builder) writeSuperblock() error {
	sb := superblock{
		Magic:             ext4Magic,
		InodesCount:       b.layout.TotalInodes(),
		BlocksCountLo:     b.layout.TotalBlocks,
		FreeBlocksCountLo: b.layout.TotalFreeBlocks(),
		FreeInodesCount:   b.layout.TotalInodes() - (firstNonResInode - 1),
		FirstDataBlock:    0,
		LogBlockSize:      blockSizeLog,
		LogClusterSize:    blockSizeLog,
		BlocksPerGroup:    blocksPerGroup,
		ClustersPerGroup:  blocksPerGroup,
		InodesPerGroup:    inodesPerGroup,
		WTime:             b.layout.CreatedAt,
		MaxMntCount:       0xFFFF,
		State:             1,
		Errors:            1,
		LastCheck:         b.layout.CreatedAt,
		CreatorOS:         0,
		RevLevel:          1,
		FirstInode:        firstNonResInode,
		InodeSize:         inodeSize,
		BlockGroupNr:      0,
		FeatureCompat:     compatExtAttr,
		FeatureIncompat:   incompatFileType | incompatExtents,
		FeatureROCompat:   roCompatSparseSuper | roCompatLargeFile | roCompatExtraIsize,
		MkfsTime:          b.layout.CreatedAt,
		DescSize:          32,
		MinExtraIsize:     32,
		WantExtraIsize:    32,
		DefHashVersion:    1,
		RBlocksCountLo:    b.layout.TotalBlocks / 20,
	}

	// Generate RFC 4122 version 4 UUID (random)
	// Using timestamp and counter as entropy source
	seed := uint64(b.layout.CreatedAt) * 1099511628211
	for i := 0; i < 16; i++ {
		seed = seed*6364136223846793005 + 1442695040888963407 // LCG
		sb.UUID[i] = byte(seed >> 56)
	}
	// Set version (4) and variant (RFC 4122)
	sb.UUID[6] = (sb.UUID[6] & 0x0F) | 0x40 // Version 4
	sb.UUID[8] = (sb.UUID[8] & 0x3F) | 0x80 // Variant RFC 4122

	copy(sb.VolumeName[:], "ext4-go")

	for i := 0; i < 4; i++ {
		sb.HashSeed[i] = b.layout.CreatedAt + uint32(i*0x12345678)
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, sb); err != nil {
		return fmt.Errorf("failed to encode superblock: %w", err)
	}

	// Write primary superblock at byte 1024
	if err := b.disk.writeAt(buf.Bytes(), int64(b.layout.PartitionStart+superblockOffset)); err != nil {
		return fmt.Errorf("failed to write primary superblock: %w", err)
	}

	// Write backup superblocks in sparse groups
	for g := uint32(1); g < b.layout.GroupCount; g++ {
		if isSparseGroup(g) {
			gl := b.layout.GetGroupLayout(g)
			sb.BlockGroupNr = uint16(g)

			buf.Reset()

			if err := binary.Write(&buf, binary.LittleEndian, sb); err != nil {
				return fmt.Errorf("failed to encode backup superblock for group %d: %w", g, err)
			}
			// Superblock is at byte 0 of the block, not byte 1024
			if err := b.disk.writeAt(buf.Bytes(), int64(b.layout.BlockOffset(gl.SuperblockBlock))); err != nil {
				return fmt.Errorf("failed to write backup superblock for group %d: %w", g, err)
			}
		}
	}

	if b.debug {
		fmt.Printf("✓ Superblock written (groups: %d, blocks: %d)\n",
			b.layout.GroupCount, b.layout.TotalBlocks)
	}

	return nil
}

// writeGroupDescriptors writes the group descriptor table (GDT) after the superblock.
// Each group descriptor (32 bytes) contains metadata for its block group including
// locations of bitmaps, inode tables, and usage statistics. The GDT enables
// efficient parallel operations across multiple block groups.
func (b *builder) writeGroupDescriptors() error {
	gdt := make([]byte, b.layout.GroupCount*32)

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		freeBlocks := gl.BlocksInGroup - gl.OverheadBlocks

		freeInodes := uint16(inodesPerGroup)
		if g == 0 {
			freeInodes = uint16(inodesPerGroup - (firstNonResInode - 1))
		}

		gd := groupDesc32{
			BlockBitmapLo:     gl.BlockBitmapBlock,
			InodeBitmapLo:     gl.InodeBitmapBlock,
			InodeTableLo:      gl.InodeTableStart,
			FreeBlocksCountLo: uint16(freeBlocks),
			FreeInodesCountLo: freeInodes,
			UsedDirsCountLo:   0,
			Flags:             0, // Don't set BGInodeZeroed without metadata_csum
			ItableUnusedLo:    freeInodes,
		}

		var buf bytes.Buffer
		if err := binary.Write(&buf, binary.LittleEndian, gd); err != nil {
			return fmt.Errorf("failed to encode group descriptor for group %d: %w", g, err)
		}

		copy(gdt[g*32:], buf.Bytes())
	}

	gl0 := b.layout.GetGroupLayout(0)
	if err := b.disk.writeAt(gdt, int64(b.layout.BlockOffset(gl0.GDTStart))); err != nil {
		return fmt.Errorf("failed to write primary group descriptors: %w", err)
	}

	for g := uint32(1); g < b.layout.GroupCount; g++ {
		if isSparseGroup(g) {
			gl := b.layout.GetGroupLayout(g)
			if err := b.disk.writeAt(gdt, int64(b.layout.BlockOffset(gl.GDTStart))); err != nil {
				return fmt.Errorf("failed to write backup group descriptors for group %d: %w", g, err)
			}
		}
	}

	if b.debug {
		fmt.Printf("✓ Group descriptors written (%d groups)\n", b.layout.GroupCount)
	}

	return nil
}

// initBitmaps initializes the block and inode bitmaps for all block groups.
// Block bitmaps track which blocks are allocated, while inode bitmaps track
// which inodes are in use. Reserved inodes (1-10) are marked as used during initialization.
func (b *builder) initBitmaps() error {
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		// Block bitmap
		blockBitmap := make([]byte, blockSize)

		// Mark overhead blocks as used
		for i := uint32(0); i < gl.OverheadBlocks; i++ {
			blockBitmap[i/8] |= 1 << (i % 8)
		}

		// Mark blocks beyond this group's range as used
		for i := gl.BlocksInGroup; i < blocksPerGroup; i++ {
			blockBitmap[i/8] |= 1 << (i % 8)
		}

		if err := b.disk.writeAt(blockBitmap, int64(b.layout.BlockOffset(gl.BlockBitmapBlock))); err != nil {
			return fmt.Errorf("failed to write block bitmap for group %d: %w", g, err)
		}

		// Inode bitmap
		inodeBitmap := make([]byte, blockSize)

		// Mark reserved inodes in group 0
		if g == 0 {
			for i := uint32(0); i < firstNonResInode-1; i++ {
				inodeBitmap[i/8] |= 1 << (i % 8)
			}
		}

		// Mark unused bits at end
		usedBytes := (inodesPerGroup + 7) / 8
		for i := usedBytes; i < blockSize; i++ {
			inodeBitmap[i] = 0xFF
		}

		if inodesPerGroup%8 != 0 {
			lastByte := usedBytes - 1
			for bit := inodesPerGroup % 8; bit < 8; bit++ {
				inodeBitmap[lastByte] |= 1 << bit
			}
		}

		if err := b.disk.writeAt(inodeBitmap, int64(b.layout.BlockOffset(gl.InodeBitmapBlock))); err != nil {
			return fmt.Errorf("failed to write inode bitmap for group %d: %w", g, err)
		}
	}

	if b.debug {
		fmt.Printf("✓ Bitmaps initialized\n")
	}

	return nil
}

// zeroInodeTables initializes all inode table blocks to zero.
// Inode tables store the actual inode structures for each block group.
// Zeroing ensures no garbage data remains from previous filesystem states.
func (b *builder) zeroInodeTables() error {
	zeroBlock := make([]byte, blockSize)

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		for i := uint32(0); i < b.layout.InodeTableBlocks; i++ {
			if err := b.disk.writeAt(zeroBlock, int64(b.layout.BlockOffset(gl.InodeTableStart+i))); err != nil {
				return fmt.Errorf("failed to zero inode table block %d in group %d: %w", i, g, err)
			}
		}
	}

	if b.debug {
		fmt.Printf("✓ Inode tables zeroed\n")
	}

	return nil
}

// createRootDirectory creates the root directory (inode 2) with essential entries.
// The root directory contains "." and ".." entries pointing to itself, and serves
// as the mount point for the filesystem. It is allocated inode 2 by convention.
func (b *builder) createRootDirectory() error {
	dataBlock, err := b.allocateBlock()
	if err != nil {
		return fmt.Errorf("failed to allocate block for root directory: %w", err)
	}

	inode := b.makeDirectoryInode(0755, 0, 0)
	inode.LinksCount = 2
	inode.SizeLo = blockSize
	inode.BlocksLo = blockSize / 512
	b.setExtent(&inode, 0, dataBlock, 1)

	if err := b.writeInode(RootInode, &inode); err != nil {
		return fmt.Errorf("failed to write root inode: %w", err)
	}

	if err := b.markInodeUsed(RootInode); err != nil {
		return fmt.Errorf("failed to mark root inode as used: %w", err)
	}

	entries := []dirEntry{
		{Inode: RootInode, Type: ftDir, Name: []byte(".")},
		{Inode: RootInode, Type: ftDir, Name: []byte("..")},
	}
	if err := b.writeDirBlock(dataBlock, entries); err != nil {
		return fmt.Errorf("failed to write root directory block: %w", err)
	}

	// Root inode is always in group 0
	b.usedDirsPerGroup[0]++

	if b.debug {
		fmt.Printf("✓ Root directory created\n")
	}

	return nil
}

// createLostFound creates the lost+found directory required by ext4 filesystem standard.
// This directory is used by fsck and other utilities to store orphaned files
// and directories found during filesystem recovery operations.
func (b *builder) createLostFound() error {
	inodeNum, err := b.allocateInode()
	if err != nil {
		return fmt.Errorf("failed to allocate inode for lost+found: %w", err)
	}

	dataBlock, err := b.allocateBlock()
	if err != nil {
		return fmt.Errorf("failed to allocate block for lost+found: %w", err)
	}

	inode := b.makeDirectoryInode(0700, 0, 0)
	inode.LinksCount = 2
	inode.SizeLo = blockSize
	inode.BlocksLo = blockSize / 512
	b.setExtent(&inode, 0, dataBlock, 1)

	if err := b.writeInode(inodeNum, &inode); err != nil {
		return fmt.Errorf("failed to write lost+found inode: %w", err)
	}

	entries := []dirEntry{
		{Inode: inodeNum, Type: ftDir, Name: []byte(".")},
		{Inode: RootInode, Type: ftDir, Name: []byte("..")},
	}
	if err := b.writeDirBlock(dataBlock, entries); err != nil {
		return fmt.Errorf("failed to write lost+found directory block: %w", err)
	}

	if err := b.addDirEntry(RootInode, dirEntry{
		Inode: inodeNum,
		Type:  ftDir,
		Name:  []byte("lost+found"),
	}); err != nil {
		return fmt.Errorf("failed to add lost+found entry to root: %w", err)
	}

	if err := b.incrementLinkCount(RootInode); err != nil {
		return fmt.Errorf("failed to increment root link count: %w", err)
	}

	// Track in correct group
	group := (inodeNum - 1) / inodesPerGroup
	b.usedDirsPerGroup[group]++

	if b.debug {
		fmt.Printf("✓ lost+found created\n")
	}

	return nil
}

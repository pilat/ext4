package ext4fs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
)

var DEBUG = false

type Builder struct {
	disk   diskBackend
	layout *Layout
	debug  bool // Enable debug output

	// Allocation state - per group
	nextBlockPerGroup   []uint32 // Next free block in each group
	freedBlocksPerGroup []uint32 // Blocks freed per group (for overwrites)
	freeBlockList       []uint32 // List of freed blocks available for reuse
	nextInode           uint32   // Next free inode (global)

	// Tracking
	usedDirsPerGroup []uint16 // Directory count per group
}

// newBuilder creates a new Builder instance with initialized allocation state.
// It sets up per-group tracking for block and inode allocation, preparing
// the builder for filesystem construction operations.
func newBuilder(disk diskBackend, layout *Layout) *Builder {
	b := &Builder{
		disk:                disk,
		layout:              layout,
		debug:               DEBUG,
		nextBlockPerGroup:   make([]uint32, layout.GroupCount),
		freedBlocksPerGroup: make([]uint32, layout.GroupCount),
		freeBlockList:       make([]uint32, 0),
		nextInode:           FirstNonResInode,
		usedDirsPerGroup:    make([]uint16, layout.GroupCount),
	}

	// Initialize next free block for each group
	for g := uint32(0); g < layout.GroupCount; g++ {
		gl := layout.GetGroupLayout(g)
		b.nextBlockPerGroup[g] = gl.FirstDataBlock
	}

	return b
}

// PrepareFilesystem initializes the complete ext4 filesystem structure.
// This includes writing the MBR, superblock, group descriptors, initializing
// bitmaps, zeroing inode tables, and creating essential directories like
// root and lost+found. This method must be called before any file operations.
func (b *Builder) PrepareFilesystem() error {
	if b.debug {
		fmt.Println(b.layout.String())
		fmt.Println()
	}

	b.writeMBR()
	b.writeSuperblock()
	b.writeGroupDescriptors()
	b.initBitmaps()
	b.zeroInodeTables()
	b.createRootDirectory()
	b.createLostFound()

	if DEBUG {
		fmt.Println("✓ Filesystem prepared successfully")
	}
	return nil
}

// ============================================================================
// Internal methods
// ============================================================================

// writeMBR writes the Master Boot Record to the first 512 bytes of the disk.
// The MBR contains the partition table with a single ext4 partition spanning
// the entire disk. This is required for the disk to be recognized as partitioned.
func (b *Builder) writeMBR() {
	mbr := MBR{
		Signature: MBRSignature,
	}

	startLBA := uint32(b.layout.PartitionStart / SectorSize)
	sizeLBA := uint32(b.layout.PartitionSize / SectorSize)

	mbr.Partitions[0] = MBRPartition{
		BootIndicator: 0x00,
		PartType:      0x83,
		StartLBA:      startLBA,
		SizeLBA:       sizeLBA,
		StartCHS:      lbaToCHS(startLBA),
		EndCHS:        lbaToCHS(startLBA + sizeLBA - 1),
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, mbr)
	b.disk.WriteAt(buf.Bytes(), 0)

	if DEBUG {
		fmt.Printf("✓ MBR written\n")
	}
}

// writeSuperblock writes the ext4 superblock to offset 1024 bytes on disk.
// The superblock contains global filesystem parameters including block size,
// inode count, feature flags, and creation timestamp. It serves as the
// filesystem's "header" containing essential metadata.
func (b *Builder) writeSuperblock() {
	sb := Superblock{
		Magic:             Ext4Magic,
		InodesCount:       b.layout.TotalInodes(),
		BlocksCountLo:     b.layout.TotalBlocks,
		FreeBlocksCountLo: b.layout.TotalFreeBlocks(),
		FreeInodesCount:   b.layout.TotalInodes() - (FirstNonResInode - 1),
		FirstDataBlock:    0,
		LogBlockSize:      BlockSizeLog,
		LogClusterSize:    BlockSizeLog,
		BlocksPerGroup:    BlocksPerGroup,
		ClustersPerGroup:  BlocksPerGroup,
		InodesPerGroup:    InodesPerGroup,
		WTime:             b.layout.CreatedAt,
		MaxMntCount:       0xFFFF,
		State:             1,
		Errors:            1,
		LastCheck:         b.layout.CreatedAt,
		CreatorOS:         0,
		RevLevel:          1,
		FirstInode:        FirstNonResInode,
		InodeSize:         InodeSize,
		BlockGroupNr:      0,
		FeatureCompat:     CompatExtAttr | CompatDirIndex,
		FeatureIncompat:   IncompatFileType | IncompatExtents,
		FeatureROCompat:   ROCompatSparseSuper | ROCompatLargeFile | ROCompatExtraIsize,
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
	binary.Write(&buf, binary.LittleEndian, sb)

	// Write primary superblock at byte 1024
	b.disk.WriteAt(buf.Bytes(), int64(b.layout.PartitionStart+SuperblockOffset))

	// Write backup superblocks in sparse groups
	for g := uint32(1); g < b.layout.GroupCount; g++ {
		if isSparseGroup(g) {
			gl := b.layout.GetGroupLayout(g)
			sb.BlockGroupNr = uint16(g)
			buf.Reset()
			binary.Write(&buf, binary.LittleEndian, sb)
			// Superblock is at byte 0 of the block, not byte 1024
			b.disk.WriteAt(buf.Bytes(), int64(b.layout.BlockOffset(gl.SuperblockBlock)))
		}
	}

	if DEBUG {
		fmt.Printf("✓ Superblock written (groups: %d, blocks: %d)\n",
			b.layout.GroupCount, b.layout.TotalBlocks)
	}
}

// writeGroupDescriptors writes the group descriptor table (GDT) after the superblock.
// Each group descriptor (32 bytes) contains metadata for its block group including
// locations of bitmaps, inode tables, and usage statistics. The GDT enables
// efficient parallel operations across multiple block groups.
func (b *Builder) writeGroupDescriptors() {
	gdt := make([]byte, b.layout.GroupCount*32)

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		freeBlocks := gl.BlocksInGroup - gl.OverheadBlocks
		freeInodes := uint16(InodesPerGroup)
		if g == 0 {
			freeInodes = uint16(InodesPerGroup - (FirstNonResInode - 1))
		}

		gd := GroupDesc32{
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
		binary.Write(&buf, binary.LittleEndian, gd)
		copy(gdt[g*32:], buf.Bytes())
	}

	gl0 := b.layout.GetGroupLayout(0)
	b.disk.WriteAt(gdt, int64(b.layout.BlockOffset(gl0.GDTStart)))

	for g := uint32(1); g < b.layout.GroupCount; g++ {
		if isSparseGroup(g) {
			gl := b.layout.GetGroupLayout(g)
			b.disk.WriteAt(gdt, int64(b.layout.BlockOffset(gl.GDTStart)))
		}
	}

	if DEBUG {
		fmt.Printf("✓ Group descriptors written (%d groups)\n", b.layout.GroupCount)
	}
}

// initBitmaps initializes the block and inode bitmaps for all block groups.
// Block bitmaps track which blocks are allocated, while inode bitmaps track
// which inodes are in use. Reserved inodes (1-10) are marked as used during initialization.
func (b *Builder) initBitmaps() {
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		// Block bitmap
		blockBitmap := make([]byte, BlockSize)

		// Mark overhead blocks as used
		for i := uint32(0); i < gl.OverheadBlocks; i++ {
			blockBitmap[i/8] |= 1 << (i % 8)
		}

		// Mark blocks beyond this group's range as used
		for i := gl.BlocksInGroup; i < BlocksPerGroup; i++ {
			blockBitmap[i/8] |= 1 << (i % 8)
		}

		b.disk.WriteAt(blockBitmap, int64(b.layout.BlockOffset(gl.BlockBitmapBlock)))

		// Inode bitmap
		inodeBitmap := make([]byte, BlockSize)

		// Mark reserved inodes in group 0
		if g == 0 {
			for i := uint32(0); i < FirstNonResInode-1; i++ {
				inodeBitmap[i/8] |= 1 << (i % 8)
			}
		}

		// Mark unused bits at end
		usedBytes := (InodesPerGroup + 7) / 8
		for i := usedBytes; i < BlockSize; i++ {
			inodeBitmap[i] = 0xFF
		}
		if InodesPerGroup%8 != 0 {
			lastByte := usedBytes - 1
			for bit := InodesPerGroup % 8; bit < 8; bit++ {
				inodeBitmap[lastByte] |= 1 << bit
			}
		}

		b.disk.WriteAt(inodeBitmap, int64(b.layout.BlockOffset(gl.InodeBitmapBlock)))
	}

	if DEBUG {
		fmt.Printf("✓ Bitmaps initialized\n")
	}
}

// zeroInodeTables initializes all inode table blocks to zero.
// Inode tables store the actual inode structures for each block group.
// Zeroing ensures no garbage data remains from previous filesystem states.
func (b *Builder) zeroInodeTables() {
	zeroBlock := make([]byte, BlockSize)
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		for i := uint32(0); i < b.layout.InodeTableBlocks; i++ {
			b.disk.WriteAt(zeroBlock, int64(b.layout.BlockOffset(gl.InodeTableStart+i)))
		}
	}

	if DEBUG {
		fmt.Printf("✓ Inode tables zeroed\n")
	}
}

// createRootDirectory creates the root directory (inode 2) with essential entries.
// The root directory contains "." and ".." entries pointing to itself, and serves
// as the mount point for the filesystem. It is allocated inode 2 by convention.
func (b *Builder) createRootDirectory() {
	dataBlock, _ := b.allocateBlock() // Ошибка невозможна при инициализации

	inode := b.makeDirectoryInode(0755, 0, 0)
	inode.LinksCount = 2
	inode.SizeLo = BlockSize
	inode.BlocksLo = BlockSize / 512
	b.setExtent(&inode, 0, dataBlock, 1)

	b.writeInode(RootInode, &inode)
	b.markInodeUsed(RootInode)

	entries := []DirEntry{
		{Inode: RootInode, Type: FTDir, Name: []byte(".")},
		{Inode: RootInode, Type: FTDir, Name: []byte("..")},
	}
	b.writeDirBlock(dataBlock, entries)

	// Root inode is always in group 0
	b.usedDirsPerGroup[0]++

	if DEBUG {
		fmt.Printf("✓ Root directory created\n")
	}
}

// createLostFound creates the lost+found directory required by ext4 filesystem standard.
// This directory is used by fsck and other utilities to store orphaned files
// and directories found during filesystem recovery operations.
func (b *Builder) createLostFound() {
	inodeNum, _ := b.allocateInode()
	dataBlock, _ := b.allocateBlock()

	inode := b.makeDirectoryInode(0700, 0, 0)
	inode.LinksCount = 2
	inode.SizeLo = BlockSize
	inode.BlocksLo = BlockSize / 512
	b.setExtent(&inode, 0, dataBlock, 1)

	b.writeInode(inodeNum, &inode)

	entries := []DirEntry{
		{Inode: inodeNum, Type: FTDir, Name: []byte(".")},
		{Inode: RootInode, Type: FTDir, Name: []byte("..")},
	}
	b.writeDirBlock(dataBlock, entries)

	b.addDirEntry(RootInode, DirEntry{
		Inode: inodeNum,
		Type:  FTDir,
		Name:  []byte("lost+found"),
	})

	b.incrementLinkCount(RootInode)

	// Track in correct group
	group := (inodeNum - 1) / InodesPerGroup
	b.usedDirsPerGroup[group]++

	if DEBUG {
		fmt.Printf("✓ lost+found created\n")
	}
}

// freeBlock marks a block as free in the appropriate block bitmap.
// This allows previously allocated blocks to be reused for future allocations.
// The block is added to the free block list for efficient reuse.
func (b *Builder) freeBlock(blockNum uint32) {
	group := blockNum / BlocksPerGroup
	indexInGroup := blockNum % BlocksPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.BlockBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	b.disk.ReadAt(buf[:], int64(offset))
	buf[0] &^= 1 << (indexInGroup % 8) // Clear the bit
	b.disk.WriteAt(buf[:], int64(offset))

	// Track freed blocks for accurate count and reuse
	b.freedBlocksPerGroup[group]++
	b.freeBlockList = append(b.freeBlockList, blockNum)
}

// ============================================================================
// Block allocation - uses all groups
// ============================================================================

// allocateBlock allocates a single free block from the filesystem.
// It first tries to reuse blocks from the free block list, then searches
// block groups for available blocks. Returns the allocated block number.
func (b *Builder) allocateBlock() (uint32, error) {
	// First, try to reuse a freed block
	if len(b.freeBlockList) > 0 {
		block := b.freeBlockList[len(b.freeBlockList)-1]
		b.freeBlockList = b.freeBlockList[:len(b.freeBlockList)-1]

		group := block / BlocksPerGroup
		b.freedBlocksPerGroup[group]--

		b.markBlockUsed(block)
		return block, nil
	}

	// Otherwise allocate new block
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		groupEnd := gl.GroupStart + gl.BlocksInGroup

		if b.nextBlockPerGroup[g] < groupEnd {
			block := b.nextBlockPerGroup[g]
			b.nextBlockPerGroup[g]++
			b.markBlockUsed(block)
			return block, nil
		}
	}
	return 0, fmt.Errorf("out of blocks")
}

// allocateBlocks allocates n consecutive free blocks from the filesystem.
// For small allocations, it tries to find contiguous blocks within a single group.
// For larger allocations, it may allocate across multiple groups.
// Returns a slice of allocated block numbers.
func (b *Builder) allocateBlocks(n uint32) ([]uint32, error) {
	if n == 0 {
		return nil, nil
	}

	blocks := make([]uint32, 0, n)

	// First, use freed blocks
	for len(blocks) < int(n) && len(b.freeBlockList) > 0 {
		block := b.freeBlockList[len(b.freeBlockList)-1]
		b.freeBlockList = b.freeBlockList[:len(b.freeBlockList)-1]

		group := block / BlocksPerGroup
		b.freedBlocksPerGroup[group]--

		b.markBlockUsed(block)
		blocks = append(blocks, block)
	}

	// Then allocate new blocks
	for len(blocks) < int(n) {
		found := false
		for g := uint32(0); g < b.layout.GroupCount; g++ {
			gl := b.layout.GetGroupLayout(g)
			groupEnd := gl.GroupStart + gl.BlocksInGroup
			available := groupEnd - b.nextBlockPerGroup[g]
			needed := n - uint32(len(blocks))

			if available > 0 {
				toAlloc := available
				if toAlloc > needed {
					toAlloc = needed
				}

				for i := uint32(0); i < toAlloc; i++ {
					block := b.nextBlockPerGroup[g]
					b.nextBlockPerGroup[g]++
					b.markBlockUsed(block)
					blocks = append(blocks, block)
				}
				found = true

				if len(blocks) >= int(n) {
					break
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("out of blocks: need %d more", n-uint32(len(blocks)))
		}
	}

	return blocks, nil
}

// allocateInode allocates the next available inode number from the global sequence.
// Inodes are allocated sequentially starting from FirstNonResInode (11).
// Returns the allocated inode number or an error if no inodes are available.
func (b *Builder) allocateInode() (uint32, error) {
	if b.nextInode > b.layout.TotalInodes() {
		return 0, fmt.Errorf("out of inodes: %d", b.nextInode)
	}

	inode := b.nextInode
	b.nextInode++
	b.markInodeUsed(inode)
	return inode, nil
}

// ============================================================================
// Bitmap operations
// ============================================================================

// markBlockUsed marks the specified block as used in its group's block bitmap.
// This prevents the block from being allocated again until it is explicitly freed.
// The bitmap is updated on disk to reflect the new allocation state.
func (b *Builder) markBlockUsed(blockNum uint32) {
	group := blockNum / BlocksPerGroup
	indexInGroup := blockNum % BlocksPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.BlockBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	b.disk.ReadAt(buf[:], int64(offset))
	buf[0] |= 1 << (indexInGroup % 8)
	b.disk.WriteAt(buf[:], int64(offset))
}

// markInodeUsed marks the specified inode as used in its group's inode bitmap.
// This prevents the inode from being allocated again and updates the bitmap on disk.
// Inode 0 is invalid and should never be marked as used.
func (b *Builder) markInodeUsed(inodeNum uint32) {
	if inodeNum < 1 {
		return
	}
	group := (inodeNum - 1) / InodesPerGroup
	indexInGroup := (inodeNum - 1) % InodesPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.InodeBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	b.disk.ReadAt(buf[:], int64(offset))
	buf[0] |= 1 << (indexInGroup % 8)
	b.disk.WriteAt(buf[:], int64(offset))
}

// ============================================================================
// Inode helpers
// ============================================================================

// makeDirectoryInode creates a new inode structure configured for a directory.
// Directory inodes have specific mode flags and initial link count of 2
// (accounting for "." and ".." entries). Timestamps are set to the filesystem creation time.
func (b *Builder) makeDirectoryInode(mode, uid, gid uint16) Inode {
	inode := Inode{
		Mode:       S_IFDIR | mode,
		UID:        uid,
		GID:        gid,
		LinksCount: 2,
		Flags:      InodeFlagExtents,
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
func (b *Builder) makeFileInode(mode, uid, gid uint16, size uint64) Inode {
	inode := Inode{
		Mode:       S_IFREG | mode,
		UID:        uid,
		GID:        gid,
		SizeLo:     uint32(size & 0xFFFFFFFF),
		SizeHi:     uint32(size >> 32),
		LinksCount: 1,
		Flags:      InodeFlagExtents,
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
func (b *Builder) initExtentHeader(inode *Inode) {
	for i := range inode.Block {
		inode.Block[i] = 0
	}
	binary.LittleEndian.PutUint16(inode.Block[0:2], ExtentMagic)
	binary.LittleEndian.PutUint16(inode.Block[2:4], 0)  // entries
	binary.LittleEndian.PutUint16(inode.Block[4:6], 4)  // max entries
	binary.LittleEndian.PutUint16(inode.Block[6:8], 0)  // depth
	binary.LittleEndian.PutUint32(inode.Block[8:12], 0) // generation
}

// setExtent sets a single extent mapping in an inode's extent tree.
// Maps a contiguous range of logical blocks to physical blocks on disk.
// Used for files that fit in a single extent or as part of a larger extent tree.
func (b *Builder) setExtent(inode *Inode, logicalBlock, physicalBlock uint32, length uint16) {
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
func (b *Builder) setExtentMultiple(inode *Inode, blocks []uint32) error {
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

	leaf := make([]byte, BlockSize)

	// Write extent header for leaf
	binary.LittleEndian.PutUint16(leaf[0:2], ExtentMagic)
	binary.LittleEndian.PutUint16(leaf[2:4], uint16(len(extents)))
	binary.LittleEndian.PutUint16(leaf[4:6], (BlockSize-12)/12) // max entries
	binary.LittleEndian.PutUint16(leaf[6:8], 0)                 // depth 0

	// Write extents to leaf
	for i, ext := range extents {
		off := 12 + i*12
		binary.LittleEndian.PutUint32(leaf[off:], ext.logical)
		binary.LittleEndian.PutUint16(leaf[off+4:], ext.length)
		binary.LittleEndian.PutUint16(leaf[off+6:], 0)
		binary.LittleEndian.PutUint32(leaf[off+8:], ext.physical)
	}

	b.disk.WriteAt(leaf, int64(b.layout.BlockOffset(leafBlock)))

	// Update inode to be index node
	for i := range inode.Block {
		inode.Block[i] = 0
	}
	binary.LittleEndian.PutUint16(inode.Block[0:2], ExtentMagic)
	binary.LittleEndian.PutUint16(inode.Block[2:4], 1) // 1 index entry
	binary.LittleEndian.PutUint16(inode.Block[4:6], 4) // max entries
	binary.LittleEndian.PutUint16(inode.Block[6:8], 1) // depth 1

	// Write index entry pointing to leaf
	binary.LittleEndian.PutUint32(inode.Block[12:16], 0)         // first logical block
	binary.LittleEndian.PutUint32(inode.Block[16:20], leafBlock) // leaf block lo
	binary.LittleEndian.PutUint16(inode.Block[20:22], 0)         // leaf block hi

	// Account for the leaf block in inode's block count
	inode.BlocksLo += BlockSize / 512

	return nil
}

// writeInode writes an inode structure to its designated location in the inode table.
// Inodes are stored in inode tables within their respective block groups.
// The inode number determines which group and offset within the table to use.
func (b *Builder) writeInode(inodeNum uint32, inode *Inode) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, inode)
	b.disk.WriteAt(buf.Bytes(), int64(b.layout.InodeOffset(inodeNum)))
}

// readInode reads an inode structure from its location in the inode table.
// Returns a pointer to the inode data, which can be used for reading file metadata
// or modifying inode attributes. The inode number determines the group and offset.
func (b *Builder) readInode(inodeNum uint32) *Inode {
	buf := make([]byte, InodeSize)
	b.disk.ReadAt(buf, int64(b.layout.InodeOffset(inodeNum)))

	inode := &Inode{}
	binary.Read(bytes.NewReader(buf), binary.LittleEndian, inode)
	return inode
}

// incrementLinkCount increases the hard link count for the specified inode.
// This is called when a directory entry is added that references the inode,
// ensuring the link count accurately reflects the number of directory references.
func (b *Builder) incrementLinkCount(inodeNum uint32) {
	inode := b.readInode(inodeNum)
	inode.LinksCount++
	b.writeInode(inodeNum, inode)
}

// ============================================================================
// Directory operations
// ============================================================================

// writeDirBlock writes a block containing directory entries to disk.
// Directory entries are packed into the block with proper record length calculations
// to ensure correct parsing. The block becomes part of the directory's data extent.
func (b *Builder) writeDirBlock(blockNum uint32, entries []DirEntry) {
	block := make([]byte, BlockSize)
	offset := 0

	for i, entry := range entries {
		nameLen := len(entry.Name)
		recLen := 8 + nameLen
		if recLen%4 != 0 {
			recLen += 4 - (recLen % 4)
		}

		if i == len(entries)-1 {
			recLen = BlockSize - offset
		}

		binary.LittleEndian.PutUint32(block[offset:], entry.Inode)
		binary.LittleEndian.PutUint16(block[offset+4:], uint16(recLen))
		block[offset+6] = uint8(nameLen)
		block[offset+7] = entry.Type
		copy(block[offset+8:], entry.Name)

		offset += recLen
	}

	b.disk.WriteAt(block, int64(b.layout.BlockOffset(blockNum)))
}

// getInodeDataBlock returns the first data block number for a directory inode.
// For directories, this is typically the block containing the "." and ".." entries.
// Used for reading directory contents during operations like adding new entries.
func (b *Builder) getInodeDataBlock(inodeNum uint32) uint32 {
	inode := b.readInode(inodeNum)
	blocks := b.getInodeBlocks(inode)
	if len(blocks) == 0 {
		panic(fmt.Sprintf("inode %d has no data blocks", inodeNum))
	}
	return blocks[0]
}

// getInodeBlocks extracts all block numbers referenced by an inode's extent tree.
// Parses the extent structures to return a complete list of data blocks allocated
// to the file or directory. Supports both simple extents and complex extent trees.
func (b *Builder) getInodeBlocks(inode *Inode) []uint32 {
	if (inode.Flags & InodeFlagExtents) == 0 {
		return nil
	}

	entries := binary.LittleEndian.Uint16(inode.Block[2:4])
	depth := binary.LittleEndian.Uint16(inode.Block[6:8])

	if entries == 0 {
		return nil
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

			leafData := make([]byte, BlockSize)
			b.disk.ReadAt(leafData, int64(b.layout.BlockOffset(leafBlock)))

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

	return blocks
}

// addDirEntry adds a new directory entry to the specified directory.
// Searches existing directory blocks for space, or allocates new blocks if needed.
// Updates the directory's size and block allocation as entries are added.
func (b *Builder) addDirEntry(dirInode uint32, entry DirEntry) error {
	inode := b.readInode(dirInode)
	dataBlocks := b.getInodeBlocks(inode)

	newNameLen := len(entry.Name)
	newRecLen := 8 + newNameLen
	if newRecLen%4 != 0 {
		newRecLen += 4 - (newRecLen % 4)
	}

	for _, blockNum := range dataBlocks {
		if b.tryAddEntryToBlock(blockNum, entry, newRecLen) {
			return nil
		}
	}

	// Allocate new block
	newBlock, err := b.allocateBlock()
	if err != nil {
		return err
	}

	if err := b.addBlockToInode(dirInode, newBlock); err != nil {
		return err
	}

	block := make([]byte, BlockSize)
	binary.LittleEndian.PutUint32(block[0:], entry.Inode)
	binary.LittleEndian.PutUint16(block[4:], uint16(BlockSize))
	block[6] = uint8(newNameLen)
	block[7] = entry.Type
	copy(block[8:], entry.Name)

	b.disk.WriteAt(block, int64(b.layout.BlockOffset(newBlock)))

	inode = b.readInode(dirInode)
	inode.SizeLo += BlockSize
	inode.BlocksLo += BlockSize / 512
	b.writeInode(dirInode, inode)

	return nil
}

// tryAddEntryToBlock attempts to add a directory entry to an existing directory block.
// Returns true if the entry fits in the available space, false if the block is full.
// Calculates proper record lengths to maintain directory entry structure integrity.
func (b *Builder) tryAddEntryToBlock(blockNum uint32, entry DirEntry, newRecLen int) bool {
	block := make([]byte, BlockSize)
	b.disk.ReadAt(block, int64(b.layout.BlockOffset(blockNum)))

	offset := 0
	lastOffset := 0
	for offset < BlockSize {
		recLen := binary.LittleEndian.Uint16(block[offset+4:])
		if recLen == 0 {
			break
		}
		lastOffset = offset
		offset += int(recLen)
	}

	lastNameLen := int(block[lastOffset+6])
	lastActualSize := 8 + lastNameLen
	if lastActualSize%4 != 0 {
		lastActualSize += 4 - (lastActualSize % 4)
	}
	lastRecLen := int(binary.LittleEndian.Uint16(block[lastOffset+4:]))

	spaceAvailable := lastRecLen - lastActualSize
	if spaceAvailable < newRecLen {
		return false
	}

	binary.LittleEndian.PutUint16(block[lastOffset+4:], uint16(lastActualSize))

	newOffset := lastOffset + lastActualSize
	remaining := BlockSize - newOffset

	binary.LittleEndian.PutUint32(block[newOffset:], entry.Inode)
	binary.LittleEndian.PutUint16(block[newOffset+4:], uint16(remaining))
	block[newOffset+6] = uint8(len(entry.Name))
	block[newOffset+7] = entry.Type
	copy(block[newOffset+8:], entry.Name)

	b.disk.WriteAt(block, int64(b.layout.BlockOffset(blockNum)))
	return true
}

// addBlockToInode adds a new block to a directory inode's extent tree.
// Used when a directory grows beyond its current block allocation.
// May convert from simple extents to indexed extents for large directories.
func (b *Builder) addBlockToInode(inodeNum, newBlock uint32) error {
	inode := b.readInode(inodeNum)

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
		b.writeInode(inodeNum, inode)
		return nil
	}

	lastOff := 12 + (entries-1)*12
	lastLogical := binary.LittleEndian.Uint32(inode.Block[lastOff:])
	lastLen := binary.LittleEndian.Uint16(inode.Block[lastOff+4:])
	lastStart := binary.LittleEndian.Uint32(inode.Block[lastOff+8:])

	if lastStart+uint32(lastLen) == newBlock && lastLen < 32768 {
		binary.LittleEndian.PutUint16(inode.Block[lastOff+4:], lastLen+1)
		b.writeInode(inodeNum, inode)
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
	b.writeInode(inodeNum, inode)
	return nil
}

// convertToIndexedExtents converts a simple extent inode to use indexed extents.
// Creates an extent index block to manage multiple extents efficiently.
// Required when a file or directory exceeds the capacity of inline extent storage.
func (b *Builder) convertToIndexedExtents(inodeNum, newBlock uint32) error {
	inode := b.readInode(inodeNum)
	entries := binary.LittleEndian.Uint16(inode.Block[2:4])

	leafBlock, err := b.allocateBlock()
	if err != nil {
		return err
	}

	leaf := make([]byte, BlockSize)
	binary.LittleEndian.PutUint16(leaf[0:], ExtentMagic)
	binary.LittleEndian.PutUint16(leaf[2:], entries+1)
	binary.LittleEndian.PutUint16(leaf[4:], (BlockSize-12)/12)
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

	b.disk.WriteAt(leaf, int64(b.layout.BlockOffset(leafBlock)))

	for i := range inode.Block {
		inode.Block[i] = 0
	}

	binary.LittleEndian.PutUint16(inode.Block[0:], ExtentMagic)
	binary.LittleEndian.PutUint16(inode.Block[2:], 1)
	binary.LittleEndian.PutUint16(inode.Block[4:], 4)
	binary.LittleEndian.PutUint16(inode.Block[6:], 1)

	binary.LittleEndian.PutUint32(inode.Block[12:], 0)
	binary.LittleEndian.PutUint32(inode.Block[16:], leafBlock)
	binary.LittleEndian.PutUint16(inode.Block[20:], 0)

	inode.BlocksLo += BlockSize / 512

	b.writeInode(inodeNum, inode)
	return nil
}

// addBlockToIndexedInode adds a new block to an inode that uses indexed extents.
// Updates the extent index structure to include the new extent mapping.
// Handles the complexity of maintaining sorted extent indices.
func (b *Builder) addBlockToIndexedInode(inodeNum, newBlock uint32) error {
	inode := b.readInode(inodeNum)

	leafBlock := binary.LittleEndian.Uint32(inode.Block[16:])

	leaf := make([]byte, BlockSize)
	b.disk.ReadAt(leaf, int64(b.layout.BlockOffset(leafBlock)))

	entries := binary.LittleEndian.Uint16(leaf[2:4])
	maxEntries := binary.LittleEndian.Uint16(leaf[4:6])

	lastOff := 12 + (entries-1)*12
	lastLogical := binary.LittleEndian.Uint32(leaf[lastOff:])
	lastLen := binary.LittleEndian.Uint16(leaf[lastOff+4:])
	lastStart := binary.LittleEndian.Uint32(leaf[lastOff+8:])

	if lastStart+uint32(lastLen) == newBlock && lastLen < 32768 {
		binary.LittleEndian.PutUint16(leaf[lastOff+4:], lastLen+1)
		b.disk.WriteAt(leaf, int64(b.layout.BlockOffset(leafBlock)))
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
	b.disk.WriteAt(leaf, int64(b.layout.BlockOffset(leafBlock)))
	return nil
}

// findEntry searches for a directory entry with the specified name.
// Returns the inode number if found, or 0 if the entry doesn't exist.
// Used to check for existing files before creation or overwriting.
func (b *Builder) findEntry(dirInode uint32, name string) uint32 {
	inode := b.readInode(dirInode)
	dataBlocks := b.getInodeBlocks(inode)

	for _, blockNum := range dataBlocks {
		block := make([]byte, BlockSize)
		b.disk.ReadAt(block, int64(b.layout.BlockOffset(blockNum)))

		offset := 0
		for offset < BlockSize {
			recLen := binary.LittleEndian.Uint16(block[offset+4:])
			if recLen == 0 {
				break
			}

			nameLen := int(block[offset+6])
			entryName := string(block[offset+8 : offset+8+nameLen])

			if entryName == name {
				return binary.LittleEndian.Uint32(block[offset:])
			}

			offset += int(recLen)
		}
	}

	return 0
}

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
	oldInode := b.readInode(inodeNum)

	// Free the xattr block if present
	if oldInode.FileACLLo != 0 {
		b.freeBlock(oldInode.FileACLLo)
	}

	// Free the old blocks
	oldBlocks := b.getInodeBlocks(oldInode)
	for _, blk := range oldBlocks {
		b.freeBlock(blk)
	}

	// If the old inode had an extent tree (depth > 0), free the index blocks too
	if (oldInode.Flags & InodeFlagExtents) != 0 {
		depth := binary.LittleEndian.Uint16(oldInode.Block[6:8])
		if depth > 0 {
			entries := binary.LittleEndian.Uint16(oldInode.Block[2:4])
			for i := uint16(0); i < entries && i < 4; i++ {
				off := 12 + i*12
				leafBlock := binary.LittleEndian.Uint32(oldInode.Block[off+4:])
				b.freeBlock(leafBlock)
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
		b.disk.WriteAt(block, int64(b.layout.BlockOffset(blk)))
	}

	b.writeInode(inodeNum, &inode)

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

	b.disk.WriteAt(block, int64(b.layout.BlockOffset(blockNum)))
	return nil
}

// readXattrBlock reads extended attribute entries from a dedicated block.
// Parses the special xattr block format to extract name-value pairs.
// Returns a slice of XattrEntry structures for further processing.
func (b *Builder) readXattrBlock(blockNum uint32) []XattrEntry {
	block := make([]byte, BlockSize)
	b.disk.ReadAt(block, int64(b.layout.BlockOffset(blockNum)))

	magic := binary.LittleEndian.Uint32(block[0:4])
	if magic != XattrMagic {
		return nil
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

	return entries
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

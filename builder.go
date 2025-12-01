package ext4fs

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var DEBUG = false

type Builder struct {
	disk   diskBackend
	layout *Layout

	// Allocation state - per group
	nextBlockPerGroup   []uint32 // Next free block in each group
	freedBlocksPerGroup []uint32 // Blocks freed per group (for overwrites)
	nextInode           uint32   // Next free inode (global)

	// Tracking
	usedDirs uint16
}

func newBuilder(disk diskBackend, layout *Layout) *Builder {
	b := &Builder{
		disk:                disk,
		layout:              layout,
		nextBlockPerGroup:   make([]uint32, layout.GroupCount),
		freedBlocksPerGroup: make([]uint32, layout.GroupCount),
		nextInode:           FirstNonResInode,
		usedDirs:            0,
	}

	// Initialize next free block for each group
	for g := uint32(0); g < layout.GroupCount; g++ {
		gl := layout.GetGroupLayout(g)
		b.nextBlockPerGroup[g] = gl.FirstDataBlock
	}

	return b
}

func (b *Builder) PrepareFilesystem() error {
	if DEBUG {
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
	b.writeAt(0, buf.Bytes())

	if DEBUG {
		fmt.Printf("✓ MBR written\n")
	}
}

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

	for i := 0; i < 16; i++ {
		sb.UUID[i] = byte(b.layout.CreatedAt>>uint(i%4*8)) ^ byte(i*17)
	}
	copy(sb.VolumeName[:], "ext4-go")
	for i := 0; i < 4; i++ {
		sb.HashSeed[i] = b.layout.CreatedAt + uint32(i*0x12345678)
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, sb)

	// Write primary superblock at byte 1024
	b.writeAt(b.layout.PartitionStart+SuperblockOffset, buf.Bytes())

	// Write backup superblocks in sparse groups
	for g := uint32(1); g < b.layout.GroupCount; g++ {
		if isSparseGroup(g) {
			gl := b.layout.GetGroupLayout(g)
			sb.BlockGroupNr = uint16(g)
			buf.Reset()
			binary.Write(&buf, binary.LittleEndian, sb)
			// Superblock is at byte 0 of the block, not byte 1024
			b.writeAt(b.layout.BlockOffset(gl.SuperblockBlock), buf.Bytes())
		}
	}

	if DEBUG {
		fmt.Printf("✓ Superblock written (groups: %d, blocks: %d)\n",
			b.layout.GroupCount, b.layout.TotalBlocks)
	}
}

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
			Flags:             BGInodeZeroed, // Mark as zeroed since we zero inode tables
			ItableUnusedLo:    freeInodes,    // Will be updated in FinalizeMetadata
		}

		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, gd)
		copy(gdt[g*32:], buf.Bytes())
	}

	gl0 := b.layout.GetGroupLayout(0)
	b.writeAt(b.layout.BlockOffset(gl0.GDTStart), gdt)

	for g := uint32(1); g < b.layout.GroupCount; g++ {
		if isSparseGroup(g) {
			gl := b.layout.GetGroupLayout(g)
			b.writeAt(b.layout.BlockOffset(gl.GDTStart), gdt)
		}
	}

	if DEBUG {
		fmt.Printf("✓ Group descriptors written (%d groups)\n", b.layout.GroupCount)
	}
}

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

		b.writeAt(b.layout.BlockOffset(gl.BlockBitmapBlock), blockBitmap)

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

		b.writeAt(b.layout.BlockOffset(gl.InodeBitmapBlock), inodeBitmap)
	}

	if DEBUG {
		fmt.Printf("✓ Bitmaps initialized\n")
	}
}

func (b *Builder) zeroInodeTables() {
	zeroBlock := make([]byte, BlockSize)
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		for i := uint32(0); i < b.layout.InodeTableBlocks; i++ {
			b.writeAt(b.layout.BlockOffset(gl.InodeTableStart+i), zeroBlock)
		}
	}

	if DEBUG {
		fmt.Printf("✓ Inode tables zeroed\n")
	}
}

func (b *Builder) createRootDirectory() {
	dataBlock := b.allocateBlock()

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

	b.usedDirs++

	if DEBUG {
		fmt.Printf("✓ Root directory created\n")
	}
}

func (b *Builder) createLostFound() {
	inodeNum := b.allocateInode()
	dataBlock := b.allocateBlock()

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
	b.usedDirs++

	if DEBUG {
		fmt.Printf("✓ lost+found created\n")
	}
}

// ============================================================================
// Public API
// ============================================================================

func (b *Builder) CreateDirectory(parentInode uint32, name string, mode, uid, gid uint16) uint32 {
	inodeNum := b.allocateInode()
	dataBlock := b.allocateBlock()

	inode := b.makeDirectoryInode(mode, uid, gid)
	inode.LinksCount = 2
	inode.SizeLo = BlockSize
	inode.BlocksLo = BlockSize / 512
	b.setExtent(&inode, 0, dataBlock, 1)

	b.writeInode(inodeNum, &inode)

	entries := []DirEntry{
		{Inode: inodeNum, Type: FTDir, Name: []byte(".")},
		{Inode: parentInode, Type: FTDir, Name: []byte("..")},
	}
	b.writeDirBlock(dataBlock, entries)

	b.addDirEntry(parentInode, DirEntry{
		Inode: inodeNum,
		Type:  FTDir,
		Name:  []byte(name),
	})

	b.incrementLinkCount(parentInode)
	b.usedDirs++

	if DEBUG {
		fmt.Printf("✓ Created directory: %s (inode %d)\n", name, inodeNum)
	}

	return inodeNum
}

func (b *Builder) CreateFile(parentInode uint32, name string, content []byte, mode, uid, gid uint16) uint32 {
	existingInode := b.findEntry(parentInode, name)
	if existingInode != 0 {
		return b.overwriteFile(existingInode, content, mode, uid, gid)
	}

	inodeNum := b.allocateInode()

	size := uint32(len(content))
	blocksNeeded := (size + BlockSize - 1) / BlockSize
	if blocksNeeded == 0 {
		blocksNeeded = 1
	}

	inode := b.makeFileInode(mode, uid, gid, size)

	blocks := b.allocateBlocks(blocksNeeded)
	if blocksNeeded == 1 {
		b.setExtent(&inode, 0, blocks[0], 1)
	} else {
		b.setExtentMultiple(&inode, blocks)
	}
	inode.BlocksLo = blocksNeeded * (BlockSize / 512)

	// Write content
	for i, blk := range blocks {
		block := make([]byte, BlockSize)
		start := uint32(i) * BlockSize
		end := start + BlockSize
		if end > size {
			end = size
		}
		if start < size {
			copy(block, content[start:end])
		}
		b.writeAt(b.layout.BlockOffset(blk), block)
	}

	b.writeInode(inodeNum, &inode)

	b.addDirEntry(parentInode, DirEntry{
		Inode: inodeNum,
		Type:  FTRegFile,
		Name:  []byte(name),
	})

	if DEBUG {
		fmt.Printf("✓ Created file: %s (inode %d, size %d)\n", name, inodeNum, size)
	}

	return inodeNum
}

// Add this new function to free blocks in bitmap
func (b *Builder) freeBlock(blockNum uint32) {
	group := blockNum / BlocksPerGroup
	indexInGroup := blockNum % BlocksPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.BlockBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	b.readAt(offset, buf[:])
	buf[0] &^= 1 << (indexInGroup % 8) // Clear the bit
	b.writeAt(offset, buf[:])

	// Track freed blocks for accurate count in FinalizeMetadata
	b.freedBlocksPerGroup[group]++
}

// Replace the existing overwriteFile function
func (b *Builder) overwriteFile(inodeNum uint32, content []byte, mode, uid, gid uint16) uint32 {
	// Read existing inode to get its blocks
	oldInode := b.readInode(inodeNum)

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

	size := uint32(len(content))
	blocksNeeded := (size + BlockSize - 1) / BlockSize
	if blocksNeeded == 0 {
		blocksNeeded = 1
	}

	inode := b.makeFileInode(mode, uid, gid, size)

	blocks := b.allocateBlocks(blocksNeeded)
	if blocksNeeded == 1 {
		b.setExtent(&inode, 0, blocks[0], 1)
	} else {
		b.setExtentMultiple(&inode, blocks)
	}
	inode.BlocksLo = blocksNeeded * (BlockSize / 512)

	for i, blk := range blocks {
		block := make([]byte, BlockSize)
		start := uint32(i) * BlockSize
		end := start + BlockSize
		if end > size {
			end = size
		}
		if start < size {
			copy(block, content[start:end])
		}
		b.writeAt(b.layout.BlockOffset(blk), block)
	}

	b.writeInode(inodeNum, &inode)

	return inodeNum
}

func (b *Builder) CreateSymlink(parentInode uint32, name, target string, uid, gid uint16) uint32 {
	inodeNum := b.allocateInode()

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

	if len(target) < 60 {
		copy(inode.Block[:], target)
		inode.Flags = 0
		inode.BlocksLo = 0
	} else {
		inode.Flags = InodeFlagExtents
		dataBlock := b.allocateBlock()
		b.initExtentHeader(&inode)
		b.setExtent(&inode, 0, dataBlock, 1)
		inode.BlocksLo = BlockSize / 512

		block := make([]byte, BlockSize)
		copy(block, target)
		b.writeAt(b.layout.BlockOffset(dataBlock), block)
	}

	b.writeInode(inodeNum, &inode)

	b.addDirEntry(parentInode, DirEntry{
		Inode: inodeNum,
		Type:  FTSymlink,
		Name:  []byte(name),
	})

	if DEBUG {
		fmt.Printf("✓ Created symlink: %s -> %s\n", name, target)
	}

	return inodeNum
}

// ============================================================================
// Block allocation - uses all groups
// ============================================================================

func (b *Builder) allocateBlock() uint32 {
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		groupEnd := gl.GroupStart + gl.BlocksInGroup

		if b.nextBlockPerGroup[g] < groupEnd {
			block := b.nextBlockPerGroup[g]
			b.nextBlockPerGroup[g]++
			b.markBlockUsed(block)
			return block
		}
	}
	panic("out of blocks")
}

func (b *Builder) allocateBlocks(n uint32) []uint32 {
	blocks := make([]uint32, 0, n)

	for len(blocks) < int(n) {
		// Try to find a group with enough contiguous blocks
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
			panic(fmt.Sprintf("out of blocks: need %d more", n-uint32(len(blocks))))
		}
	}

	return blocks
}

func (b *Builder) allocateInode() uint32 {
	if b.nextInode > b.layout.TotalInodes() {
		panic(fmt.Sprintf("out of inodes: %d", b.nextInode))
	}

	inode := b.nextInode
	b.nextInode++
	b.markInodeUsed(inode)
	return inode
}

// ============================================================================
// Bitmap operations
// ============================================================================

func (b *Builder) markBlockUsed(blockNum uint32) {
	group := blockNum / BlocksPerGroup
	indexInGroup := blockNum % BlocksPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.BlockBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	b.readAt(offset, buf[:])
	buf[0] |= 1 << (indexInGroup % 8)
	b.writeAt(offset, buf[:])
}

func (b *Builder) markInodeUsed(inodeNum uint32) {
	if inodeNum < 1 {
		return
	}
	group := (inodeNum - 1) / InodesPerGroup
	indexInGroup := (inodeNum - 1) % InodesPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.InodeBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	b.readAt(offset, buf[:])
	buf[0] |= 1 << (indexInGroup % 8)
	b.writeAt(offset, buf[:])
}

// ============================================================================
// Inode helpers
// ============================================================================

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

func (b *Builder) makeFileInode(mode, uid, gid uint16, size uint32) Inode {
	inode := Inode{
		Mode:       S_IFREG | mode,
		UID:        uid,
		GID:        gid,
		SizeLo:     size,
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

func (b *Builder) setExtent(inode *Inode, logicalBlock, physicalBlock uint32, length uint16) {
	binary.LittleEndian.PutUint16(inode.Block[2:4], 1)
	binary.LittleEndian.PutUint32(inode.Block[12:16], logicalBlock)
	binary.LittleEndian.PutUint16(inode.Block[16:18], length)
	binary.LittleEndian.PutUint16(inode.Block[18:20], 0)
	binary.LittleEndian.PutUint32(inode.Block[20:24], physicalBlock)
}

// setExtentMultiple handles non-contiguous blocks by creating multiple extents or extent tree
func (b *Builder) setExtentMultiple(inode *Inode, blocks []uint32) {
	if len(blocks) == 0 {
		return
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
		return
	}

	// Need extent tree - allocate leaf block
	leafBlock := b.allocateBlock()
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

	b.writeAt(b.layout.BlockOffset(leafBlock), leaf)

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
}

func (b *Builder) writeInode(inodeNum uint32, inode *Inode) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, inode)
	b.writeAt(b.layout.InodeOffset(inodeNum), buf.Bytes())
}

func (b *Builder) readInode(inodeNum uint32) *Inode {
	buf := make([]byte, InodeSize)
	b.readAt(b.layout.InodeOffset(inodeNum), buf)

	inode := &Inode{}
	binary.Read(bytes.NewReader(buf), binary.LittleEndian, inode)
	return inode
}

func (b *Builder) incrementLinkCount(inodeNum uint32) {
	inode := b.readInode(inodeNum)
	inode.LinksCount++
	b.writeInode(inodeNum, inode)
}

// ============================================================================
// Directory operations
// ============================================================================

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

	b.writeAt(b.layout.BlockOffset(blockNum), block)
}

func (b *Builder) getInodeDataBlock(inodeNum uint32) uint32 {
	inode := b.readInode(inodeNum)
	blocks := b.getInodeBlocks(inode)
	if len(blocks) == 0 {
		panic(fmt.Sprintf("inode %d has no data blocks", inodeNum))
	}
	return blocks[0]
}

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
			b.readAt(b.layout.BlockOffset(leafBlock), leafData)

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

func (b *Builder) addDirEntry(dirInode uint32, entry DirEntry) {
	inode := b.readInode(dirInode)
	dataBlocks := b.getInodeBlocks(inode)

	newNameLen := len(entry.Name)
	newRecLen := 8 + newNameLen
	if newRecLen%4 != 0 {
		newRecLen += 4 - (newRecLen % 4)
	}

	for _, blockNum := range dataBlocks {
		if b.tryAddEntryToBlock(blockNum, entry, newRecLen) {
			return
		}
	}

	// Allocate new block
	newBlock := b.allocateBlock()
	b.addBlockToInode(dirInode, newBlock)

	block := make([]byte, BlockSize)
	binary.LittleEndian.PutUint32(block[0:], entry.Inode)
	binary.LittleEndian.PutUint16(block[4:], uint16(BlockSize))
	block[6] = uint8(newNameLen)
	block[7] = entry.Type
	copy(block[8:], entry.Name)

	b.writeAt(b.layout.BlockOffset(newBlock), block)

	inode = b.readInode(dirInode)
	inode.SizeLo += BlockSize
	inode.BlocksLo += BlockSize / 512
	b.writeInode(dirInode, inode)
}

func (b *Builder) tryAddEntryToBlock(blockNum uint32, entry DirEntry, newRecLen int) bool {
	block := make([]byte, BlockSize)
	b.readAt(b.layout.BlockOffset(blockNum), block)

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

	b.writeAt(b.layout.BlockOffset(blockNum), block)
	return true
}

func (b *Builder) addBlockToInode(inodeNum, newBlock uint32) {
	inode := b.readInode(inodeNum)

	entries := binary.LittleEndian.Uint16(inode.Block[2:4])
	maxEntries := binary.LittleEndian.Uint16(inode.Block[4:6])
	depth := binary.LittleEndian.Uint16(inode.Block[6:8])

	if depth != 0 {
		b.addBlockToIndexedInode(inodeNum, newBlock)
		return
	}

	if entries == 0 {
		binary.LittleEndian.PutUint16(inode.Block[2:4], 1)
		binary.LittleEndian.PutUint32(inode.Block[12:], 0)
		binary.LittleEndian.PutUint16(inode.Block[16:], 1)
		binary.LittleEndian.PutUint16(inode.Block[18:], 0)
		binary.LittleEndian.PutUint32(inode.Block[20:], newBlock)
		b.writeInode(inodeNum, inode)
		return
	}

	lastOff := 12 + (entries-1)*12
	lastLogical := binary.LittleEndian.Uint32(inode.Block[lastOff:])
	lastLen := binary.LittleEndian.Uint16(inode.Block[lastOff+4:])
	lastStart := binary.LittleEndian.Uint32(inode.Block[lastOff+8:])

	if lastStart+uint32(lastLen) == newBlock && lastLen < 32768 {
		binary.LittleEndian.PutUint16(inode.Block[lastOff+4:], lastLen+1)
		b.writeInode(inodeNum, inode)
		return
	}

	if entries >= maxEntries {
		b.convertToIndexedExtents(inodeNum, newBlock)
		return
	}

	newOff := 12 + entries*12
	nextLogical := lastLogical + uint32(lastLen)

	binary.LittleEndian.PutUint32(inode.Block[newOff:], nextLogical)
	binary.LittleEndian.PutUint16(inode.Block[newOff+4:], 1)
	binary.LittleEndian.PutUint16(inode.Block[newOff+6:], 0)
	binary.LittleEndian.PutUint32(inode.Block[newOff+8:], newBlock)

	binary.LittleEndian.PutUint16(inode.Block[2:4], entries+1)
	b.writeInode(inodeNum, inode)
}

func (b *Builder) convertToIndexedExtents(inodeNum, newBlock uint32) {
	inode := b.readInode(inodeNum)
	entries := binary.LittleEndian.Uint16(inode.Block[2:4])

	leafBlock := b.allocateBlock()

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

	b.writeAt(b.layout.BlockOffset(leafBlock), leaf)

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
}

func (b *Builder) addBlockToIndexedInode(inodeNum, newBlock uint32) {
	inode := b.readInode(inodeNum)

	leafBlock := binary.LittleEndian.Uint32(inode.Block[16:])

	leaf := make([]byte, BlockSize)
	b.readAt(b.layout.BlockOffset(leafBlock), leaf)

	entries := binary.LittleEndian.Uint16(leaf[2:4])
	maxEntries := binary.LittleEndian.Uint16(leaf[4:6])

	lastOff := 12 + (entries-1)*12
	lastLogical := binary.LittleEndian.Uint32(leaf[lastOff:])
	lastLen := binary.LittleEndian.Uint16(leaf[lastOff+4:])
	lastStart := binary.LittleEndian.Uint32(leaf[lastOff+8:])

	if lastStart+uint32(lastLen) == newBlock && lastLen < 32768 {
		binary.LittleEndian.PutUint16(leaf[lastOff+4:], lastLen+1)
		b.writeAt(b.layout.BlockOffset(leafBlock), leaf)
		return
	}

	if entries >= maxEntries {
		panic("extent tree depth > 1 not implemented")
	}

	nextLogical := lastLogical + uint32(lastLen)
	newOff := 12 + entries*12

	binary.LittleEndian.PutUint32(leaf[newOff:], nextLogical)
	binary.LittleEndian.PutUint16(leaf[newOff+4:], 1)
	binary.LittleEndian.PutUint16(leaf[newOff+6:], 0)
	binary.LittleEndian.PutUint32(leaf[newOff+8:], newBlock)

	binary.LittleEndian.PutUint16(leaf[2:4], entries+1)
	b.writeAt(b.layout.BlockOffset(leafBlock), leaf)
}

func (b *Builder) findEntry(dirInode uint32, name string) uint32 {
	inode := b.readInode(dirInode)
	dataBlocks := b.getInodeBlocks(inode)

	for _, blockNum := range dataBlocks {
		block := make([]byte, BlockSize)
		b.readAt(b.layout.BlockOffset(blockNum), block)

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

// ============================================================================
// Finalization
// ============================================================================

func (b *Builder) FinalizeMetadata() {
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
		b.readAt(gdOffset, gdBuf)

		// Update fields
		binary.LittleEndian.PutUint16(gdBuf[12:14], uint16(freeBlocks))
		binary.LittleEndian.PutUint16(gdBuf[14:16], freeInodes)
		if g == 0 {
			binary.LittleEndian.PutUint16(gdBuf[16:18], b.usedDirs)
		}
		// Flags at offset 18 - keep BGInodeZeroed
		binary.LittleEndian.PutUint16(gdBuf[18:20], BGInodeZeroed)
		// ItableUnusedLo at offset 28
		binary.LittleEndian.PutUint16(gdBuf[28:30], itableUnused)

		b.writeAt(gdOffset, gdBuf)

		// Update backup GDTs
		for bg := uint32(1); bg < b.layout.GroupCount; bg++ {
			if isSparseGroup(bg) {
				backupGl := b.layout.GetGroupLayout(bg)
				backupOffset := b.layout.BlockOffset(backupGl.GDTStart) + uint64(g*32)
				b.writeAt(backupOffset, gdBuf)
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
	b.readAt(sbOffset, sbBuf)

	binary.LittleEndian.PutUint32(sbBuf[0x0C:0x10], totalFreeBlocks)
	binary.LittleEndian.PutUint32(sbBuf[0x10:0x14], totalFreeInodes)

	b.writeAt(sbOffset, sbBuf)

	if DEBUG {
		fmt.Printf("✓ Metadata finalized: %d free blocks, %d free inodes\n",
			totalFreeBlocks, totalFreeInodes)
	}
}

// ============================================================================
// I/O
// ============================================================================

func (b *Builder) writeAt(offset uint64, data []byte) {
	if _, err := b.disk.WriteAt(data, int64(offset)); err != nil {
		panic(fmt.Sprintf("write at %d: %v", offset, err))
	}
}

func (b *Builder) readAt(offset uint64, buf []byte) {
	if _, err := b.disk.ReadAt(buf, int64(offset)); err != nil {
		panic(fmt.Sprintf("read at %d: %v", offset, err))
	}
}

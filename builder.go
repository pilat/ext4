package ext4

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// Ext4ImageBuilder handles the creation of ext4 images backed by a diskBackend.
type Ext4ImageBuilder struct {
	disk           diskBackend
	imagePath      string
	totalSize      uint64
	partitionStart uint64
	partitionSize  uint64
	blockCount     uint32
	groupCount     uint32
	inodesCount    uint32
	nextFreeInode  uint32
	nextFreeBlock  uint32
	createdAt      uint32
}

// newExt4ImageBuilder constructs an Ext4ImageBuilder with a pre-initialized disk backend.
// Callers are responsible for creating/truncating the underlying disk to totalSize bytes.
func newExt4ImageBuilder(disk diskBackend, imagePath string, totalSize uint64) *Ext4ImageBuilder {
	partitionStart := uint64(1 * 1024 * 1024) // 1MB offset for alignment
	partitionSize := totalSize - partitionStart
	blockCount := uint32(partitionSize / BlockSize)
	groupCount := (blockCount + BlocksPerGroup - 1) / BlocksPerGroup
	inodesCount := groupCount * InodesPerGroup

	return &Ext4ImageBuilder{
		disk:           disk,
		imagePath:      imagePath,
		totalSize:      totalSize,
		partitionStart: partitionStart,
		partitionSize:  partitionSize,
		blockCount:     blockCount,
		groupCount:     groupCount,
		inodesCount:    inodesCount,
		nextFreeInode:  FirstNonResInode,
		nextFreeBlock:  0, // Will be calculated
		createdAt:      uint32(time.Now().Unix()),
	}
}

// PrepareFilesystem initializes the disk image with MBR, core ext4 metadata,
// the root directory, and the lost+found directory.
func (b *Ext4ImageBuilder) PrepareFilesystem() {
	b.writeMBR()
	b.writeSuperblock()
	b.writeGroupDescriptors()
	b.writeBitmaps()
	b.writeRootDirectory()
	b.CreateLostFound()
}

// CreateDirectory creates a new directory in the filesystem
func (b *Ext4ImageBuilder) CreateDirectory(parentInodeNum uint32, name string, mode, uid, gid uint16) uint32 {
	// Allocate inode for new directory
	newInodeNum := b.allocateInode()

	// Create directory inode
	dirInode := b.createDirInode(mode, uid, gid)
	b.writeInode(newInodeNum, dirInode)

	// Allocate data block for directory
	dataBlock := b.allocateBlock()
	b.setInodeBlock(newInodeNum, dataBlock)

	// Create . and .. entries
	entries := []Ext4DirEntry2{
		{Inode: newInodeNum, FileType: FTDir, Name: []byte(".")},
		{Inode: parentInodeNum, FileType: FTDir, Name: []byte("..")},
	}
	b.writeDirEntries(dataBlock, entries)

	// Update inode without losing the extent mapping we just created
	dirInode = b.readInode(newInodeNum)
	dirInode.LinksCount = 2
	dirInode.SizeLo = BlockSize
	b.writeInode(newInodeNum, dirInode)

	// Add entry to parent directory
	b.addDirEntry(parentInodeNum, Ext4DirEntry2{
		Inode:    newInodeNum,
		FileType: FTDir,
		Name:     []byte(name),
	})

	// Increment parent link count
	parentInode := b.readInode(parentInodeNum)
	parentInode.LinksCount++
	b.writeInode(parentInodeNum, parentInode)

	fmt.Printf("  ✓ Created directory: %s (inode %d, uid=%d, gid=%d, mode=%04o)\n",
		name, newInodeNum, uid, gid, mode)

	return newInodeNum
}

// CreateFile creates a new file in the filesystem or overwrites an existing one
func (b *Ext4ImageBuilder) CreateFile(parentInodeNum uint32, name string, content []byte, mode, uid, gid uint16) uint32 {
	// Check if file already exists
	existingInode := b.findInodeByName(parentInodeNum, name)

	if existingInode != 0 {
		// File exists - overwrite it
		return b.overwriteFile(existingInode, content, mode, uid, gid, name)
	}

	// File doesn't exist - create new one
	// Allocate inode for new file
	newInodeNum := b.allocateInode()

	// Calculate blocks needed
	size := uint32(len(content))
	blocksNeeded := (size + BlockSize - 1) / BlockSize
	if blocksNeeded == 0 {
		blocksNeeded = 1
	}

	// Create file inode
	fileInode := b.createFileInode(mode, uid, gid, size)
	b.writeInode(newInodeNum, fileInode)

	// Allocate data blocks
	if blocksNeeded > 0 {
		blocks := b.allocateBlocks(blocksNeeded)
		b.setInodeBlocks(newInodeNum, blocks)

		// Write content to blocks
		for i, blockNum := range blocks {
			startIdx := i * BlockSize
			if startIdx >= len(content) {
				break
			}
			endIdx := startIdx + BlockSize
			if endIdx > len(content) {
				endIdx = len(content)
			}

			blockOffset := b.blockOffset(blockNum)
			b.writeAt(blockOffset, content[startIdx:endIdx])
		}
	}

	// Update inode blocks count
	fileInode = b.readInode(newInodeNum)
	fileInode.BlocksLo = blocksNeeded * (BlockSize / 512)
	b.writeInode(newInodeNum, fileInode)

	// Add entry to parent directory
	b.addDirEntry(parentInodeNum, Ext4DirEntry2{
		Inode:    newInodeNum,
		FileType: FTRegFile,
		Name:     []byte(name),
	})

	fmt.Printf("  ✓ Created file: %s (inode %d, size=%d, uid=%d, gid=%d, mode=%04o)\n",
		name, newInodeNum, size, uid, gid, mode)

	return newInodeNum
}

// overwriteFile overwrites an existing file with new content, mode, uid, and gid
func (b *Ext4ImageBuilder) overwriteFile(inodeNum uint32, content []byte, mode, uid, gid uint16, name string) uint32 {
	// Free existing blocks
	b.freeInodeBlocks(inodeNum)

	// Calculate blocks needed for new content
	size := uint32(len(content))
	blocksNeeded := (size + BlockSize - 1) / BlockSize
	if blocksNeeded == 0 {
		blocksNeeded = 1
	}

	// Update inode with new metadata
	fileInode := b.createFileInode(mode, uid, gid, size)
	b.writeInode(inodeNum, fileInode)

	// Allocate new data blocks
	if blocksNeeded > 0 {
		blocks := b.allocateBlocks(blocksNeeded)
		b.setInodeBlocks(inodeNum, blocks)

		// Write content to blocks
		for i, blockNum := range blocks {
			startIdx := i * BlockSize
			if startIdx >= len(content) {
				break
			}
			endIdx := startIdx + BlockSize
			if endIdx > len(content) {
				endIdx = len(content)
			}

			blockOffset := b.blockOffset(blockNum)
			// Zero out the entire block first
			zeroed := make([]byte, BlockSize)
			b.writeAt(blockOffset, zeroed)
			// Then write actual content
			b.writeAt(blockOffset, content[startIdx:endIdx])
		}
	}

	// Update inode blocks count
	fileInode = b.readInode(inodeNum)
	fileInode.BlocksLo = blocksNeeded * (BlockSize / 512)
	b.writeInode(inodeNum, fileInode)

	fmt.Printf("  ✓ Overwritten file: %s (inode %d, size=%d, uid=%d, gid=%d, mode=%04o)\n",
		name, inodeNum, size, uid, gid, mode)

	return inodeNum
}

// CreateSymlink creates a symbolic link
func (b *Ext4ImageBuilder) CreateSymlink(parentInodeNum uint32, name, target string, uid, gid uint16) uint32 {
	newInodeNum := b.allocateInode()

	inode := &Ext4Inode{
		Mode:       S_IFLNK | 0777,
		UID:        uid,
		GID:        gid,
		SizeLo:     uint32(len(target)),
		LinksCount: 1,
		ATime:      b.createdAt,
		CTime:      b.createdAt,
		MTime:      b.createdAt,
		CrTime:     b.createdAt,
		ExtraIsize: 32,
	}

	// For short symlinks (< 60 bytes), store target in inode block area (fast symlink)
	// Fast symlinks must NOT have EXTENT_FL set - the Block field contains the path directly
	if len(target) < 60 {
		copy(inode.Block[:], target)
		inode.BlocksLo = 0
		inode.Flags = 0 // No EXTENT_FL for fast symlinks!
	} else {
		// For longer symlinks, use data block with extents
		inode.Flags = 0x00080000 // EXTENTS_FL
		b.initExtentHeader(inode)
		b.writeInode(newInodeNum, inode)

		dataBlock := b.allocateBlock()
		b.setInodeBlock(newInodeNum, dataBlock)

		blockOffset := b.blockOffset(dataBlock)
		b.writeAt(blockOffset, []byte(target))

		inode = b.readInode(newInodeNum)
		inode.BlocksLo = BlockSize / 512
	}

	b.writeInode(newInodeNum, inode)

	b.addDirEntry(parentInodeNum, Ext4DirEntry2{
		Inode:    newInodeNum,
		FileType: FTSymlink,
		Name:     []byte(name),
	})

	fmt.Printf("  ✓ Created symlink: %s -> %s (inode %d)\n", name, target, newInodeNum)

	return newInodeNum
}

// CreateLostFound creates the lost+found directory
func (b *Ext4ImageBuilder) CreateLostFound() uint32 {
	lfInode := b.allocateInode()

	// lost+found typically has multiple blocks preallocated
	dirInode := b.createDirInode(0700, 0, 0)
	b.writeInode(lfInode, dirInode)

	dataBlock := b.allocateBlock()
	b.setInodeBlock(lfInode, dataBlock)

	entries := []Ext4DirEntry2{
		{Inode: lfInode, FileType: FTDir, Name: []byte(".")},
		{Inode: RootInode, FileType: FTDir, Name: []byte("..")},
	}
	b.writeDirEntries(dataBlock, entries)

	// Update inode without losing the extent mapping
	dirInode = b.readInode(lfInode)
	dirInode.LinksCount = 2
	dirInode.SizeLo = BlockSize
	b.writeInode(lfInode, dirInode)

	// Add to root directory
	b.addDirEntry(RootInode, Ext4DirEntry2{
		Inode:    lfInode,
		FileType: FTDir,
		Name:     []byte("lost+found"),
	})

	// Increment root link count
	rootInode := b.readInode(RootInode)
	rootInode.LinksCount++
	b.writeInode(RootInode, rootInode)

	fmt.Println("✓ Created lost+found directory")

	return lfInode
}

// FinalizeMetadata recomputes superblock and group descriptor counters so
// the filesystem metadata (free blocks/inodes, used dirs, etc.) matches
// the actual bitmaps and inodes we have written.
func (b *Ext4ImageBuilder) FinalizeMetadata() {
	var totalFreeBlocks uint32
	var totalFreeInodes uint32

	// Group descriptor table starts at block 1
	gdtOffset := b.blockOffset(1)

	for g := uint32(0); g < b.groupCount; g++ {
		groupStart := g * BlocksPerGroup

		overhead := b.calculateGroupOverhead(g)

		blockBitmapBlock := groupStart + overhead
		inodeBitmapBlock := blockBitmapBlock + 1

		// Actual number of blocks in this group (last group may be short)
		blocksInGroup := uint32(BlocksPerGroup)
		if g == b.groupCount-1 {
			blocksInGroup = b.blockCount - g*BlocksPerGroup
		}

		// Count free blocks in this group by scanning the on-disk block bitmap.
		blockBitmapOffset := b.blockOffset(blockBitmapBlock)
		blockBitmap := make([]byte, BlockSize)
		b.readAt(blockBitmapOffset, blockBitmap)

		freeBlocks := uint32(0)
		for i := uint32(0); i < blocksInGroup; i++ {
			byteIndex := i / 8
			bitIndex := i % 8
			if (blockBitmap[byteIndex] & (1 << bitIndex)) == 0 {
				freeBlocks++
			}
		}
		totalFreeBlocks += freeBlocks

		// Count free inodes and directories in this group.
		inodeBitmapOffset := b.blockOffset(inodeBitmapBlock)
		inodeBitmap := make([]byte, BlockSize)
		b.readAt(inodeBitmapOffset, inodeBitmap)

		freeInodes := uint32(0)
		usedDirs := uint32(0)

		for i := uint32(0); i < InodesPerGroup; i++ {
			byteIndex := i / 8
			bitIndex := i % 8
			used := (inodeBitmap[byteIndex] & (1 << bitIndex)) != 0
			if !used {
				freeInodes++
				continue
			}

			inodeNum := g*InodesPerGroup + i + 1
			if inodeNum == 0 || inodeNum > b.inodesCount {
				continue
			}

			inode := b.readInode(inodeNum)
			if inode != nil && (inode.Mode&S_IFDIR) == S_IFDIR {
				usedDirs++
			}
		}
		totalFreeInodes += freeInodes

		// Read, update, and rewrite the group descriptor (32-byte on-disk form)
		descOffset := gdtOffset + uint64(g*32)
		descBuf := make([]byte, 32)
		b.readAt(descOffset, descBuf)

		var gd Ext4GroupDesc32
		reader := bytes.NewReader(descBuf)
		if err := binary.Read(reader, binary.LittleEndian, &gd); err != nil {
			panic(fmt.Sprintf("failed to read group descriptor %d: %v", g, err))
		}

		gd.FreeBlocksCountLo = uint16(freeBlocks)
		gd.FreeInodesCountLo = uint16(freeInodes)
		gd.UsedDirsCountLo = uint16(usedDirs)
		gd.ItableUnusedLo = uint16(freeInodes)

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.LittleEndian, &gd); err != nil {
			panic(fmt.Sprintf("failed to write group descriptor %d: %v", g, err))
		}
		b.writeAt(descOffset, buf.Bytes()[:32])
	}

	// Update the primary superblock
	sbOffset := b.partOffset(1024)
	var sb Ext4Superblock
	sbSize := binary.Size(sb)

	sbBuf := make([]byte, sbSize)
	b.readAt(sbOffset, sbBuf)

	reader := bytes.NewReader(sbBuf)
	if err := binary.Read(reader, binary.LittleEndian, &sb); err != nil {
		panic(fmt.Sprintf("failed to read superblock: %v", err))
	}

	sb.FreeBlocksCountLo = totalFreeBlocks
	sb.FreeBlocksCountHi = 0
	sb.FreeInodesCount = totalFreeInodes

	// We are not advertising metadata checksums, so we can leave Checksum
	// as zero and simply rewrite the structure.
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, &sb); err != nil {
		panic(fmt.Sprintf("failed to write superblock: %v", err))
	}
	b.writeAt(sbOffset, buf.Bytes())
}

// Save is implemented in ext4_disk.go where the on-disk backend is configured.

// partOffset returns the absolute offset from partition-relative offset
func (b *Ext4ImageBuilder) partOffset(offset uint64) uint64 {
	return b.partitionStart + offset
}

// blockOffset returns the absolute offset for a block number
func (b *Ext4ImageBuilder) blockOffset(blockNum uint32) uint64 {
	return b.partOffset(uint64(blockNum) * BlockSize)
}

// writeAt writes data at an absolute position on the underlying disk.
func (b *Ext4ImageBuilder) writeAt(offset uint64, data []byte) {
	if len(data) == 0 {
		return
	}
	if b.disk == nil {
		panic("writeAt called with nil disk backend")
	}

	off := int64(offset)
	if off < 0 {
		panic(fmt.Sprintf("writeAt offset overflows int64: %d", offset))
	}

	if _, err := b.disk.WriteAt(data, off); err != nil {
		panic(fmt.Sprintf("failed to write %d bytes at offset %d: %v", len(data), offset, err))
	}
}

// readAt reads len(buf) bytes from the underlying disk at the given absolute offset.
func (b *Ext4ImageBuilder) readAt(offset uint64, buf []byte) {
	if len(buf) == 0 {
		return
	}
	if b.disk == nil {
		panic("readAt called with nil disk backend")
	}

	off := int64(offset)
	if off < 0 {
		panic(fmt.Sprintf("readAt offset overflows int64: %d", offset))
	}

	n, err := b.disk.ReadAt(buf, off)
	if err != nil {
		panic(fmt.Sprintf("failed to read %d bytes at offset %d: %v", len(buf), offset, err))
	}
	if n != len(buf) {
		panic(fmt.Sprintf("short read at offset %d: expected %d bytes, got %d", offset, len(buf), n))
	}
}

// writeMBR writes the Master Boot Record.
func (b *Ext4ImageBuilder) writeMBR() {
	mbr := MBR{
		Signature: MBRMagic,
	}

	// Calculate partition in sectors
	startSector := uint32(b.partitionStart / SectorSize)
	sectorCount := uint32(b.partitionSize / SectorSize)

	// Create a single Linux partition (type 0x83)
	mbr.Partitions[0] = MBRPartitionEntry{
		Status:      0x00, // Not bootable
		Type:        0x83, // Linux
		StartLBA:    startSector,
		SectorCount: sectorCount,
	}

	// Convert start LBA to CHS (simplified)
	mbr.Partitions[0].StartCHS = lbaToCHS(startSector)
	mbr.Partitions[0].EndCHS = lbaToCHS(startSector + sectorCount - 1)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, mbr)
	b.writeAt(0, buf.Bytes())

	fmt.Println("✓ MBR written with Linux partition")
}

// calculateGroupOverhead calculates the overhead blocks at the start of a group
// (superblock backup, GDT, reserved GDT). Does NOT include bitmaps or inode table.
func (b *Ext4ImageBuilder) calculateGroupOverhead(groupNum uint32) uint32 {
	if groupNum == 0 || isSparseGroup(groupNum) {
		// Superblock + GDT blocks (no reserved GDT blocks - we're not using resize feature)
		gdtBlocks := (b.groupCount*32 + BlockSize - 1) / BlockSize
		return 1 + gdtBlocks
	}
	return 0
}

// calculateOverhead calculates total filesystem overhead blocks for a group
// including bitmaps and inode table
func (b *Ext4ImageBuilder) calculateOverhead(groupNum uint32) uint32 {
	overhead := b.calculateGroupOverhead(groupNum)

	overhead += 2 // Block bitmap + Inode bitmap

	inodeTableBlocks := uint32((InodesPerGroup*InodeSize + BlockSize - 1) / BlockSize)
	overhead += inodeTableBlocks

	return overhead
}

// writeSuperblock writes the superblock.
func (b *Ext4ImageBuilder) writeSuperblock() {
	sb := Ext4Superblock{
		InodesCount:       b.inodesCount,
		BlocksCountLo:     b.blockCount,
		RBlocksCountLo:    b.blockCount / 20, // 5% reserved
		FreeBlocksCountLo: b.blockCount,      // Will be updated
		FreeInodesCount:   b.inodesCount - FirstNonResInode + 1,
		FirstDataBlock:    FirstDataBlock,
		LogBlockSize:      2, // 4096 = 2^(10+2)
		LogClusterSize:    2,
		BlocksPerGroup:    BlocksPerGroup,
		ClustersPerGroup:  BlocksPerGroup,
		InodesPerGroup:    InodesPerGroup,
		MTime:             0,
		WTime:             b.createdAt,
		MntCount:          0,
		MaxMntCount:       0xFFFF,
		Magic:             Ext4Magic,
		State:             1, // Clean
		Errors:            1, // Continue
		MinorRevLevel:     0,
		LastCheck:         b.createdAt,
		CheckInterval:     0,
		CreatorOS:         0, // Linux
		RevLevel:          1, // Dynamic
		DefResUID:         0,
		DefResGID:         0,
		FirstIno:          FirstNonResInode,
		InodeSize:         InodeSize,
		BlockGroupNr:      0,
		FeatureCompat:     CompatExtAttr | CompatDirIndex,
		FeatureIncompat:   IncompatFiletype | IncompatExtents,
		FeatureROCompat:   ROCompatSparseSuper | ROCompatLargeFile | ROCompatExtraIsize,
		MkfsTime:          b.createdAt,
		DescSize:          32, // 32-byte group descriptors (not 64-bit mode)
		MinExtraIsize:     32,
		WantExtraIsize:    32,
		LogGroupsPerFlex:  0, // Disable flex_bg
		ReservedGDTBlocks: 0, // Not using resize feature
	}

	// Generate a simple UUID
	for i := 0; i < 16; i++ {
		sb.UUID[i] = byte((b.createdAt>>uint(i%4*8))&0xFF) ^ byte(i*17)
	}

	// Set volume name
	copy(sb.VolumeName[:], "ext4-pure-go")

	// Generate hash seed
	for i := 0; i < 4; i++ {
		sb.HashSeed[i] = b.createdAt + uint32(i*0x12345678)
	}
	sb.DefHashVersion = 1 // Half MD4

	// Calculate first free block after group 0 overhead
	b.nextFreeBlock = b.calculateOverhead(0)

	// Calculate total free blocks
	usedBlocks := uint32(0)
	for g := uint32(0); g < b.groupCount; g++ {
		usedBlocks += b.calculateOverhead(g)
	}
	sb.FreeBlocksCountLo = b.blockCount - usedBlocks

	// Write superblock at offset 1024 within the partition
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, sb)

	b.writeAt(b.partOffset(1024), buf.Bytes())

	fmt.Printf("✓ Superblock written (blocks: %d, inodes: %d, groups: %d)\n",
		b.blockCount, b.inodesCount, b.groupCount)
}

// writeGroupDescriptors writes group descriptors.
func (b *Ext4ImageBuilder) writeGroupDescriptors() {
	// GDT starts at block 1 (after superblock which is in block 0)
	gdtOffset := b.blockOffset(1)

	for g := uint32(0); g < b.groupCount; g++ {
		groupStart := g * BlocksPerGroup

		// Calculate block positions for this group
		overhead := b.calculateGroupOverhead(g)

		blockBitmap := groupStart + overhead
		inodeBitmap := blockBitmap + 1
		inodeTable := inodeBitmap + 1
		inodeTableBlocks := uint32((InodesPerGroup*InodeSize + BlockSize - 1) / BlockSize)

		// Calculate initial free blocks (total - overhead - bitmaps - inode table)
		totalOverhead := overhead + 2 + inodeTableBlocks

		// For the last group, we might have fewer blocks
		blocksInGroup := uint32(BlocksPerGroup)
		if g == b.groupCount-1 {
			blocksInGroup = b.blockCount - g*BlocksPerGroup
		}

		freeBlocks := uint16(0)
		if blocksInGroup > totalOverhead {
			freeBlocks = uint16(blocksInGroup - totalOverhead)
		}

		gd := Ext4GroupDesc32{
			BlockBitmapLo:     blockBitmap,
			InodeBitmapLo:     inodeBitmap,
			InodeTableLo:      inodeTable,
			FreeBlocksCountLo: freeBlocks,
			FreeInodesCountLo: InodesPerGroup,
			UsedDirsCountLo:   0,
			Flags:             0, // No INODE_ZEROED - we're not using checksums
			ItableUnusedLo:    InodesPerGroup,
		}

		if g == 0 {
			gd.FreeInodesCountLo = InodesPerGroup - FirstNonResInode + 1
			gd.ItableUnusedLo = InodesPerGroup - FirstNonResInode + 1
			gd.UsedDirsCountLo = 2
		}

		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, gd)
		b.writeAt(gdtOffset+uint64(g*32), buf.Bytes())
	}

	fmt.Printf("✓ Group descriptors written (%d groups)\n", b.groupCount)
}

// writeBitmaps writes block and inode bitmaps.
func (b *Ext4ImageBuilder) writeBitmaps() {
	for g := uint32(0); g < b.groupCount; g++ {
		groupStart := g * BlocksPerGroup

		overhead := b.calculateGroupOverhead(g)

		blockBitmapBlock := groupStart + overhead
		inodeBitmapBlock := blockBitmapBlock + 1
		inodeTableBlocks := uint32((InodesPerGroup*InodeSize + BlockSize - 1) / BlockSize)

		// Create block bitmap
		blockBitmap := make([]byte, BlockSize)

		// Total overhead = group overhead + bitmaps + inode table
		usedBlocks := overhead + 2 + inodeTableBlocks

		// Mark used blocks in bitmap
		for i := uint32(0); i < usedBlocks && i < BlocksPerGroup; i++ {
			blockBitmap[i/8] |= 1 << (i % 8)
		}

		// Actual number of blocks in this group (last group may be short)
		blocksInGroup := uint32(BlocksPerGroup)
		if g == b.groupCount-1 {
			blocksInGroup = b.blockCount - g*BlocksPerGroup
		}

		// Mark blocks beyond this group's valid range as used (padding)
		for i := blocksInGroup; i < BlocksPerGroup; i++ {
			blockBitmap[i/8] |= 1 << (i % 8)
		}

		// Also mark any remaining bytes as 0xFF for padding beyond BlocksPerGroup
		bitmapBitsUsed := BlocksPerGroup
		bitmapBytesUsed := (bitmapBitsUsed + 7) / 8
		for i := bitmapBytesUsed; i < BlockSize; i++ {
			blockBitmap[i] = 0xFF
		}

		b.writeAt(b.blockOffset(blockBitmapBlock), blockBitmap)

		// Create inode bitmap
		inodeBitmap := make([]byte, BlockSize)

		// For group 0, mark reserved inodes as used (inodes 1-10)
		if g == 0 {
			for i := uint32(0); i < FirstNonResInode-1; i++ {
				inodeBitmap[i/8] |= 1 << (i % 8)
			}
		}

		// Mark padding bits at the end of the inode bitmap as used
		// InodesPerGroup bits are valid, the rest should be 1
		inodesInGroup := uint32(InodesPerGroup)
		inodeBitmapBytes := (inodesInGroup + 7) / 8 // bytes needed for inode bits

		// Set all remaining bytes to 0xFF (mark non-existent inodes as "used")
		for i := inodeBitmapBytes; i < BlockSize; i++ {
			inodeBitmap[i] = 0xFF
		}

		// Handle partial byte at the end of valid inodes
		if inodesInGroup%8 != 0 {
			lastByteIdx := inodeBitmapBytes - 1
			validBits := inodesInGroup % 8
			// Set bits beyond validBits to 1
			for bit := validBits; bit < 8; bit++ {
				inodeBitmap[lastByteIdx] |= 1 << bit
			}
		}

		b.writeAt(b.blockOffset(inodeBitmapBlock), inodeBitmap)
	}

	fmt.Println("✓ Block and inode bitmaps written")
}

// writeRootDirectory creates the root directory.
func (b *Ext4ImageBuilder) writeRootDirectory() {
	// Root inode
	rootInode := b.createDirInode(0755, 0, 0)
	b.writeInode(RootInode, rootInode)

	// Allocate a block for root directory data
	rootDirBlock := b.allocateBlock()
	b.setInodeBlock(RootInode, rootDirBlock)

	// Create directory entries for . and ..
	entries := []Ext4DirEntry2{
		{Inode: RootInode, FileType: FTDir, Name: []byte(".")},
		{Inode: RootInode, FileType: FTDir, Name: []byte("..")},
	}

	b.writeDirEntries(rootDirBlock, entries)

	// Update inode with link count and size without clobbering the extent tree
	rootInode = b.readInode(RootInode)
	rootInode.LinksCount = 2
	rootInode.SizeLo = BlockSize
	b.writeInode(RootInode, rootInode)

	// Mark inode as used in bitmap
	b.markInodeUsed(RootInode)

	fmt.Println("✓ Root directory created")
}

// createDirInode creates an inode for a directory
func (b *Ext4ImageBuilder) createDirInode(mode, uid, gid uint16) *Ext4Inode {
	inode := &Ext4Inode{
		Mode:       S_IFDIR | mode,
		UID:        uid,
		GID:        gid,
		LinksCount: 2,
		BlocksLo:   8,          // 4096 / 512
		Flags:      0x00080000, // EXTENTS_FL
		ATime:      b.createdAt,
		CTime:      b.createdAt,
		MTime:      b.createdAt,
		CrTime:     b.createdAt,
		ExtraIsize: 32,
	}

	// Initialize extent header
	b.initExtentHeader(inode)

	return inode
}

// createFileInode creates an inode for a regular file
func (b *Ext4ImageBuilder) createFileInode(mode, uid, gid uint16, size uint32) *Ext4Inode {
	blocks := (size + BlockSize - 1) / BlockSize

	inode := &Ext4Inode{
		Mode:       S_IFREG | mode,
		UID:        uid,
		GID:        gid,
		SizeLo:     size,
		LinksCount: 1,
		BlocksLo:   blocks * (BlockSize / 512),
		Flags:      0x00080000, // EXTENTS_FL
		ATime:      b.createdAt,
		CTime:      b.createdAt,
		MTime:      b.createdAt,
		CrTime:     b.createdAt,
		ExtraIsize: 32,
	}

	// Initialize extent header
	b.initExtentHeader(inode)

	return inode
}

// initExtentHeader initializes the extent header in an inode
func (b *Ext4ImageBuilder) initExtentHeader(inode *Ext4Inode) {
	header := Ext4ExtentHeader{
		Magic:   0xF30A,
		Entries: 0,
		Max:     4, // Maximum extents in inode
		Depth:   0, // Leaf level
	}

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, header)
	copy(inode.Block[:], buf.Bytes())
}

// setInodeBlock sets the data block for an inode using extents
func (b *Ext4ImageBuilder) setInodeBlock(inodeNum, blockNum uint32) {
	// Read the inode
	inode := b.readInode(inodeNum)

	// Update extent header to have 1 entry
	binary.LittleEndian.PutUint16(inode.Block[2:4], 1) // entries

	// Write extent at offset 12 (after header)
	extent := Ext4Extent{
		Block:   0,
		Len:     1,
		StartHi: 0,
		StartLo: blockNum,
	}

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, extent)
	copy(inode.Block[12:], buf.Bytes())

	b.writeInode(inodeNum, inode)
}

// setInodeBlocks sets multiple data blocks for an inode using extents
func (b *Ext4ImageBuilder) setInodeBlocks(inodeNum uint32, blocks []uint32) {
	if len(blocks) == 0 {
		return
	}

	inode := b.readInode(inodeNum)

	// For simplicity, assume blocks are contiguous
	// Update extent header
	binary.LittleEndian.PutUint16(inode.Block[2:4], 1) // entries

	extent := Ext4Extent{
		Block:   0,
		Len:     uint16(len(blocks)),
		StartHi: 0,
		StartLo: blocks[0],
	}

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, extent)
	copy(inode.Block[12:], buf.Bytes())

	b.writeInode(inodeNum, inode)
}

// writeInode writes an inode to the inode table
func (b *Ext4ImageBuilder) writeInode(inodeNum uint32, inode *Ext4Inode) {
	if inodeNum < 1 {
		return
	}

	group := (inodeNum - 1) / InodesPerGroup
	indexInGroup := (inodeNum - 1) % InodesPerGroup

	// Get inode table block from group descriptor
	inodeTableBlock := b.getInodeTableBlock(group)

	offset := b.blockOffset(inodeTableBlock) + uint64(indexInGroup)*InodeSize

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, inode)
	b.writeAt(offset, buf.Bytes())
}

// readInode reads an inode from the inode table
func (b *Ext4ImageBuilder) readInode(inodeNum uint32) *Ext4Inode {
	if inodeNum < 1 {
		return nil
	}

	group := (inodeNum - 1) / InodesPerGroup
	indexInGroup := (inodeNum - 1) % InodesPerGroup

	inodeTableBlock := b.getInodeTableBlock(group)
	offset := b.blockOffset(inodeTableBlock) + uint64(indexInGroup)*InodeSize

	inode := &Ext4Inode{}
	buf := make([]byte, InodeSize)
	b.readAt(offset, buf)
	reader := bytes.NewReader(buf)
	if err := binary.Read(reader, binary.LittleEndian, inode); err != nil {
		panic(fmt.Sprintf("failed to read inode %d: %v", inodeNum, err))
	}

	return inode
}

// getInodeTableBlock returns the inode table start block for a group
func (b *Ext4ImageBuilder) getInodeTableBlock(group uint32) uint32 {
	groupStart := group * BlocksPerGroup

	overhead := b.calculateGroupOverhead(group)

	// Block bitmap + inode bitmap + inode table start
	return groupStart + overhead + 2
}

// allocateBlock allocates a new block and returns its number
func (b *Ext4ImageBuilder) allocateBlock() uint32 {
	// Ensure we never allocate beyond the end of the filesystem
	if b.nextFreeBlock >= b.blockCount {
		panic(fmt.Sprintf("out of data blocks: next=%d total=%d", b.nextFreeBlock, b.blockCount))
	}

	block := b.nextFreeBlock
	b.nextFreeBlock++

	// Mark block as used in bitmap
	b.markBlockUsed(block)

	return block
}

// allocateBlocks allocates multiple contiguous blocks
func (b *Ext4ImageBuilder) allocateBlocks(count uint32) []uint32 {
	blocks := make([]uint32, count)
	for i := uint32(0); i < count; i++ {
		blocks[i] = b.allocateBlock()
	}
	return blocks
}

// markBlockUsed marks a block as used in the block bitmap
func (b *Ext4ImageBuilder) markBlockUsed(blockNum uint32) {
	group := blockNum / BlocksPerGroup
	indexInGroup := blockNum % BlocksPerGroup

	groupStart := group * BlocksPerGroup
	overhead := b.calculateGroupOverhead(group)

	blockBitmapBlock := groupStart + overhead
	bitmapOffset := b.blockOffset(blockBitmapBlock)

	byteIndex := indexInGroup / 8
	bitIndex := indexInGroup % 8

	offset := bitmapOffset + uint64(byteIndex)
	buf := make([]byte, 1)
	b.readAt(offset, buf)
	buf[0] |= 1 << bitIndex
	b.writeAt(offset, buf)
}

// markInodeUsed marks an inode as used in the inode bitmap
func (b *Ext4ImageBuilder) markInodeUsed(inodeNum uint32) {
	group := (inodeNum - 1) / InodesPerGroup
	indexInGroup := (inodeNum - 1) % InodesPerGroup

	groupStart := group * BlocksPerGroup
	overhead := b.calculateGroupOverhead(group)

	inodeBitmapBlock := groupStart + overhead + 1
	bitmapOffset := b.blockOffset(inodeBitmapBlock)

	byteIndex := indexInGroup / 8
	bitIndex := indexInGroup % 8

	offset := bitmapOffset + uint64(byteIndex)
	buf := make([]byte, 1)
	b.readAt(offset, buf)
	buf[0] |= 1 << bitIndex
	b.writeAt(offset, buf)
}

// allocateInode allocates a new inode and returns its number
func (b *Ext4ImageBuilder) allocateInode() uint32 {
	// Ensure we never allocate beyond the number of inodes described
	if b.nextFreeInode > b.inodesCount {
		panic(fmt.Sprintf("out of inodes: next=%d total=%d", b.nextFreeInode, b.inodesCount))
	}

	inode := b.nextFreeInode
	b.nextFreeInode++
	b.markInodeUsed(inode)
	return inode
}

// writeDirEntries writes directory entries to a single data block.
func (b *Ext4ImageBuilder) writeDirEntries(blockNum uint32, entries []Ext4DirEntry2) {
	offset := b.blockOffset(blockNum)
	block := make([]byte, BlockSize)
	currentOffset := 0

	for i, entry := range entries {
		nameLen := uint8(len(entry.Name))
		// Record length must be 4-byte aligned
		recLen := uint16(8 + nameLen)
		if recLen%4 != 0 {
			recLen += 4 - (recLen % 4)
		}

		// Last entry takes remaining space in block
		if i == len(entries)-1 {
			recLen = uint16(BlockSize - currentOffset)
		}

		off := currentOffset
		// Write entry header
		binary.LittleEndian.PutUint32(block[off:], entry.Inode)
		binary.LittleEndian.PutUint16(block[off+4:], recLen)
		block[off+6] = nameLen
		block[off+7] = entry.FileType
		copy(block[off+8:], entry.Name)

		currentOffset += int(recLen)
	}

	b.writeAt(offset, block)
}

// findInodeByName searches for a file/directory by name in a directory and returns its inode number.
// Returns 0 if not found.
func (b *Ext4ImageBuilder) findInodeByName(dirInodeNum uint32, name string) uint32 {
	// Read directory inode to get data block
	dirInode := b.readInode(dirInodeNum)

	// Get the data block from extent
	dataBlock := binary.LittleEndian.Uint32(dirInode.Block[20:24]) // extent start_lo

	// Read directory block
	offset := b.blockOffset(dataBlock)
	block := make([]byte, BlockSize)
	b.readAt(offset, block)

	// Search for the entry
	currentOffset := 0
	for currentOffset < BlockSize {
		recLen := binary.LittleEndian.Uint16(block[currentOffset+4:])
		if recLen == 0 {
			break
		}

		nameLen := block[currentOffset+6]
		entryName := string(block[currentOffset+8 : currentOffset+8+int(nameLen)])

		if entryName == name {
			inodeNum := binary.LittleEndian.Uint32(block[currentOffset:])
			return inodeNum
		}

		currentOffset += int(recLen)
		if currentOffset >= BlockSize {
			break
		}
	}

	return 0 // Not found
}

// freeInodeBlocks frees all data blocks allocated to an inode
func (b *Ext4ImageBuilder) freeInodeBlocks(inodeNum uint32) {
	inode := b.readInode(inodeNum)
	if inode == nil {
		return
	}

	// Only handle extent-based files
	if (inode.Flags & 0x00080000) == 0 {
		return // Not using extents
	}

	// Read extent header
	entries := binary.LittleEndian.Uint16(inode.Block[2:4])

	// Free each extent
	for i := uint16(0); i < entries && i < 4; i++ {
		extentOffset := 12 + (i * 12) // Each extent is 12 bytes, header is 12 bytes
		if extentOffset+12 > 60 {
			break
		}

		// Ext4Extent structure: Block(4), Len(2), StartHi(2), StartLo(4)
		length := binary.LittleEndian.Uint16(inode.Block[extentOffset+4:])
		startLo := binary.LittleEndian.Uint32(inode.Block[extentOffset+8:])

		// Free each block in the extent
		for j := uint16(0); j < length; j++ {
			blockNum := startLo + uint32(j)
			b.freeBlock(blockNum)
		}
	}
}

// freeBlock marks a block as free in the block bitmap
func (b *Ext4ImageBuilder) freeBlock(blockNum uint32) {
	if blockNum >= b.blockCount {
		return
	}

	group := blockNum / BlocksPerGroup
	indexInGroup := blockNum % BlocksPerGroup

	groupStart := group * BlocksPerGroup
	overhead := b.calculateGroupOverhead(group)

	blockBitmapBlock := groupStart + overhead
	bitmapOffset := b.blockOffset(blockBitmapBlock)

	byteIndex := indexInGroup / 8
	bitIndex := indexInGroup % 8

	offset := bitmapOffset + uint64(byteIndex)
	buf := make([]byte, 1)
	b.readAt(offset, buf)
	buf[0] &^= 1 << bitIndex // Clear the bit
	b.writeAt(offset, buf)
}

// addDirEntry adds a new entry to a directory
func (b *Ext4ImageBuilder) addDirEntry(dirInodeNum uint32, newEntry Ext4DirEntry2) {
	// Read directory inode to get data block
	dirInode := b.readInode(dirInodeNum)

	// Get the data block from extent
	dataBlock := binary.LittleEndian.Uint32(dirInode.Block[20:24]) // extent start_lo

	// Read existing entries into memory
	offset := b.blockOffset(dataBlock)
	block := make([]byte, BlockSize)
	b.readAt(offset, block)

	// Find the last entry and shrink it
	currentOffset := 0
	lastEntryOffset := 0
	var lastRecLen uint16

	for currentOffset < BlockSize {
		recLen := binary.LittleEndian.Uint16(block[currentOffset+4:])
		if recLen == 0 {
			break
		}

		lastEntryOffset = currentOffset
		lastRecLen = recLen
		currentOffset += int(recLen)

		if currentOffset >= BlockSize {
			break
		}
	}

	// Calculate actual size of last entry
	lastNameLen := block[lastEntryOffset+6]
	lastActualSize := uint16(8 + lastNameLen)
	if lastActualSize%4 != 0 {
		lastActualSize += 4 - (lastActualSize % 4)
	}

	// Calculate new entry size
	newNameLen := uint8(len(newEntry.Name))
	newRecLen := uint16(8 + newNameLen)
	if newRecLen%4 != 0 {
		newRecLen += 4 - (newRecLen % 4)
	}

	// Check if there's room
	spaceAvailable := lastRecLen - lastActualSize
	if spaceAvailable >= newRecLen {
		// Shrink last entry
		binary.LittleEndian.PutUint16(block[lastEntryOffset+4:], lastActualSize)

		// Write new entry
		newOffset := lastEntryOffset + int(lastActualSize)
		remainingSpace := BlockSize - newOffset

		binary.LittleEndian.PutUint32(block[newOffset:], newEntry.Inode)
		binary.LittleEndian.PutUint16(block[newOffset+4:], uint16(remainingSpace))
		block[newOffset+6] = newNameLen
		block[newOffset+7] = newEntry.FileType
		copy(block[newOffset+8:], newEntry.Name)

		// Persist updated directory block.
		b.writeAt(offset, block)
		return
	}

	// For now, fail fast if a directory overflows its single block.
	// This avoids silently creating an inconsistent filesystem image.
	panic(fmt.Sprintf("directory inode %d is full; multi-block directories are not yet supported", dirInodeNum))
}

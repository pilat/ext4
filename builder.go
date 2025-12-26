package ext4fs

import (
	"fmt"
)

var DEBUG = false

type builder struct {
	disk   diskBackend
	layout *Layout
	debug  bool // Enable debug output

	// Allocation state - per group
	nextBlockPerGroup   []uint32 // Next free block in each group
	freedBlocksPerGroup []uint32 // Blocks freed per group (for overwrites)
	freeBlockList       []uint32 // List of freed blocks available for reuse
	nextInode           uint32   // Next free inode (global)
	freedInodesPerGroup []uint32 // Inodes freed per group (for deletes)
	freeInodeList       []uint32 // List of freed inodes available for reuse

	// Tracking
	usedDirsPerGroup []uint16 // Directory count per group
}

// newBuilder creates a new Builder instance with initialized allocation state.
// It sets up per-group tracking for block and inode allocation, preparing
// the builder for filesystem construction operations.
func newBuilder(disk diskBackend, layout *Layout) *builder {
	b := &builder{
		disk:                disk,
		layout:              layout,
		debug:               DEBUG,
		nextBlockPerGroup:   make([]uint32, layout.GroupCount),
		freedBlocksPerGroup: make([]uint32, layout.GroupCount),
		freeBlockList:       make([]uint32, 0),
		nextInode:           firstNonResInode,
		freedInodesPerGroup: make([]uint32, layout.GroupCount),
		freeInodeList:       make([]uint32, 0),
		usedDirsPerGroup:    make([]uint16, layout.GroupCount),
	}

	// Initialize next free block for each group
	for g := uint32(0); g < layout.GroupCount; g++ {
		gl := layout.GetGroupLayout(g)
		b.nextBlockPerGroup[g] = gl.FirstDataBlock
	}

	return b
}

// loadBitmaps reads existing block and inode bitmaps from an opened ext4 image.
// It scans the bitmaps to determine which blocks and inodes are already allocated,
// enabling proper allocation state for modification operations.
//
// For each block group, it:
//   - Scans the block bitmap to find the first free block (sets nextBlockPerGroup)
//   - Scans the inode bitmap to find the highest allocated inode (sets nextInode)
//   - Reads the group descriptor to get the directory count (sets usedDirsPerGroup)
//
// This must be called after newBuilder when opening an existing image.
func (b *builder) loadBitmaps() error {
	// Track the highest allocated inode to set nextInode correctly
	highestInode := uint32(firstNonResInode - 1)

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		// Read and scan block bitmap
		blockBitmap := make([]byte, blockSize)
		if err := b.disk.readAt(blockBitmap, int64(b.layout.BlockOffset(gl.BlockBitmapBlock))); err != nil {
			return fmt.Errorf("read block bitmap for group %d: %w", g, err)
		}

		// Scan block bitmap to find:
		// 1. Highest allocated block (sets nextBlockPerGroup for new allocations)
		// 2. Any free "holes" before that (adds to freeBlockList for reuse)
		highestUsed := gl.FirstDataBlock - gl.GroupStart - 1 // Start before data area
		dataStart := gl.FirstDataBlock - gl.GroupStart
		for i := dataStart; i < gl.BlocksInGroup; i++ {
			byteIdx := i / 8
			bitIdx := i % 8
			if blockBitmap[byteIdx]&(1<<bitIdx) != 0 {
				highestUsed = i
			}
		}

		// nextBlockPerGroup is one past the highest used block
		b.nextBlockPerGroup[g] = gl.GroupStart + highestUsed + 1

		// Find free blocks (holes) before nextBlockPerGroup and add to freeBlockList
		for i := dataStart; i <= highestUsed; i++ {
			byteIdx := i / 8
			bitIdx := i % 8
			if blockBitmap[byteIdx]&(1<<bitIdx) == 0 {
				b.freeBlockList = append(b.freeBlockList, gl.GroupStart+i)
			}
		}

		// Read and scan inode bitmap
		inodeBitmap := make([]byte, blockSize)
		if err := b.disk.readAt(inodeBitmap, int64(b.layout.BlockOffset(gl.InodeBitmapBlock))); err != nil {
			return fmt.Errorf("read inode bitmap for group %d: %w", g, err)
		}

		// Find highest allocated inode in this group
		for i := uint32(0); i < b.layout.InodesPerGroup; i++ {
			byteIdx := i / 8
			bitIdx := i % 8
			if inodeBitmap[byteIdx]&(1<<bitIdx) != 0 {
				inodeNum := g*b.layout.InodesPerGroup + i + 1
				if inodeNum > highestInode {
					highestInode = inodeNum
				}
			}
		}

		// Read group descriptor to get used directories count
		// UsedDirsCountLo is at offset 16-17 in groupDesc32
		gdtOffset := b.layout.BlockOffset(b.layout.GetGroupLayout(0).GDTStart) + uint64(g*32)
		gdData := make([]byte, 32)
		if err := b.disk.readAt(gdData, int64(gdtOffset)); err != nil {
			return fmt.Errorf("read group descriptor %d: %w", g, err)
		}
		b.usedDirsPerGroup[g] = uint16(gdData[16]) | uint16(gdData[17])<<8
	}

	b.nextInode = highestInode + 1

	if b.debug {
		fmt.Printf("✓ Bitmaps loaded (next inode: %d)\n", b.nextInode)
	}

	return nil
}

// prepareFilesystem initializes the complete ext4 filesystem structure.
// This includes writing the superblock, group descriptors, initializing
// bitmaps, zeroing inode tables, and creating essential directories like
// root and lost+found. This method must be called before any file operations.
func (b *builder) prepareFilesystem() error {
	if b.debug {
		fmt.Println(b.layout.String())
		fmt.Println()
	}

	if err := b.writeSuperblock(); err != nil {
		return err
	}

	if err := b.writeGroupDescriptors(); err != nil {
		return err
	}

	if err := b.initBitmaps(); err != nil {
		return err
	}

	if err := b.zeroInodeTables(); err != nil {
		return err
	}

	if err := b.createRootDirectory(); err != nil {
		return err
	}

	if err := b.createLostFound(); err != nil {
		return err
	}

	if DEBUG {
		fmt.Println("✓ Filesystem prepared successfully")
	}

	return nil
}

package ext4fs

import (
	"fmt"
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

	if err := b.writeMBR(); err != nil {
		return err
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

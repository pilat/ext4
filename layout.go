package ext4fs

import (
	"fmt"
)

// Layout contains pre-calculated filesystem layout parameters.
// This structure holds all the geometry and metadata positioning information
// needed to construct an ext4 filesystem with proper block and inode allocation.
type Layout struct {
	// Partition geometry
	PartitionStart uint64
	PartitionSize  uint64
	TotalBlocks    uint32

	// Group geometry
	GroupCount     uint32
	BlocksPerGroup uint32
	InodesPerGroup uint32

	// Per-group metadata (computed for each group)
	InodeTableBlocks uint32

	// Timestamps
	CreatedAt uint32
}

// GroupLayout holds the block positions and metadata layout for a specific block group.
// Each block group contains its own metadata (bitmaps, inode table) and data blocks.
// This structure defines where each component is located within the group.
type GroupLayout struct {
	GroupStart       uint32 // First block of this group
	SuperblockBlock  uint32 // 0 if no superblock backup
	GDTStart         uint32 // 0 if no GDT backup
	GDTBlocks        uint32
	BlockBitmapBlock uint32
	InodeBitmapBlock uint32
	InodeTableStart  uint32
	FirstDataBlock   uint32 // First usable data block in this group
	BlocksInGroup    uint32 // Actual blocks in this group (last may be smaller)
	OverheadBlocks   uint32 // Metadata blocks in this group
}

// CalculateLayout computes the complete filesystem layout based on partition geometry.
// Determines the number of block groups, block and inode distribution, and metadata placement.
// The layout ensures optimal performance and compatibility with ext4 specifications.
func CalculateLayout(partitionStart, partitionSize uint64, createdAt uint32) (*Layout, error) {
	if partitionSize < 4*1024*1024 {
		return nil, fmt.Errorf("partition too small: need at least 4MB, got %d", partitionSize)
	}

	totalBlocks := uint32(partitionSize / BlockSize)
	groupCount := (totalBlocks + BlocksPerGroup - 1) / BlocksPerGroup

	// Limit to reasonable number of groups for now
	if groupCount > 256 {
		groupCount = 256
		totalBlocks = groupCount * BlocksPerGroup
	}

	l := &Layout{
		PartitionStart:   partitionStart,
		PartitionSize:    partitionSize,
		TotalBlocks:      totalBlocks,
		GroupCount:       groupCount,
		BlocksPerGroup:   BlocksPerGroup,
		InodesPerGroup:   InodesPerGroup,
		InodeTableBlocks: (InodesPerGroup * InodeSize) / BlockSize,
		CreatedAt:        createdAt,
	}

	return l, nil
}

// GetGroupLayout calculates the detailed layout for a specific block group.
// Determines the positions of superblock backups, group descriptors, bitmaps,
// inode tables, and data blocks within the group. Accounts for sparse superblock
// placement to optimize metadata distribution.
func (l *Layout) GetGroupLayout(group uint32) GroupLayout {
	gl := GroupLayout{
		GroupStart: group * BlocksPerGroup,
	}

	// Calculate actual blocks in this group
	if group == l.GroupCount-1 {
		// Last group may have fewer blocks
		gl.BlocksInGroup = l.TotalBlocks - gl.GroupStart
	} else {
		gl.BlocksInGroup = BlocksPerGroup
	}

	// Group 0 always has superblock and GDT
	// Other groups have backup if sparse_super (groups 0, 1, 3, 5, 7, 9, 25, 27, 49, 81...)
	hasSuperblock := (group == 0) || isSparseGroup(group)

	nextBlock := gl.GroupStart

	if hasSuperblock {
		gl.SuperblockBlock = nextBlock
		nextBlock++ // Superblock takes block 0 of group

		gl.GDTStart = nextBlock
		// GDT needs enough blocks for all group descriptors
		gl.GDTBlocks = (l.GroupCount*32 + BlockSize - 1) / BlockSize
		nextBlock += gl.GDTBlocks
	}

	gl.BlockBitmapBlock = nextBlock
	nextBlock++

	gl.InodeBitmapBlock = nextBlock
	nextBlock++

	gl.InodeTableStart = nextBlock
	nextBlock += l.InodeTableBlocks

	gl.FirstDataBlock = nextBlock
	gl.OverheadBlocks = nextBlock - gl.GroupStart

	return gl
}

// BlockOffset returns the absolute byte offset for a given block number.
// Converts logical block numbers to physical byte positions within the partition.
func (l *Layout) BlockOffset(blockNum uint32) uint64 {
	return l.PartitionStart + uint64(blockNum)*BlockSize
}

// InodeOffset returns the absolute byte offset for a given inode number.
// Calculates which block group contains the inode and its position within
// the group's inode table. Inode numbers start from 1.
func (l *Layout) InodeOffset(inodeNum uint32) uint64 {
	if inodeNum < 1 {
		panic(fmt.Sprintf("invalid inode number: %d", inodeNum)) // This should never happen in normal operation
	}

	// Determine which group this inode belongs to
	group := (inodeNum - 1) / InodesPerGroup
	indexInGroup := (inodeNum - 1) % InodesPerGroup

	gl := l.GetGroupLayout(group)
	return l.BlockOffset(gl.InodeTableStart) + uint64(indexInGroup)*InodeSize
}

// TotalInodes returns the total number of inodes available in the filesystem.
// Calculated as the number of block groups multiplied by inodes per group.
func (l *Layout) TotalInodes() uint32 {
	return l.GroupCount * InodesPerGroup
}

// TotalFreeBlocks calculates the initial number of free blocks available for data.
// Subtracts all metadata overhead (superblocks, group descriptors, bitmaps, inode tables)
// from the total blocks to determine usable data space.
func (l *Layout) TotalFreeBlocks() uint32 {
	var overhead uint32
	for g := uint32(0); g < l.GroupCount; g++ {
		gl := l.GetGroupLayout(g)
		overhead += gl.OverheadBlocks
	}
	if l.TotalBlocks > overhead {
		return l.TotalBlocks - overhead
	}
	return 0
}

// String returns a human-readable description of the filesystem layout.
// Useful for debugging and understanding the calculated geometry parameters.
func (l *Layout) String() string {
	return fmt.Sprintf(`Filesystem Layout:
  Partition: offset=%d, size=%d bytes
  Total blocks: %d
  Group count: %d
  Blocks per group: %d
  Inodes per group: %d
  Inode table blocks per group: %d
  Total free blocks: %d`,
		l.PartitionStart, l.PartitionSize,
		l.TotalBlocks,
		l.GroupCount,
		l.BlocksPerGroup,
		l.InodesPerGroup,
		l.InodeTableBlocks,
		l.TotalFreeBlocks())
}

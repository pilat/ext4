package ext4fs

import (
	"fmt"
)

// freeBlock marks a block as free in the appropriate block bitmap.
// This allows previously allocated blocks to be reused for future allocations.
// The block is added to the free block list for efficient reuse.
func (b *builder) freeBlock(blockNum uint32) error {
	group := blockNum / blocksPerGroup
	indexInGroup := blockNum % blocksPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.BlockBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	if err := b.disk.readAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to read block bitmap for freeing block %d: %w", blockNum, err)
	}

	buf[0] &^= 1 << (indexInGroup % 8) // Clear the bit
	if err := b.disk.writeAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to write block bitmap for freeing block %d: %w", blockNum, err)
	}

	// Track freed blocks for accurate count and reuse
	b.freedBlocksPerGroup[group]++
	b.freeBlockList = append(b.freeBlockList, blockNum)

	return nil
}

// allocateBlock allocates a single free block from the filesystem.
// It first tries to reuse blocks from the free block list, then searches
// block groups for available blocks. Returns the allocated block number.
func (b *builder) allocateBlock() (uint32, error) {
	// First, try to reuse a freed block
	if len(b.freeBlockList) > 0 {
		block := b.freeBlockList[len(b.freeBlockList)-1]
		b.freeBlockList = b.freeBlockList[:len(b.freeBlockList)-1]

		group := block / blocksPerGroup
		b.freedBlocksPerGroup[group]--

		if err := b.markBlockUsed(block); err != nil {
			return 0, fmt.Errorf("failed to mark reused block as used: %w", err)
		}

		return block, nil
	}

	// Otherwise allocate new block
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		groupEnd := gl.GroupStart + gl.BlocksInGroup

		if b.nextBlockPerGroup[g] < groupEnd {
			block := b.nextBlockPerGroup[g]

			b.nextBlockPerGroup[g]++
			if err := b.markBlockUsed(block); err != nil {
				return 0, fmt.Errorf("failed to mark allocated block as used: %w", err)
			}

			return block, nil
		}
	}

	return 0, fmt.Errorf("out of blocks")
}

// allocateBlocks allocates n consecutive free blocks from the filesystem.
// For small allocations, it tries to find contiguous blocks within a single group.
// For larger allocations, it may allocate across multiple groups.
// Returns a slice of allocated block numbers.
func (b *builder) allocateBlocks(n uint32) ([]uint32, error) {
	if n == 0 {
		return nil, nil
	}

	blocks := make([]uint32, 0, n)

	// First, use freed blocks
	for len(blocks) < int(n) && len(b.freeBlockList) > 0 {
		block := b.freeBlockList[len(b.freeBlockList)-1]
		b.freeBlockList = b.freeBlockList[:len(b.freeBlockList)-1]

		group := block / blocksPerGroup
		b.freedBlocksPerGroup[group]--

		if err := b.markBlockUsed(block); err != nil {
			return nil, fmt.Errorf("failed to mark reused block as used: %w", err)
		}

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
					if err := b.markBlockUsed(block); err != nil {
						return nil, fmt.Errorf("failed to mark allocated block as used: %w", err)
					}

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
func (b *builder) allocateInode() (uint32, error) {
	if b.nextInode > b.layout.TotalInodes() {
		return 0, fmt.Errorf("out of inodes: %d", b.nextInode)
	}

	inode := b.nextInode

	b.nextInode++
	if err := b.markInodeUsed(inode); err != nil {
		return 0, fmt.Errorf("failed to mark allocated inode as used: %w", err)
	}

	return inode, nil
}

// markBlockUsed marks the specified block as used in its group's block bitmap.
// This prevents the block from being allocated again until it is explicitly freed.
// The bitmap is updated on disk to reflect the new allocation state.
func (b *builder) markBlockUsed(blockNum uint32) error {
	group := blockNum / blocksPerGroup
	indexInGroup := blockNum % blocksPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.BlockBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	if err := b.disk.readAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to read block bitmap for marking block %d used: %w", blockNum, err)
	}

	buf[0] |= 1 << (indexInGroup % 8)
	if err := b.disk.writeAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to write block bitmap for marking block %d used: %w", blockNum, err)
	}

	return nil
}

// freeInode marks the specified inode as free in its group's inode bitmap.
// This allows the inode to be reused for future allocations.
// Inode 0 is invalid and should never be freed.
func (b *builder) freeInode(inodeNum uint32) error {
	if inodeNum < 1 {
		return nil
	}

	group := (inodeNum - 1) / inodesPerGroup
	indexInGroup := (inodeNum - 1) % inodesPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.InodeBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	if err := b.disk.readAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to read inode bitmap for freeing inode %d: %w", inodeNum, err)
	}

	buf[0] &^= 1 << (indexInGroup % 8) // Clear the bit
	if err := b.disk.writeAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to write inode bitmap for freeing inode %d: %w", inodeNum, err)
	}

	// Track freed inodes for accurate count
	b.freedInodesPerGroup[group]++

	return nil
}

// markInodeUsed marks the specified inode as used in its group's inode bitmap.
// This prevents the inode from being allocated again and updates the bitmap on disk.
// Inode 0 is invalid and should never be marked as used.
func (b *builder) markInodeUsed(inodeNum uint32) error {
	if inodeNum < 1 {
		return nil
	}

	group := (inodeNum - 1) / inodesPerGroup
	indexInGroup := (inodeNum - 1) % inodesPerGroup

	gl := b.layout.GetGroupLayout(group)
	offset := b.layout.BlockOffset(gl.InodeBitmapBlock) + uint64(indexInGroup/8)

	var buf [1]byte
	if err := b.disk.readAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to read inode bitmap for marking inode %d used: %w", inodeNum, err)
	}

	buf[0] |= 1 << (indexInGroup % 8)
	if err := b.disk.writeAt(buf[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to write inode bitmap for marking inode %d used: %w", inodeNum, err)
	}

	return nil
}

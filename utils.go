package ext4fs

// isSparseGroup checks if group should have superblock backup
func isSparseGroup(group uint32) bool {
	if group <= 1 {
		return true
	}
	// Powers of 3, 5, 7
	for _, base := range []uint32{3, 5, 7} {
		for n := base; n <= group; n *= base {
			if n == group {
				return true
			}
		}
	}
	return false
}

// lbaToCHS converts LBA to CHS addressing (simplified, for compatibility only).
func lbaToCHS(lba uint32) [3]byte {
	sectorsPerTrack := uint32(63)
	heads := uint32(255)

	sector := (lba % sectorsPerTrack) + 1
	temp := lba / sectorsPerTrack
	head := temp % heads
	cylinder := temp / heads

	if cylinder > 1023 {
		cylinder = 1023
	}

	return [3]byte{
		byte(head),
		byte((sector & 0x3F) | ((cylinder >> 2) & 0xC0)),
		byte(cylinder & 0xFF),
	}
}

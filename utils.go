package ext4fs

import (
	"fmt"
	"strings"
)

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

// validateName checks if a filename is valid for ext4
func validateName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("filename cannot be empty")
	}
	if len(name) > 255 {
		return fmt.Errorf("filename too long: %d > 255", len(name))
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf("filename cannot contain '/'")
	}
	if strings.Contains(name, "\x00") {
		return fmt.Errorf("filename cannot contain null byte")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("filename cannot be '.' or '..'")
	}
	return nil
}

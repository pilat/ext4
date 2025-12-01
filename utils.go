package ext4fs

import (
	"fmt"
	"strings"
)

// isSparseGroup checks if a block group should contain a superblock backup.
// Ext4 uses sparse superblock placement to reduce metadata overhead.
// Groups 0, 1 and powers of 3, 5, and 7 get superblock backups.
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

// validateName checks if a filename is valid for use in an ext4 filesystem.
// Enforces ext4 naming restrictions including length limits, forbidden characters,
// and reserved names. Used before creating files or directories.
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

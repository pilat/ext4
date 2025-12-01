package ext4fs

import (
	"fmt"
	"os"
)

// ImageOption is a functional option for configuring Image creation.
type ImageOption func(*Image) error

// WithImagePath sets the image path.
func WithImagePath(imagePath string) ImageOption {
	return func(i *Image) error {
		i.imagePath = imagePath

		// Create/truncate file
		f, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("failed to open image file %s: %w", imagePath, err)
		}

		i.backend = &fileBackend{f: f}

		return nil
	}
}

// WithSizeInMB sets the image size in MB.
func WithSizeInMB(sizeMB int) ImageOption {
	return func(i *Image) error {
		i.sizeBytes = uint64(sizeMB) * 1024 * 1024
		return nil
	}
}

// WithSize sets image size in bytes.
func WithSize(sizeBytes uint64) ImageOption {
	return func(i *Image) error {
		i.sizeBytes = sizeBytes
		return nil
	}
}

// WithCreatedAt sets the creation timestamp.
func WithCreatedAt(createdAt uint32) ImageOption {
	return func(i *Image) error {
		i.createdAt = createdAt
		return nil
	}
}

// WithMemoryBackend creates an in-memory image of the given size in MB.
// Useful for testing and benchmarks to avoid disk I/O.
func WithMemoryBackend() ImageOption {
	return func(i *Image) error {
		i.backend = &memoryBackend{}
		return nil
	}
}

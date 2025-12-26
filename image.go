package ext4fs

import (
	"errors"
	"fmt"
	"time"
)

// Image provides the public API for creating ext4 filesystem images.
// It wraps the internal Builder with file-based storage and provides high-level
// methods for filesystem construction, metadata management, and image persistence.
type Image struct {
	builder *builder    // Internal filesystem builder
	backend diskBackend // File-based storage backend

	imagePath string // Path to the output image file
	sizeBytes uint64 // Image size in Bytes
	createdAt uint32 // Creation timestamp
}

// New creates a new ext4 filesystem image with the provided options.
// The imagePath and sizeMB must be specified via options. Size must be at least 4MB.
// Creates the necessary directory structure, allocates the image file, and initializes the filesystem layout and builder.
// Returns an Image ready for filesystem construction operations.
func New(opts ...ImageOption) (*Image, error) {
	img := &Image{
		createdAt: uint32(time.Now().Unix()),
	}
	for _, opt := range opts {
		if err := opt(img); err != nil {
			return nil, err
		}
	}

	if img.sizeBytes < 4*1024*1024 {
		return nil, fmt.Errorf("minimum size is 4MB")
	}

	partitionStart := uint64(0) // 0 offset for raw ext4
	partitionSize := img.sizeBytes - partitionStart

	// Calculate layout
	layout, err := CalculateLayout(partitionStart, partitionSize, img.createdAt)
	if err != nil {
		return nil, err
	}

	img.builder = newBuilder(img.backend, layout)

	if err := img.backend.truncate(int64(img.sizeBytes)); err != nil {
		return nil, fmt.Errorf("failed to truncate image file: %w", err)
	}

	if err := img.builder.prepareFilesystem(); err != nil {
		return nil, fmt.Errorf("failed to prepare filesystem: %w", err)
	}

	return img, nil
}

// Open opens an existing ext4 filesystem image for modification.
// The image path must be specified via WithExistingImagePath option.
// Returns an Image ready for filesystem operations like CreateFile, Delete, Save, and Close.
//
// Open reads the superblock to reconstruct filesystem geometry and scans
// allocation bitmaps to determine which blocks and inodes are already in use.
// This enables proper allocation for new files without corrupting existing data.
//
// Example:
//
//	img, err := ext4fs.Open(ext4fs.WithExistingImagePath("disk.img"))
//	if err != nil {
//	    return err
//	}
//	defer img.Close()
//
//	// Modify the filesystem
//	img.Delete(ext4fs.RootInode, "old-init")
//	img.CreateFile(ext4fs.RootInode, "init", newInitBinary, 0755, 0, 0)
//	return img.Save()
func Open(opts ...ImageOption) (*Image, error) {
	img := &Image{}
	for _, opt := range opts {
		if err := opt(img); err != nil {
			return nil, err
		}
	}

	if img.backend == nil {
		return nil, errors.New("backend is required: use WithExistingImagePath to open an existing image file")
	}

	// Load filesystem layout from superblock
	layout, err := loadLayoutFromDisk(img.backend)
	if err != nil {
		_ = img.backend.close()
		return nil, fmt.Errorf("load filesystem: %w", err)
	}

	img.createdAt = layout.CreatedAt
	img.builder = newBuilder(img.backend, layout)

	// Load allocation bitmaps into memory
	if err := img.builder.loadBitmaps(); err != nil {
		_ = img.backend.close()
		return nil, fmt.Errorf("load bitmaps: %w", err)
	}

	return img, nil
}

// CreateDirectory creates a new directory under the specified parent directory.
// Returns the inode number of the created directory, or an error if creation fails.
// The directory will be initialized with "." and ".." entries.
func (e *Image) CreateDirectory(parent uint32, name string, mode, uid, gid uint16) (uint32, error) {
	return e.builder.createDirectory(parent, name, mode, uid, gid)
}

// CreateFile creates a new regular file with the specified content.
// If a file with the same name exists, it will be overwritten.
// Returns the inode number of the created or overwritten file.
func (e *Image) CreateFile(parent uint32, name string, content []byte, mode, uid, gid uint16) (uint32, error) {
	return e.builder.createFile(parent, name, content, mode, uid, gid)
}

// CreateSymlink creates a symbolic link pointing to the specified target path.
// For targets <= 60 bytes, the target is stored directly in the inode.
// For longer targets, a separate data block is allocated.
func (e *Image) CreateSymlink(parent uint32, name, target string, uid, gid uint16) (uint32, error) {
	return e.builder.createSymlink(parent, name, target, uid, gid)
}

// SetXattr sets an extended attribute on the specified inode.
// Extended attributes use namespace prefixes like "user.", "trusted.", etc.
// If the attribute already exists, its value is updated.
func (e *Image) SetXattr(inodeNum uint32, name string, value []byte) error {
	return e.builder.setXattr(inodeNum, name, value)
}

// ListXattrs returns a list of all extended attribute names for the specified inode.
// Names include their namespace prefixes (e.g., "user.attr", "trusted.security").
func (e *Image) ListXattrs(inodeNum uint32) ([]string, error) {
	return e.builder.listXattrs(inodeNum)
}

// RemoveXattr removes an extended attribute from the specified inode.
// If the attribute doesn't exist, no error is returned.
// The xattr block may be deallocated if it becomes empty.
func (e *Image) RemoveXattr(inodeNum uint32, name string) error {
	return e.builder.removeXattr(inodeNum, name)
}

// Delete removes a file, symlink, or empty directory from the parent directory.
// Returns an error if the entry is a non-empty directory (use DeleteDirectory instead).
// This is similar to os.Remove behavior.
func (e *Image) Delete(parent uint32, name string) error {
	return e.builder.deleteEntry(parent, name)
}

// DeleteDirectory recursively removes a directory and all its contents.
// This is similar to os.RemoveAll behavior - it deletes everything without
// checking if subdirectories are empty.
// Returns an error if the entry is not a directory.
func (e *Image) DeleteDirectory(parent uint32, name string) error {
	return e.builder.deleteDirectory(parent, name)
}

// Save finalizes the filesystem and saves the image to disk.
// This includes finalizing the metadata, syncing the image, and closing the backend.
// Returns an error if the operation fails.
func (e *Image) Save() error {
	if err := e.builder.finalizeMetadata(); err != nil {
		return fmt.Errorf("failed to finalize metadata: %w", err)
	}

	if err := e.backend.sync(); err != nil {
		return fmt.Errorf("failed to sync image: %w", err)
	}

	return nil
}

// Close closes the image and backend.
// Returns an error if the operation fails.
func (e *Image) Close() error {
	return e.backend.close()
}

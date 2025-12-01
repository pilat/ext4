package ext4fs

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// diskBackend abstracts I/O operations for different storage backends.
// This interface allows the filesystem builder to work with various storage
// types (files, memory buffers, network storage) through a common API.
type diskBackend interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
}

// fileBackend implements diskBackend using a regular file on disk.
// Provides random access read/write operations for ext4 image files.
// Includes additional methods for synchronization and resource cleanup.
type fileBackend struct {
	f *os.File
}

func (fb *fileBackend) ReadAt(p []byte, off int64) (int, error) {
	n, err := fb.f.ReadAt(p, off)
	if err != nil {
		return n, fmt.Errorf("disk read error: %w", err)
	}
	return n, nil
}
func (fb *fileBackend) WriteAt(p []byte, off int64) (int, error) {
	n, err := fb.f.WriteAt(p, off)
	if err != nil {
		return n, fmt.Errorf("disk write error: %w", err)
	}
	return n, nil
}
func (fb *fileBackend) Sync() error {
	if err := fb.f.Sync(); err != nil {
		return fmt.Errorf("disk sync error: %w", err)
	}
	return nil
}
func (fb *fileBackend) Close() error {
	if err := fb.f.Close(); err != nil {
		return fmt.Errorf("disk close error: %w", err)
	}
	return nil
}

// Ext4ImageBuilder provides the public API for creating ext4 filesystem images.
// It wraps the internal Builder with file-based storage and provides high-level
// methods for filesystem construction, metadata management, and image persistence.
type Ext4ImageBuilder struct {
	builder   *Builder     // Internal filesystem builder
	imagePath string       // Path to the output image file
	disk      *fileBackend // File-based storage backend
}

// New creates a new ext4 filesystem image at the specified path.
// The image size must be at least 4MB. Creates the necessary directory structure,
// allocates the image file, and initializes the filesystem layout and builder.
// Returns an Ext4ImageBuilder ready for filesystem construction operations.
func New(imagePath string, sizeMB int) (*Ext4ImageBuilder, error) {
	if sizeMB < 4 {
		return nil, fmt.Errorf("minimum size is 4MB")
	}

	totalSize := uint64(sizeMB) * 1024 * 1024

	// partitionStart := uint64(1024 * 1024) // 1MB offset for MBR
	partitionStart := uint64(0) // 0 offset for raw ext4
	partitionSize := totalSize - partitionStart

	// Create directory if needed
	dir := filepath.Dir(imagePath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Create/truncate file
	f, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open image file %s: %w", imagePath, err)
	}

	if err := f.Truncate(int64(totalSize)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to truncate image file: %w", err)
	}

	backend := &fileBackend{f: f}

	// Calculate layout
	layout, err := CalculateLayout(partitionStart, partitionSize, uint32(time.Now().Unix()))
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Ext4ImageBuilder{
		builder:   newBuilder(backend, layout),
		imagePath: imagePath,
		disk:      backend,
	}, nil
}

// PrepareFilesystem initializes the core ext4 filesystem structures.
// This includes writing the superblock, group descriptors, bitmaps,
// inode tables, and creating essential directories. Must be called before
// any file or directory creation operations.
func (e *Ext4ImageBuilder) PrepareFilesystem() error {
	return e.builder.PrepareFilesystem()
}

// CreateDirectory creates a new directory under the specified parent directory.
// Returns the inode number of the created directory, or an error if creation fails.
// The directory will be initialized with "." and ".." entries.
func (e *Ext4ImageBuilder) CreateDirectory(parent uint32, name string, mode, uid, gid uint16) (uint32, error) {
	return e.builder.CreateDirectory(parent, name, mode, uid, gid)
}

// CreateFile creates a new regular file with the specified content.
// If a file with the same name exists, it will be overwritten.
// Returns the inode number of the created or overwritten file.
func (e *Ext4ImageBuilder) CreateFile(parent uint32, name string, content []byte, mode, uid, gid uint16) (uint32, error) {
	return e.builder.CreateFile(parent, name, content, mode, uid, gid)
}

// CreateSymlink creates a symbolic link pointing to the specified target path.
// For targets <= 60 bytes, the target is stored directly in the inode.
// For longer targets, a separate data block is allocated.
func (e *Ext4ImageBuilder) CreateSymlink(parent uint32, name, target string, uid, gid uint16) (uint32, error) {
	return e.builder.CreateSymlink(parent, name, target, uid, gid)
}

// SetXattr sets an extended attribute on the specified inode.
// Extended attributes use namespace prefixes like "user.", "trusted.", etc.
// If the attribute already exists, its value is updated.
func (e *Ext4ImageBuilder) SetXattr(inodeNum uint32, name string, value []byte) error {
	return e.builder.SetXattr(inodeNum, name, value)
}

// GetXattr retrieves the value of an extended attribute from the specified inode.
// Returns the attribute value as a byte slice, or an error if the attribute doesn't exist.
func (e *Ext4ImageBuilder) GetXattr(inodeNum uint32, name string) ([]byte, error) {
	return e.builder.GetXattr(inodeNum, name)
}

// ListXattrs returns a list of all extended attribute names for the specified inode.
// Names include their namespace prefixes (e.g., "user.attr", "trusted.security").
func (e *Ext4ImageBuilder) ListXattrs(inodeNum uint32) ([]string, error) {
	return e.builder.ListXattrs(inodeNum)
}

// RemoveXattr removes an extended attribute from the specified inode.
// If the attribute doesn't exist, no error is returned.
// The xattr block may be deallocated if it becomes empty.
func (e *Ext4ImageBuilder) RemoveXattr(inodeNum uint32, name string) error {
	return e.builder.RemoveXattr(inodeNum, name)
}

// Save finalizes all filesystem metadata and synchronizes all pending writes
// to the underlying storage. This updates block and inode usage statistics,
// group descriptors, and the superblock, then ensures the image is durably
// written to disk. Must be called after all file operations are complete.
func (e *Ext4ImageBuilder) Save() error {
	if err := e.builder.FinalizeMetadata(); err != nil {
		return err
	}
	return e.disk.Sync()
}

// Close releases all resources associated with the image builder.
// This includes closing the underlying file handle and freeing any
// internal data structures. The image file remains on disk.
func (e *Ext4ImageBuilder) Close() error {
	return e.disk.Close()
}

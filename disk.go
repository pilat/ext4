package ext4fs

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// diskBackend abstracts I/O operations
type diskBackend interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
}

type fileBackend struct {
	f *os.File
}

func (fb *fileBackend) ReadAt(p []byte, off int64) (int, error)  { return fb.f.ReadAt(p, off) }
func (fb *fileBackend) WriteAt(p []byte, off int64) (int, error) { return fb.f.WriteAt(p, off) }
func (fb *fileBackend) Sync() error                              { return fb.f.Sync() }
func (fb *fileBackend) Close() error                             { return fb.f.Close() }

// Ext4ImageBuilder is the public API (wraps Builder)
type Ext4ImageBuilder struct {
	builder   *Builder
	imagePath string
	disk      *fileBackend
}

// NewExt4ImageBuilder creates a new ext4 image
func NewExt4ImageBuilder(imagePath string, sizeMB int) (*Ext4ImageBuilder, error) {
	if sizeMB < 4 {
		return nil, fmt.Errorf("minimum size is 4MB")
	}

	totalSize := uint64(sizeMB) * 1024 * 1024
	partitionStart := uint64(1024 * 1024) // 1MB offset
	partitionSize := totalSize - partitionStart

	// Create directory if needed
	dir := filepath.Dir(imagePath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	// Create/truncate file
	f, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	if err := f.Truncate(int64(totalSize)); err != nil {
		f.Close()
		return nil, err
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

// PrepareFilesystem initializes filesystem structures
func (e *Ext4ImageBuilder) PrepareFilesystem() {
	e.builder.PrepareFilesystem()
}

// CreateDirectory creates a directory
func (e *Ext4ImageBuilder) CreateDirectory(parent uint32, name string, mode, uid, gid uint16) (uint32, error) {
	return e.builder.CreateDirectory(parent, name, mode, uid, gid)
}

// CreateFile creates a file
func (e *Ext4ImageBuilder) CreateFile(parent uint32, name string, content []byte, mode, uid, gid uint16) (uint32, error) {
	return e.builder.CreateFile(parent, name, content, mode, uid, gid)
}

// CreateSymlink creates a symbolic link
func (e *Ext4ImageBuilder) CreateSymlink(parent uint32, name, target string, uid, gid uint16) (uint32, error) {
	return e.builder.CreateSymlink(parent, name, target, uid, gid)
}

// FinalizeMetadata updates counters
func (e *Ext4ImageBuilder) FinalizeMetadata() {
	e.builder.FinalizeMetadata()
}

// Save syncs to disk
func (e *Ext4ImageBuilder) Save() error {
	return e.disk.Sync()
}

// Close releases resources
func (e *Ext4ImageBuilder) Close() error {
	return e.disk.Close()
}

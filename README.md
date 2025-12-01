# ext4fs

Pure Go ext4 filesystem implementation for creating disk images without external dependencies.

[![Go Reference](https://pkg.go.dev/badge/github.com/pilat/go-ext4fs.svg)](https://pkg.go.dev/github.com/pilat/go-ext4fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/pilat/go-ext4fs)](https://goreportcard.com/report/github.com/pilat/go-ext4fs)

## Features

- **Pure Go**: No external dependencies or system calls
- **Full ext4 support**: Extents, extended attributes, symlinks, and more
- **Docker integration**: End-to-end testing with Docker containers
- **Simple API**: Easy to use for creating filesystem images programmatically

## Installation

```bash
go get github.com/pilat/go-ext4fs
```

## Quick Start

```go
package main

import (
    "github.com/pilat/go-ext4fs"
)

func main() {
    // Create 64MB ext4 image
    builder, err := ext4fs.NewExt4ImageBuilder("disk.img", 64)
    if err != nil {
        panic(err)
    }
    defer builder.Close()

    // Prepare filesystem
    if err := builder.PrepareFilesystem(); err != nil {
        panic(err)
    }

    // Create directories and files
    etcDir, err := builder.CreateDirectory(ext4fs.RootInode, "etc", 0755, 0, 0)
    if err != nil {
        panic(err)
    }

    _, err = builder.CreateFile(etcDir, "hostname", []byte("myhost\n"), 0644, 0, 0)
    if err != nil {
        panic(err)
    }

    // Create symlinks
    _, err = builder.CreateSymlink(etcDir, "hosts", "/etc/hostname", 0, 0)
    if err != nil {
        panic(err)
    }

    // Set extended attributes
    err = builder.SetXattr(etcDir, "user.comment", []byte("System configuration"))
    if err != nil {
        panic(err)
    }

    // Finalize and save
    builder.FinalizeMetadata()
    if err := builder.Save(); err != nil {
        panic(err)
    }
}
```

## API Overview

### Creating Filesystems

- `NewExt4ImageBuilder(path, sizeMB)` - Create a new ext4 image builder
- `PrepareFilesystem()` - Initialize the filesystem structure
- `FinalizeMetadata()` - Update filesystem metadata before saving
- `Save()` - Write the image to disk

### File Operations

- `CreateDirectory(parent, name, mode, uid, gid)` - Create a directory
- `CreateFile(parent, name, content, mode, uid, gid)` - Create a file with content
- `CreateSymlink(parent, name, target, uid, gid)` - Create a symbolic link

### Extended Attributes

- `SetXattr(inode, name, value)` - Set an extended attribute
- `GetXattr(inode, name)` - Get an extended attribute value
- `ListXattrs(inode)` - List all extended attributes
- `RemoveXattr(inode, name)` - Remove an extended attribute

## Mounting and Testing

The created images can be mounted on Linux systems:

```bash
# Mount the ext4 partition (skip 1MB MBR offset)
sudo mount -o loop,offset=1048576 disk.img /mnt
```

For testing, the package includes end-to-end tests that use Docker to mount and verify the generated filesystems.

## Requirements

- Go 1.19 or later
- Linux for mounting (optional, for testing)

## Contributing

Contributions are welcome! Please ensure:

- Code follows Go conventions
- Tests pass (`go test`)
- New features include appropriate tests
- Documentation is updated

## License

MIT License - see [LICENSE](LICENSE) file for details.
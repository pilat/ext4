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

## License

MIT License - see [LICENSE](LICENSE) file for details.
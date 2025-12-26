# ext4fs

Pure Go ext4 filesystem implementation for creating disk images without external dependencies.

[![Go Reference](https://pkg.go.dev/badge/github.com/pilat/go-ext4fs.svg)](https://pkg.go.dev/github.com/pilat/go-ext4fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/pilat/go-ext4fs)](https://goreportcard.com/report/github.com/pilat/go-ext4fs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pilat/go-ext4fs)](https://github.com/pilat/go-ext4fs)
[![CI](https://github.com/pilat/go-ext4fs/actions/workflows/ci.yml/badge.svg)](https://github.com/pilat/go-ext4fs/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/github/pilat/go-ext4fs/graph/badge.svg)](https://codecov.io/github/pilat/go-ext4fs)
![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/pilat/go-ext4fs?utm_source=oss&utm_medium=github&utm_campaign=pilat%2Fgo-ext4fs&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)

## Overview

A pure Go library for **creating** ext4 filesystem images programmatically. Designed for building disk images for virtual machines and embedded systems without requiring root privileges or external tools like `mke2fs`.

> **Primary use case**: Creating new ext4 images from scratch. Limited support exists for modifying images previously created by this library (e.g., updating `/init` binary). Standard ext4 filesystems created by `mke2fs` cannot be opened due to unsupported features (journaling, 64-bit, flex_bg).

## Features

- **Pure Go**: No external dependencies or system calls
- **Extent-based**: Modern ext4 extent trees for efficient block mapping
- **Extended attributes**: Full xattr support for security labels and metadata
- **Symlinks**: Both fast (inline) and slow (block-based) symlinks
- **Simple API**: Easy to use for creating filesystem images programmatically
- **Docker integration**: End-to-end testing with real Linux kernel verification

## Installation
```bash
go get github.com/pilat/go-ext4fs
```

## Quick Start

### Creating a New Image
```go
package main

import "github.com/pilat/go-ext4fs"

func main() {
    // Create 64MB ext4 image
    img, err := ext4fs.New(
        ext4fs.WithImagePath("disk.img"),
        ext4fs.WithSizeInMB(64),
    )
    if err != nil {
        panic(err)
    }
    defer img.Close()

    // Create directories and files
    etcDir, _ := img.CreateDirectory(ext4fs.RootInode, "etc", 0755, 0, 0)
    img.CreateFile(etcDir, "hostname", []byte("myhost\n"), 0644, 0, 0)
    img.CreateSymlink(etcDir, "hosts", "/etc/hostname", 0, 0)
    img.SetXattr(etcDir, "user.comment", []byte("System configuration"))

    // Finalize and save
    if err := img.Save(); err != nil {
        panic(err)
    }
}
```

### Modifying an Existing Image
```go
package main

import "github.com/pilat/go-ext4fs"

func main() {
    // Open existing image (must have been created by this library)
    img, err := ext4fs.Open(ext4fs.WithExistingImagePath("disk.img"))
    if err != nil {
        panic(err)
    }
    defer img.Close()

    // Modify: delete old file, add new one
    newInit := []byte("#!/bin/sh\nexec /bin/sh\n")
    img.Delete(ext4fs.RootInode, "old-init")
    img.CreateFile(ext4fs.RootInode, "init", newInit, 0755, 0, 0)

    if err := img.Save(); err != nil {
        panic(err)
    }
}
```

## Limitations

This library is optimized for creating simple disk images for small VMs. The following ext4 features are not implemented:

| Feature | Impact |
|---------|--------|
| Journaling | No crash recovery (not needed for image creation) |
| 64-bit block addresses | Maximum filesystem size ~16 TB |
| Extent tree depth > 1 | Maximum ~1,360 extents per file; sufficient for contiguous files up to ~170 TB |
| HTree directory indexing | Linear directory scan; fine for small directories |
| Inline data | Small files use regular blocks |
| Encryption | No at-rest encryption support |
| Quotas | No user/group quota tracking |

For most VM use cases (boot disks, configuration filesystems, small data volumes), these limitations have no practical impact.

## Verification

Created images can be verified using standard Linux tools:
```bash
# Check filesystem integrity
e2fsck -n -f disk.img

# Mount and inspect (requires root)
sudo mount -o loop disk.img /mnt

# View filesystem info
dumpe2fs disk.img
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

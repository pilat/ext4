# ext4

Pure Go ext4 filesystem implementation for creating disk images without external dependencies.

## Installation

```bash
go get github.com/pilat/go-ext4fs
```

## Usage

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

    // Prepare filesystem
    builder.PrepareFilesystem()

    // Create directories and files
    etcDir := builder.CreateDirectory(ext4fs.RootInode, "etc", 0755, 0, 0)
    builder.CreateFile(etcDir, "hostname", []byte("myhost\n"), 0644, 0, 0)

    // Finalize and save
    builder.FinalizeMetadata()
    builder.Save()
}
```

## Mount

```bash
sudo mount -o loop,offset=1048576 disk.img /mnt
```

## License

MIT
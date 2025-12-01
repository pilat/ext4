package ext4fs_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilat/go-ext4fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Docker image to use for ext4 validation
	dockerImage = "alpine:latest"
	// Default test image size in MB
	defaultImageSizeMB = 64
	// Partition offset (1MB) for mounting
	partitionOffset = 1048576
)

// testContext holds resources for a single test case
type testContext struct {
	t         *testing.T
	imagePath string
	builder   *ext4fs.Ext4ImageBuilder
}

// newTestContext creates a new test context with a temporary image file
func newTestContext(t *testing.T, sizeMB int) *testContext {
	t.Helper()

	prevDebug := ext4fs.DEBUG
	ext4fs.DEBUG = true
	defer func() {
		ext4fs.DEBUG = prevDebug
	}()

	tmpDir := t.TempDir()
	imagePath := filepath.Join(tmpDir, "test.img")

	builder, err := ext4fs.NewExt4ImageBuilder(imagePath, sizeMB)
	require.NoError(t, err, "failed to create ext4 image builder")

	return &testContext{
		t:         t,
		imagePath: imagePath,
		builder:   builder,
	}
}

// finalize prepares the filesystem and saves the image
func (tc *testContext) finalize() {
	tc.t.Helper()
	tc.builder.FinalizeMetadata()
	err := tc.builder.Save()
	require.NoError(tc.t, err, "failed to save image")
	err = tc.builder.Close()
	require.NoError(tc.t, err, "failed to close builder")
}

// dockerExec runs a command inside a privileged Docker container with the image mounted
// Returns stdout, stderr, and any error
func (tc *testContext) dockerExec(commands ...string) (string, string, error) {
	tc.t.Helper()

	script := fmt.Sprintf(`
set -e
apk add --no-cache e2fsprogs > /dev/null 2>&1

# Copy image to container filesystem
cp /image/test.img /tmp/test.img

# Extract partition (skip 1MB MBR offset)
dd if=/tmp/test.img of=/tmp/partition.img bs=1M skip=1 2>/dev/null

# Check filesystem
e2fsck -n -f /tmp/partition.img || { echo "e2fsck failed"; exit 1; }

# Mount the filesystem
mkdir -p /mnt/ext4
mount -t ext4 -o loop /tmp/partition.img /mnt/ext4

# Run the verification commands
cd /mnt/ext4
%s

# Cleanup
cd /
umount /mnt/ext4
`, strings.Join(commands, "\n"))

	absPath, err := filepath.Abs(filepath.Dir(tc.imagePath))
	if err != nil {
		return "", "", fmt.Errorf("failed to get absolute path: %w", err)
	}

	args := []string{
		"run", "--rm", "--privileged",
		"-v", fmt.Sprintf("%s:/image:ro", absPath),
		dockerImage,
		"sh", "-c", script,
	}

	// Use a context with timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return stdout.String(), stderr.String(), fmt.Errorf("docker command timed out after 60s")
	}
	return stdout.String(), stderr.String(), err
}

// dockerExecSimple runs commands and returns combined output, failing the test on error
func (tc *testContext) dockerExecSimple(commands ...string) string {
	tc.t.Helper()
	stdout, stderr, err := tc.dockerExec(commands...)
	if err != nil {
		tc.t.Fatalf("docker exec failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}
	return stdout
}

// skipIfNoDocker skips the test if Docker is not available
func skipIfNoDocker(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		t.Skip("Docker not available, skipping e2e test")
	}
}

// TestBasicFilesystemCreation verifies that a basic ext4 filesystem can be created and mounted
func TestBasicFilesystemCreation(t *testing.T) {
	skipIfNoDocker(t)

	tc := newTestContext(t, defaultImageSizeMB)
	tc.builder.PrepareFilesystem()
	tc.finalize()

	// Verify the filesystem can be mounted and has basic structure
	output := tc.dockerExecSimple(
		`ls -la`,
		`test -d "lost+found" && echo "lost+found exists"`,
	)

	assert.Contains(t, output, "lost+found exists")
}

// TestFileCreation tests creating files with various sizes and contents
func TestFileCreation(t *testing.T) {
	skipIfNoDocker(t)

	testCases := []struct {
		name     string
		filename string
		content  string
		mode     uint16
		uid      uint16
		gid      uint16
	}{
		{
			name:     "simple text file",
			filename: "hello.txt",
			content:  "Hello, World!\n",
			mode:     0644,
			uid:      0,
			gid:      0,
		},
		{
			name:     "empty file",
			filename: "empty.txt",
			content:  "",
			mode:     0644,
			uid:      0,
			gid:      0,
		},
		{
			name:     "file with custom permissions",
			filename: "secret.txt",
			content:  "secret data",
			mode:     0600,
			uid:      1000,
			gid:      1000,
		},
		{
			name:     "executable script",
			filename: "script.sh",
			content:  "#!/bin/sh\necho 'Hello'\n",
			mode:     0755,
			uid:      0,
			gid:      0,
		},
		{
			name:     "multiline file",
			filename: "multiline.txt",
			content:  "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n",
			mode:     0644,
			uid:      0,
			gid:      0,
		},
		{
			name:     "file with special characters",
			filename: "special.txt",
			content:  "Tab:\there\nUnicode: 你好世界\nSymbols: @#$%^&*()\n",
			mode:     0644,
			uid:      0,
			gid:      0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestContext(t, defaultImageSizeMB)
			ctx.builder.PrepareFilesystem()

			ctx.builder.CreateFile(ext4fs.RootInode, tc.filename, []byte(tc.content), tc.mode, tc.uid, tc.gid)

			ctx.finalize()

			// Verify file exists and has correct content
			commands := []string{
				fmt.Sprintf(`test -f "%s" && echo "file exists"`, tc.filename),
			}

			if tc.content != "" {
				commands = append(commands, fmt.Sprintf(`cat "%s"`, tc.filename))
			}

			// Check permissions (convert to octal string)
			commands = append(commands, fmt.Sprintf(`stat -c "%%a" "%s"`, tc.filename))

			// Check ownership
			commands = append(commands, fmt.Sprintf(`stat -c "%%u:%%g" "%s"`, tc.filename))

			output := ctx.dockerExecSimple(commands...)

			assert.Contains(t, output, "file exists")
			if tc.content != "" {
				assert.Contains(t, output, tc.content)
			}
			assert.Contains(t, output, fmt.Sprintf("%o", tc.mode))
			assert.Contains(t, output, fmt.Sprintf("%d:%d", tc.uid, tc.gid))
		})
	}
}

// TestFileOverwriting tests overwriting files with new content, permissions, and ownership
func TestFileOverwriting(t *testing.T) {
	skipIfNoDocker(t)

	testCases := []struct {
		name            string
		originalContent string
		originalMode    uint16
		originalUID     uint16
		originalGID     uint16
		newContent      string
		newMode         uint16
		newUID          uint16
		newGID          uint16
	}{
		{
			name:            "overwrite with larger content",
			originalContent: "tiny",
			originalMode:    0644,
			originalUID:     0,
			originalGID:     0,
			newContent:      "This is a much longer content that should completely replace the original file data",
			newMode:         0644,
			newUID:          0,
			newGID:          0,
		},
		{
			name:            "overwrite with smaller content",
			originalContent: "This is a very long original content that will be replaced with something shorter",
			originalMode:    0644,
			originalUID:     0,
			originalGID:     0,
			newContent:      "short",
			newMode:         0644,
			newUID:          0,
			newGID:          0,
		},
		{
			name:            "overwrite with different permissions",
			originalContent: "test content",
			originalMode:    0644,
			originalUID:     0,
			originalGID:     0,
			newContent:      "test content",
			newMode:         0600,
			newUID:          0,
			newGID:          0,
		},
		{
			name:            "overwrite with different ownership",
			originalContent: "test content",
			originalMode:    0644,
			originalUID:     0,
			originalGID:     0,
			newContent:      "test content",
			newMode:         0644,
			newUID:          1000,
			newGID:          1000,
		},
		{
			name:            "overwrite everything",
			originalContent: "original",
			originalMode:    0644,
			originalUID:     0,
			originalGID:     0,
			newContent:      "completely new content with different everything",
			newMode:         0755,
			newUID:          500,
			newGID:          500,
		},
		{
			name:            "overwrite empty file",
			originalContent: "",
			originalMode:    0644,
			originalUID:     0,
			originalGID:     0,
			newContent:      "now has content",
			newMode:         0644,
			newUID:          0,
			newGID:          0,
		},
		{
			name:            "overwrite to empty file",
			originalContent: "has content",
			originalMode:    0644,
			originalUID:     0,
			originalGID:     0,
			newContent:      "",
			newMode:         0644,
			newUID:          0,
			newGID:          0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestContext(t, defaultImageSizeMB)
			ctx.builder.PrepareFilesystem()

			filename := "testfile.txt"

			// Create original file
			ctx.builder.CreateFile(ext4fs.RootInode, filename, []byte(tc.originalContent), tc.originalMode, tc.originalUID, tc.originalGID)

			// Overwrite the file
			ctx.builder.CreateFile(ext4fs.RootInode, filename, []byte(tc.newContent), tc.newMode, tc.newUID, tc.newGID)

			ctx.finalize()

			// Verify file exists with new content, permissions, and ownership
			commands := []string{
				fmt.Sprintf(`test -f "%s" && echo "file exists"`, filename),
			}

			if tc.newContent != "" {
				commands = append(commands, fmt.Sprintf(`cat "%s"`, filename))
			} else {
				// For empty files, check size is 0
				commands = append(commands, fmt.Sprintf(`[ $(stat -c %%s "%s") -eq 0 ] && echo "file is empty"`, filename))
			}

			// Check permissions
			commands = append(commands, fmt.Sprintf(`stat -c "%%a" "%s"`, filename))

			// Check ownership
			commands = append(commands, fmt.Sprintf(`stat -c "%%u:%%g" "%s"`, filename))

			output := ctx.dockerExecSimple(commands...)

			assert.Contains(t, output, "file exists")
			if tc.newContent != "" {
				assert.Contains(t, output, tc.newContent)
				// Only check that original content is not present if it's different from new content
				if tc.originalContent != tc.newContent && tc.originalContent != "" {
					assert.NotContains(t, output, tc.originalContent, "should not contain original content")
				}
			} else {
				assert.Contains(t, output, "file is empty")
			}
			assert.Contains(t, output, fmt.Sprintf("%o", tc.newMode))
			assert.Contains(t, output, fmt.Sprintf("%d:%d", tc.newUID, tc.newGID))
		})
	}
}

// TestMultipleFileOverwrites tests overwriting the same file multiple times
func TestMultipleFileOverwrites(t *testing.T) {
	skipIfNoDocker(t)

	ctx := newTestContext(t, defaultImageSizeMB)
	ctx.builder.PrepareFilesystem()

	filename := "multi-overwrite.txt"

	// Create and overwrite the file multiple times
	contents := []string{
		"First version",
		"Second version - longer content",
		"Third",
		"Fourth version with even more content than before",
		"Final version",
	}

	modes := []uint16{0644, 0600, 0755, 0400, 0777}
	uids := []uint16{0, 1000, 500, 0, 1001}
	gids := []uint16{0, 1000, 500, 0, 1001}

	for i, content := range contents {
		ctx.builder.CreateFile(ext4fs.RootInode, filename, []byte(content), modes[i], uids[i], gids[i])
	}

	ctx.finalize()

	// Verify only the final version is present
	output := ctx.dockerExecSimple(
		fmt.Sprintf(`test -f "%s" && echo "file exists"`, filename),
		fmt.Sprintf(`cat "%s"`, filename),
		fmt.Sprintf(`stat -c "%%a" "%s"`, filename),
		fmt.Sprintf(`stat -c "%%u:%%g" "%s"`, filename),
	)

	assert.Contains(t, output, "file exists")
	assert.Contains(t, output, contents[len(contents)-1])
	// Should not contain earlier versions
	for i := 0; i < len(contents)-1; i++ {
		assert.NotContains(t, output, contents[i])
	}
	assert.Contains(t, output, fmt.Sprintf("%o", modes[len(modes)-1]))
	assert.Contains(t, output, fmt.Sprintf("%d:%d", uids[len(uids)-1], gids[len(gids)-1]))
}

// TestOverwriteInSubdirectory tests overwriting files in subdirectories
func TestOverwriteInSubdirectory(t *testing.T) {
	skipIfNoDocker(t)

	ctx := newTestContext(t, defaultImageSizeMB)
	ctx.builder.PrepareFilesystem()

	// Create a subdirectory
	subdir, err := ctx.builder.CreateDirectory(ext4fs.RootInode, "subdir", 0755, 0, 0)
	require.NoError(t, err)

	filename := "file.txt"

	// Create original file in subdirectory
	ctx.builder.CreateFile(subdir, filename, []byte("original content"), 0644, 0, 0)

	// Overwrite the file
	ctx.builder.CreateFile(subdir, filename, []byte("new content"), 0600, 1000, 1000)

	ctx.finalize()

	// Verify the overwritten file
	output := ctx.dockerExecSimple(
		`test -f "subdir/file.txt" && echo "file exists"`,
		`cat "subdir/file.txt"`,
		`stat -c "%a" "subdir/file.txt"`,
		`stat -c "%u:%g" "subdir/file.txt"`,
	)

	assert.Contains(t, output, "file exists")
	assert.Contains(t, output, "new content")
	assert.NotContains(t, output, "original content")
	assert.Contains(t, output, "600")
	assert.Contains(t, output, "1000:1000")
}

// TestDirectoryCreation tests creating directories with various structures
func TestDirectoryCreation(t *testing.T) {
	skipIfNoDocker(t)

	testCases := []struct {
		name      string
		structure func(b *ext4fs.Ext4ImageBuilder)
		verify    []string
		expects   []string
	}{
		{
			name: "single directory",
			structure: func(b *ext4fs.Ext4ImageBuilder) {
				b.CreateDirectory(ext4fs.RootInode, "mydir", 0755, 0, 0)
			},
			verify: []string{
				`test -d "mydir" && echo "mydir is directory"`,
				`stat -c "%a" "mydir"`,
			},
			expects: []string{"mydir is directory", "755"},
		},
		{
			name: "nested directories",
			structure: func(b *ext4fs.Ext4ImageBuilder) {
				dir1, err := b.CreateDirectory(ext4fs.RootInode, "level1", 0755, 0, 0)
				require.NoError(t, err)
				dir2, err := b.CreateDirectory(dir1, "level2", 0755, 0, 0)
				require.NoError(t, err)
				b.CreateDirectory(dir2, "level3", 0755, 0, 0)
			},
			verify: []string{
				`test -d "level1/level2/level3" && echo "nested dirs exist"`,
			},
			expects: []string{"nested dirs exist"},
		},
		{
			name: "directory with sticky bit",
			structure: func(b *ext4fs.Ext4ImageBuilder) {
				b.CreateDirectory(ext4fs.RootInode, "tmp", 0777|ext4fs.S_ISVTX, 0, 0)
			},
			verify: []string{
				`test -d "tmp" && echo "tmp exists"`,
				`stat -c "%a" "tmp"`,
			},
			expects: []string{"tmp exists", "1777"},
		},
		{
			name: "directory with custom ownership",
			structure: func(b *ext4fs.Ext4ImageBuilder) {
				b.CreateDirectory(ext4fs.RootInode, "userdir", 0750, 1000, 1000)
			},
			verify: []string{
				`stat -c "%u:%g" "userdir"`,
				`stat -c "%a" "userdir"`,
			},
			expects: []string{"1000:1000", "750"},
		},
		{
			name: "multiple directories at same level",
			structure: func(b *ext4fs.Ext4ImageBuilder) {
				b.CreateDirectory(ext4fs.RootInode, "bin", 0755, 0, 0)
				b.CreateDirectory(ext4fs.RootInode, "etc", 0755, 0, 0)
				b.CreateDirectory(ext4fs.RootInode, "home", 0755, 0, 0)
				b.CreateDirectory(ext4fs.RootInode, "var", 0755, 0, 0)
			},
			verify: []string{
				`ls -1d bin etc home var | wc -l | tr -d ' '`,
			},
			expects: []string{"4"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestContext(t, defaultImageSizeMB)
			ctx.builder.PrepareFilesystem()
			tc.structure(ctx.builder)
			ctx.finalize()

			output := ctx.dockerExecSimple(tc.verify...)

			for _, expected := range tc.expects {
				assert.Contains(t, output, expected)
			}
		})
	}
}

// TestSymlinkCreation tests creating symbolic links
func TestSymlinkCreation(t *testing.T) {
	skipIfNoDocker(t)

	testCases := []struct {
		name    string
		setup   func(b *ext4fs.Ext4ImageBuilder)
		verify  []string
		expects []string
	}{
		{
			name: "simple symlink to file",
			setup: func(b *ext4fs.Ext4ImageBuilder) {
				b.CreateFile(ext4fs.RootInode, "original.txt", []byte("original content"), 0644, 0, 0)
				b.CreateSymlink(ext4fs.RootInode, "link.txt", "original.txt", 0, 0)
			},
			verify: []string{
				`test -L "link.txt" && echo "is symlink"`,
				`readlink "link.txt"`,
				`cat "link.txt"`,
			},
			expects: []string{"is symlink", "original.txt", "original content"},
		},
		{
			name: "symlink to directory",
			setup: func(b *ext4fs.Ext4ImageBuilder) {
				dir, err := b.CreateDirectory(ext4fs.RootInode, "realdir", 0755, 0, 0)
				require.NoError(t, err)
				b.CreateFile(dir, "file.txt", []byte("in dir"), 0644, 0, 0)
				b.CreateSymlink(ext4fs.RootInode, "linkdir", "realdir", 0, 0)
			},
			verify: []string{
				`test -L "linkdir" && echo "is symlink"`,
				`cat "linkdir/file.txt"`,
			},
			expects: []string{"is symlink", "in dir"},
		},
		{
			name: "relative symlink",
			setup: func(b *ext4fs.Ext4ImageBuilder) {
				dir, err := b.CreateDirectory(ext4fs.RootInode, "subdir", 0755, 0, 0)
				require.NoError(t, err)
				b.CreateFile(ext4fs.RootInode, "root.txt", []byte("root file"), 0644, 0, 0)
				b.CreateSymlink(dir, "toroot", "../root.txt", 0, 0)
			},
			verify: []string{
				`readlink "subdir/toroot"`,
				`cat "subdir/toroot"`,
			},
			expects: []string{"../root.txt", "root file"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestContext(t, defaultImageSizeMB)
			ctx.builder.PrepareFilesystem()
			tc.setup(ctx.builder)
			ctx.finalize()

			output := ctx.dockerExecSimple(tc.verify...)

			for _, expected := range tc.expects {
				assert.Contains(t, output, expected)
			}
		})
	}
}

// TestComplexFilesystem tests a realistic filesystem structure
func TestComplexFilesystem(t *testing.T) {
	skipIfNoDocker(t)

	ctx := newTestContext(t, defaultImageSizeMB)
	ctx.builder.PrepareFilesystem()

	// Create /etc with config files
	etcDir, err := ctx.builder.CreateDirectory(ext4fs.RootInode, "etc", 0755, 0, 0)
	require.NoError(t, err)
	ctx.builder.CreateFile(etcDir, "passwd", []byte("root:x:0:0:root:/root:/bin/sh\n"), 0644, 0, 0)
	ctx.builder.CreateFile(etcDir, "hostname", []byte("testhost\n"), 0644, 0, 0)

	// Create /home/user
	homeDir, err := ctx.builder.CreateDirectory(ext4fs.RootInode, "home", 0755, 0, 0)
	require.NoError(t, err)
	userDir, err := ctx.builder.CreateDirectory(homeDir, "user", 0750, 1000, 1000)
	require.NoError(t, err)
	ctx.builder.CreateFile(userDir, ".bashrc", []byte("export PS1='$ '\n"), 0644, 1000, 1000)
	ctx.builder.CreateFile(userDir, ".profile", []byte("# profile\n"), 0644, 1000, 1000)

	// Create /var/log
	varDir, err := ctx.builder.CreateDirectory(ext4fs.RootInode, "var", 0755, 0, 0)
	require.NoError(t, err)
	logDir, err := ctx.builder.CreateDirectory(varDir, "log", 0755, 0, 0)
	require.NoError(t, err)
	ctx.builder.CreateFile(logDir, "messages", []byte("system started\n"), 0640, 0, 4)

	// Create /tmp with sticky bit
	ctx.builder.CreateDirectory(ext4fs.RootInode, "tmp", 0777|ext4fs.S_ISVTX, 0, 0)

	// Create symlink
	ctx.builder.CreateSymlink(userDir, "logs", "/var/log", 1000, 1000)

	ctx.finalize()

	// Comprehensive verification
	output := ctx.dockerExecSimple(
		// Check directory structure
		`find . -type d | sort`,
		`echo "---"`,
		// Check file contents
		`cat etc/passwd`,
		`cat etc/hostname`,
		`echo "---"`,
		// Check permissions
		`stat -c "%a %n" tmp`,
		`stat -c "%u:%g %n" home/user/.bashrc`,
		`echo "---"`,
		// Check symlink
		`readlink home/user/logs`,
	)

	// Verify structure
	assert.Contains(t, output, "./etc")
	assert.Contains(t, output, "./home")
	assert.Contains(t, output, "./home/user")
	assert.Contains(t, output, "./var")
	assert.Contains(t, output, "./var/log")
	assert.Contains(t, output, "./tmp")
	assert.Contains(t, output, "./lost+found")

	// Verify file contents
	assert.Contains(t, output, "root:x:0:0:root:/root:/bin/sh")
	assert.Contains(t, output, "testhost")

	// Verify permissions
	assert.Contains(t, output, "1777 tmp")
	assert.Contains(t, output, "1000:1000 home/user/.bashrc")

	// Verify symlink
	assert.Contains(t, output, "/var/log")
}

// TestLargeFile tests creating a file larger than one block
func TestLargeFile(t *testing.T) {
	skipIfNoDocker(t)

	ctx := newTestContext(t, defaultImageSizeMB)
	ctx.builder.PrepareFilesystem()

	// Create a file larger than 4KB (one block)
	// Use 16KB of data (4 blocks)
	largeContent := make([]byte, 16*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	// Add a recognizable header and footer
	copy(largeContent[:16], []byte("HEADER_START____"))
	copy(largeContent[len(largeContent)-16:], []byte("____FOOTER_END__"))

	ctx.builder.CreateFile(ext4fs.RootInode, "largefile.bin", largeContent, 0644, 0, 0)

	ctx.finalize()

	output := ctx.dockerExecSimple(
		`stat -c "%s" largefile.bin`,
		`head -c 16 largefile.bin`,
		`tail -c 16 largefile.bin`,
	)

	assert.Contains(t, output, "16384") // 16KB
	assert.Contains(t, output, "HEADER_START____")
	assert.Contains(t, output, "____FOOTER_END__")
}

// TestFilesystemIntegrity runs e2fsck to verify filesystem integrity
func TestFilesystemIntegrity(t *testing.T) {
	skipIfNoDocker(t)

	testCases := []struct {
		name  string
		setup func(b *ext4fs.Ext4ImageBuilder)
	}{
		{
			name: "empty filesystem",
			setup: func(b *ext4fs.Ext4ImageBuilder) {
				// Just the base filesystem
			},
		},
		{
			name: "filesystem with files",
			setup: func(b *ext4fs.Ext4ImageBuilder) {
				b.CreateFile(ext4fs.RootInode, "test.txt", []byte("test"), 0644, 0, 0)
			},
		},
		{
			name: "filesystem with directories",
			setup: func(b *ext4fs.Ext4ImageBuilder) {
				dir, err := b.CreateDirectory(ext4fs.RootInode, "subdir", 0755, 0, 0)
				require.NoError(t, err)
				b.CreateFile(dir, "nested.txt", []byte("nested"), 0644, 0, 0)
			},
		},
		{
			name: "complex filesystem",
			setup: func(b *ext4fs.Ext4ImageBuilder) {
				for i := 0; i < 5; i++ {
					dirName := fmt.Sprintf("dir%d", i)
					dir, err := b.CreateDirectory(ext4fs.RootInode, dirName, 0755, 0, 0)
					require.NoError(t, err)
					for j := 0; j < 3; j++ {
						fileName := fmt.Sprintf("file%d.txt", j)
						content := fmt.Sprintf("Content of %s/%s\n", dirName, fileName)
						b.CreateFile(dir, fileName, []byte(content), 0644, 0, 0)
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestContext(t, defaultImageSizeMB)
			ctx.builder.PrepareFilesystem()
			tc.setup(ctx.builder)
			ctx.finalize()

			// The dockerExec already runs e2fsck -n -f, which will fail if there are issues
			// We just need to verify it succeeds
			_, stderr, err := ctx.dockerExec(`echo "Filesystem check passed"`)
			require.NoError(t, err, "e2fsck failed: %s", stderr)
		})
	}
}

// TestDifferentImageSizes tests creating images of various sizes
func TestDifferentImageSizes(t *testing.T) {
	skipIfNoDocker(t)

	testCases := []struct {
		sizeMB int
	}{
		{sizeMB: 16},
		{sizeMB: 32},
		{sizeMB: 64},
		{sizeMB: 128},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%dMB", tc.sizeMB), func(t *testing.T) {
			ctx := newTestContext(t, tc.sizeMB)
			ctx.builder.PrepareFilesystem()

			// Add some content
			ctx.builder.CreateFile(ext4fs.RootInode, "test.txt", []byte("test content"), 0644, 0, 0)
			dir, err := ctx.builder.CreateDirectory(ext4fs.RootInode, "subdir", 0755, 0, 0)
			require.NoError(t, err)
			ctx.builder.CreateFile(dir, "nested.txt", []byte("nested content"), 0644, 0, 0)

			ctx.finalize()

			// Verify filesystem works
			output := ctx.dockerExecSimple(
				`cat test.txt`,
				`cat subdir/nested.txt`,
			)

			assert.Contains(t, output, "test content")
			assert.Contains(t, output, "nested content")
		})
	}
}

// TestFilesystemStatistics verifies that df and other tools work correctly
func TestFilesystemStatistics(t *testing.T) {
	skipIfNoDocker(t)

	ctx := newTestContext(t, defaultImageSizeMB)
	ctx.builder.PrepareFilesystem()

	// Create some files to use space
	ctx.builder.CreateFile(ext4fs.RootInode, "data.bin", make([]byte, 8192), 0644, 0, 0)

	ctx.finalize()

	output := ctx.dockerExecSimple(
		`df -h . | tail -1`,
		`echo "---"`,
		`stat -f -c "%T" .`,
	)

	// Verify filesystem type is ext4 (stat -f shows ext2/ext3/ext4)
	assert.Contains(t, output, "ext")
}

// TestSpecialFileNames tests files with edge-case names
func TestSpecialFileNames(t *testing.T) {
	skipIfNoDocker(t)

	testCases := []struct {
		name     string
		filename string
	}{
		{"single char", "a"},
		{"max length minus safety", strings.Repeat("x", 200)},
		{"with spaces", "file with spaces.txt"},
		{"with dots", "file.multiple.dots.txt"},
		{"hidden file", ".hidden"},
		{"numbers only", "12345"},
		{"mixed case", "MixedCase.TXT"},
		{"underscore", "file_name_with_underscores"},
		{"hyphen", "file-name-with-hyphens"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestContext(t, defaultImageSizeMB)
			ctx.builder.PrepareFilesystem()

			content := fmt.Sprintf("Content of %s\n", tc.filename)
			ctx.builder.CreateFile(ext4fs.RootInode, tc.filename, []byte(content), 0644, 0, 0)

			ctx.finalize()

			output := ctx.dockerExecSimple(
				fmt.Sprintf(`cat "%s"`, tc.filename),
			)

			assert.Contains(t, output, content)
		})
	}
}

// TestMultiBlockDirectory tests that directories can span multiple blocks
// This is important for directories with many entries
func TestMultiBlockDirectory(t *testing.T) {
	skipIfNoDocker(t)

	tc := newTestContext(t, 128) // Use larger image for this test
	tc.builder.PrepareFilesystem()

	// Create a directory that will need multiple blocks
	// Each directory entry takes roughly 12-24 bytes (depends on name length)
	// A 4KB block can hold approximately 170-340 entries
	// We'll create 400 files to ensure we need at least 2 blocks
	testDir, err := tc.builder.CreateDirectory(ext4fs.RootInode, "many_files", 0755, 0, 0)
	require.NoError(t, err)

	numFiles := 400
	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("file_%03d.txt", i)
		content := fmt.Sprintf("This is file number %d\n", i)
		tc.builder.CreateFile(testDir, fileName, []byte(content), 0644, 0, 0)
	}

	tc.finalize()

	// Verify the directory exists and has all files
	output := tc.dockerExecSimple(
		`cd many_files`,
		`ls -1 | wc -l`, // Count files
		`test -f file_000.txt && echo "first file exists"`,
		`test -f file_199.txt && echo "middle file exists"`,
		`test -f file_399.txt && echo "last file exists"`,
		`cat file_000.txt`,
		`cat file_399.txt`,
	)

	assert.Contains(t, output, "400") // Should have 400 files
	assert.Contains(t, output, "first file exists")
	assert.Contains(t, output, "middle file exists")
	assert.Contains(t, output, "last file exists")
	assert.Contains(t, output, "This is file number 0")
	assert.Contains(t, output, "This is file number 399")
}

// TestMultiBlockDirectoryWithSymlinks tests multi-block directories with various entry types
func TestMultiBlockDirectoryWithSymlinks(t *testing.T) {
	skipIfNoDocker(t)

	tc := newTestContext(t, 128)
	tc.builder.PrepareFilesystem()

	// Create a directory with mixed content types
	testDir, err := tc.builder.CreateDirectory(ext4fs.RootInode, "mixed_dir", 0755, 0, 0)
	require.NoError(t, err)

	// Add 100 regular files
	for i := 0; i < 100; i++ {
		fileName := fmt.Sprintf("regular_%03d.txt", i)
		tc.builder.CreateFile(testDir, fileName, []byte("regular file"), 0644, 0, 0)
	}

	// Add 100 subdirectories
	for i := 0; i < 100; i++ {
		dirName := fmt.Sprintf("subdir_%03d", i)
		tc.builder.CreateDirectory(testDir, dirName, 0755, 0, 0)
	}

	// Add 100 symlinks
	for i := 0; i < 100; i++ {
		linkName := fmt.Sprintf("link_%03d", i)
		target := fmt.Sprintf("regular_%03d.txt", i)
		tc.builder.CreateSymlink(testDir, linkName, target, 0, 0)
	}

	tc.finalize()

	// Verify all entries exist
	output := tc.dockerExecSimple(
		`cd mixed_dir`,
		`ls -1 | wc -l`, // Count total entries
		`ls -1 regular_* | wc -l`,
		`ls -1d subdir_* | wc -l`,
		`ls -1 link_* | wc -l`,
		`test -f regular_000.txt && echo "regular file exists"`,
		`test -d subdir_050 && echo "subdir exists"`,
		`test -L link_099 && echo "symlink exists"`,
		`readlink link_050`,
	)

	assert.Contains(t, output, "300") // Total entries
	assert.Contains(t, output, "100") // Regular files, subdirs, and symlinks each
	assert.Contains(t, output, "regular file exists")
	assert.Contains(t, output, "subdir exists")
	assert.Contains(t, output, "symlink exists")
	assert.Contains(t, output, "regular_050.txt") // Symlink target
}

// TestMultiBlockDirectoryNested tests nested directories with many entries
func TestMultiBlockDirectoryNested(t *testing.T) {
	skipIfNoDocker(t)

	tc := newTestContext(t, 128)
	tc.builder.PrepareFilesystem()

	// Create nested structure with many files at each level
	level1, err := tc.builder.CreateDirectory(ext4fs.RootInode, "level1", 0755, 0, 0)
	require.NoError(t, err)

	// Add many files to level1
	for i := 0; i < 200; i++ {
		fileName := fmt.Sprintf("l1_file_%03d.txt", i)
		tc.builder.CreateFile(level1, fileName, []byte("level 1 content"), 0644, 0, 0)
	}

	level2, err := tc.builder.CreateDirectory(level1, "level2", 0755, 0, 0)
	require.NoError(t, err)

	// Add many files to level2
	for i := 0; i < 200; i++ {
		fileName := fmt.Sprintf("l2_file_%03d.txt", i)
		tc.builder.CreateFile(level2, fileName, []byte("level 2 content"), 0644, 0, 0)
	}

	tc.finalize()

	// Verify nested structure
	output := tc.dockerExecSimple(
		`cd level1`,
		`ls -1 l1_file_* | wc -l`,
		`test -d level2 && echo "level2 exists"`,
		`cd level2`,
		`ls -1 l2_file_* | wc -l`,
		`cat l2_file_000.txt`,
	)

	assert.Contains(t, output, "200")
	assert.Contains(t, output, "level2 exists")
	assert.Contains(t, output, "level 2 content")
}

// BenchmarkFilesystemCreation benchmarks the image creation process
func BenchmarkFilesystemCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tmpDir := b.TempDir()
		imagePath := filepath.Join(tmpDir, "bench.img")

		builder, err := ext4fs.NewExt4ImageBuilder(imagePath, 64)
		if err != nil {
			b.Fatal(err)
		}

		builder.PrepareFilesystem()

		// Create some content
		for j := 0; j < 10; j++ {
			dirName := fmt.Sprintf("dir%d", j)
			dir, err := builder.CreateDirectory(ext4fs.RootInode, dirName, 0755, 0, 0)
			require.NoError(b, err)
			for k := 0; k < 5; k++ {
				fileName := fmt.Sprintf("file%d.txt", k)
				content := fmt.Sprintf("Content %d-%d", j, k)
				builder.CreateFile(dir, fileName, []byte(content), 0644, 0, 0)
			}
		}

		builder.FinalizeMetadata()
		if err := builder.Save(); err != nil {
			b.Fatal(err)
		}
		if err := builder.Close(); err != nil {
			b.Fatal(err)
		}

		os.Remove(imagePath)
	}
}

// TestExtentTreeConversion tests the conversion from flat to indexed extent trees
func TestExtentTreeConversion(t *testing.T) {
	skipIfNoDocker(t)

	tc := newTestContext(t, 128)
	tc.builder.PrepareFilesystem()

	// Create a directory and add files until we trigger extent tree conversion
	// Each file creates a directory entry, and when entries fill the first block,
	// a new block is allocated. When we have >4 non-contiguous extent ranges,
	// the extent tree must convert from flat to indexed.

	testDir, err := tc.builder.CreateDirectory(ext4fs.RootInode, "extent_test", 0755, 0, 0)
	require.NoError(t, err)

	// Create many files to force multiple blocks and potential non-contiguous allocation
	for i := 0; i < 500; i++ {
		fileName := fmt.Sprintf("file_%04d.txt", i)
		content := fmt.Sprintf("Content %d\n", i)
		tc.builder.CreateFile(testDir, fileName, []byte(content), 0644, 0, 0)
	}

	tc.finalize()

	// Verify all files exist and are readable
	output := tc.dockerExecSimple(
		`cd extent_test`,
		`ls -1 | wc -l`,
		`test -f file_0000.txt && echo "first exists"`,
		`test -f file_0250.txt && echo "middle exists"`,
		`test -f file_0499.txt && echo "last exists"`,
		`cat file_0000.txt`,
		`cat file_0499.txt`,
	)

	assert.Contains(t, output, "500")
	assert.Contains(t, output, "first exists")
	assert.Contains(t, output, "middle exists")
	assert.Contains(t, output, "last exists")
	assert.Contains(t, output, "Content 0")
	assert.Contains(t, output, "Content 499")
}

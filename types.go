package ext4fs

// ============================================================================
// Constants - ext4 on-disk format
// ============================================================================

const (
	// Block geometry
	SectorSize     = 512
	BlockSize      = 4096
	BlockSizeLog   = 2 // block_size = 1024 << BlockSizeLog
	BlocksPerGroup = 32768
	InodesPerGroup = 8192
	InodeSize      = 256

	// Superblock is always at byte offset 1024 from partition start
	SuperblockOffset = 1024

	// Magic numbers
	Ext4Magic    = 0xEF53
	ExtentMagic  = 0xF30A
	MBRSignature = 0xAA55

	// Reserved inodes
	BadBlocksInode   = 1
	RootInode        = 2
	UserQuotaInode   = 3
	GroupQuotaInode  = 4
	BootLoaderInode  = 5
	UndelDirInode    = 6
	ResizeInode      = 7
	JournalInode     = 8
	ExcludeInode     = 9
	ReplicaInode     = 10
	FirstNonResInode = 11 // First inode for user data

	// Directory entry file types
	FTUnknown = 0
	FTRegFile = 1
	FTDir     = 2
	FTChrDev  = 3
	FTBlkDev  = 4
	FTFifo    = 5
	FTSock    = 6
	FTSymlink = 7
	FTMax     = 8

	// Inode mode bits
	S_IXOTH  = 0o0001
	S_IWOTH  = 0o0002
	S_IROTH  = 0o0004
	S_IXGRP  = 0o0010
	S_IWGRP  = 0o0020
	S_IRGRP  = 0o0040
	S_IXUSR  = 0o0100
	S_IWUSR  = 0o0200
	S_IRUSR  = 0o0400
	S_ISVTX  = 0o1000 // Sticky bit
	S_ISGID  = 0o2000
	S_ISUID  = 0o4000
	S_IFIFO  = 0x1000
	S_IFCHR  = 0x2000
	S_IFDIR  = 0x4000
	S_IFBLK  = 0x6000
	S_IFREG  = 0x8000
	S_IFLNK  = 0xA000
	S_IFSOCK = 0xC000

	// Inode flags
	InodeFlagExtents = 0x00080000

	// Feature flags - MINIMAL SET for kernel compatibility
	// Compatible features (optional)
	CompatExtAttr  = 0x0008
	CompatDirIndex = 0x0020

	// Incompatible features (required)
	IncompatFileType = 0x0002
	IncompatExtents  = 0x0040

	// Read-only compatible features
	ROCompatSparseSuper = 0x0001
	ROCompatLargeFile   = 0x0002
	ROCompatExtraIsize  = 0x0040

	// Group descriptor flags
	BGInodeUninit = 0x0001 // Inode table not initialized
	BGBlockUninit = 0x0002 // Block bitmap not initialized
	BGInodeZeroed = 0x0004 // Inode table zeroed
)

// ============================================================================
// On-disk structures (must match kernel exactly)
// ============================================================================

// MBRPartition is a single MBR partition entry (16 bytes)
type MBRPartition struct {
	BootIndicator byte
	StartCHS      [3]byte
	PartType      byte
	EndCHS        [3]byte
	StartLBA      uint32
	SizeLBA       uint32
}

// MBR is the Master Boot Record (512 bytes)
type MBR struct {
	BootCode   [446]byte
	Partitions [4]MBRPartition
	Signature  uint16
}

// Superblock is the ext4 superblock structure (1024 bytes)
// Only fields needed for minimal ext4 are included with correct offsets
type Superblock struct {
	InodesCount       uint32     // 0x00
	BlocksCountLo     uint32     // 0x04
	RBlocksCountLo    uint32     // 0x08
	FreeBlocksCountLo uint32     // 0x0C
	FreeInodesCount   uint32     // 0x10
	FirstDataBlock    uint32     // 0x14: 0 for 4K blocks
	LogBlockSize      uint32     // 0x18: block_size = 1024 << log_block_size
	LogClusterSize    uint32     // 0x1C
	BlocksPerGroup    uint32     // 0x20
	ClustersPerGroup  uint32     // 0x24
	InodesPerGroup    uint32     // 0x28
	MTime             uint32     // 0x2C
	WTime             uint32     // 0x30
	MntCount          uint16     // 0x34
	MaxMntCount       uint16     // 0x36
	Magic             uint16     // 0x38: 0xEF53
	State             uint16     // 0x3A: 1 = clean
	Errors            uint16     // 0x3C: 1 = continue
	MinorRevLevel     uint16     // 0x3E
	LastCheck         uint32     // 0x40
	CheckInterval     uint32     // 0x44
	CreatorOS         uint32     // 0x48: 0 = Linux
	RevLevel          uint32     // 0x4C: 1 = dynamic
	DefResUID         uint16     // 0x50
	DefResGID         uint16     // 0x52
	FirstInode        uint32     // 0x54: 11
	InodeSize         uint16     // 0x58: 256
	BlockGroupNr      uint16     // 0x5A
	FeatureCompat     uint32     // 0x5C
	FeatureIncompat   uint32     // 0x60
	FeatureROCompat   uint32     // 0x64
	UUID              [16]byte   // 0x68
	VolumeName        [16]byte   // 0x78
	LastMounted       [64]byte   // 0x88
	AlgorithmUsageBmp uint32     // 0xC8
	PreallocBlocks    uint8      // 0xCC
	PreallocDirBlocks uint8      // 0xCD
	ReservedGDTBlocks uint16     // 0xCE
	JournalUUID       [16]byte   // 0xD0
	JournalInum       uint32     // 0xE0
	JournalDev        uint32     // 0xE4
	LastOrphan        uint32     // 0xE8
	HashSeed          [4]uint32  // 0xEC
	DefHashVersion    uint8      // 0xFC
	JnlBackupType     uint8      // 0xFD
	DescSize          uint16     // 0xFE: 32 for 32-bit mode
	DefaultMountOpts  uint32     // 0x100
	FirstMetaBg       uint32     // 0x104
	MkfsTime          uint32     // 0x108
	JnlBlocks         [17]uint32 // 0x10C
	BlocksCountHi     uint32     // 0x150
	RBlocksCountHi    uint32     // 0x154
	FreeBlocksCountHi uint32     // 0x158
	MinExtraIsize     uint16     // 0x15C
	WantExtraIsize    uint16     // 0x15E
	Flags             uint32     // 0x160
	RaidStride        uint16     // 0x164
	MmpInterval       uint16     // 0x166
	MmpBlock          uint64     // 0x168
	RaidStripeWidth   uint32     // 0x170
	LogGroupsPerFlex  uint8      // 0x174
	ChecksumType      uint8      // 0x175
	ReservedPad       uint16     // 0x176
	KBytesWritten     uint64     // 0x178
	SnapshotInum      uint32     // 0x180
	SnapshotID        uint32     // 0x184
	SnapshotRBlksCnt  uint64     // 0x188
	SnapshotList      uint32     // 0x190
	ErrorCount        uint32     // 0x194
	FirstErrorTime    uint32     // 0x198
	FirstErrorIno     uint32     // 0x19C
	FirstErrorBlock   uint64     // 0x1A0
	FirstErrorFunc    [32]byte   // 0x1A8
	FirstErrorLine    uint32     // 0x1C8
	LastErrorTime     uint32     // 0x1CC
	LastErrorIno      uint32     // 0x1D0
	LastErrorLine     uint32     // 0x1D4
	LastErrorBlock    uint64     // 0x1D8
	LastErrorFunc     [32]byte   // 0x1E0
	MountOpts         [64]byte   // 0x200
	UsrQuotaInum      uint32     // 0x240
	GrpQuotaInum      uint32     // 0x244
	OverheadBlocks    uint32     // 0x248
	BackupBgs         [2]uint32  // 0x24C
	EncryptAlgos      [4]uint8   // 0x254
	EncryptPwSalt     [16]byte   // 0x258
	LpfIno            uint32     // 0x268
	PrjQuotaInum      uint32     // 0x26C
	ChecksumSeed      uint32     // 0x270
	WtimeHi           uint8      // 0x274
	MtimeHi           uint8      // 0x275
	MkfsTimeHi        uint8      // 0x276
	LastcheckHi       uint8      // 0x277
	FirstErrorTimeHi  uint8      // 0x278
	LastErrorTimeHi   uint8      // 0x279
	ErrorTimePad      [2]uint8   // 0x27A
	Encoding          uint16     // 0x27C
	EncodingFlags     uint16     // 0x27E
	OrphanFileInum    uint32     // 0x280
	Reserved          [94]uint32 // 0x284
	Checksum          uint32     // 0x3FC
}

// GroupDesc32 is the 32-byte block group descriptor (non-64bit mode)
type GroupDesc32 struct {
	BlockBitmapLo     uint32 // Block bitmap block
	InodeBitmapLo     uint32 // Inode bitmap block
	InodeTableLo      uint32 // Inode table block
	FreeBlocksCountLo uint16 // Free blocks count
	FreeInodesCountLo uint16 // Free inodes count
	UsedDirsCountLo   uint16 // Directories count
	Flags             uint16 // Flags
	ExcludeBitmapLo   uint32 // Exclude bitmap
	BlockBitmapCsumLo uint16 // Block bitmap checksum
	InodeBitmapCsumLo uint16 // Inode bitmap checksum
	ItableUnusedLo    uint16 // Unused inodes count
	Checksum          uint16 // Group checksum
}

// Inode is the ext4 inode structure (256 bytes with extra fields)
type Inode struct {
	Mode        uint16   // 0x00: File mode
	UID         uint16   // 0x02: Owner UID
	SizeLo      uint32   // 0x04: Size in bytes (low)
	Atime       uint32   // 0x08: Access time
	Ctime       uint32   // 0x0C: Change time
	Mtime       uint32   // 0x10: Modification time
	Dtime       uint32   // 0x14: Deletion time
	GID         uint16   // 0x18: Group ID
	LinksCount  uint16   // 0x1A: Links count
	BlocksLo    uint32   // 0x1C: Block count (512-byte units)
	Flags       uint32   // 0x20: Flags
	Version     uint32   // 0x24: Version (osd1)
	Block       [60]byte // 0x28: Block pointers / extent tree
	Generation  uint32   // 0x64: Generation
	FileACLLo   uint32   // 0x68: File ACL
	SizeHi      uint32   // 0x6C: Size high / dir ACL
	ObsoFAddr   uint32   // 0x70: Obsolete
	BlocksHi    uint16   // 0x74: Blocks count high
	FileACLHi   uint16   // 0x76: File ACL high
	UIDHi       uint16   // 0x78: UID high
	GIDHi       uint16   // 0x7A: GID high
	ChecksumLo  uint16   // 0x7C: Checksum low
	Reserved    uint16   // 0x7E: Reserved
	ExtraIsize  uint16   // 0x80: Extra inode size
	ChecksumHi  uint16   // 0x82: Checksum high
	CtimeExtra  uint32   // 0x84: Ctime extra
	MtimeExtra  uint32   // 0x88: Mtime extra
	AtimeExtra  uint32   // 0x8C: Atime extra
	Crtime      uint32   // 0x90: Creation time
	CrtimeExtra uint32   // 0x94: Creation time extra
	VersionHi   uint32   // 0x98: Version high
	Projid      uint32   // 0x9C: Project ID
	// Padding to 256 bytes
	Padding [96]byte // 0xA0-0xFF
}

// ExtentHeader is the extent tree header (12 bytes)
type ExtentHeader struct {
	Magic      uint16 // 0xF30A
	Entries    uint16 // Number of entries
	Max        uint16 // Max entries
	Depth      uint16 // Tree depth (0 = leaf)
	Generation uint32 // Generation
}

// Extent is a leaf extent (12 bytes)
type Extent struct {
	Block   uint32 // First logical block
	Len     uint16 // Number of blocks
	StartHi uint16 // Physical block high
	StartLo uint32 // Physical block low
}

// DirEntry is a directory entry (variable length)
type DirEntry struct {
	Inode   uint32 // Inode number
	RecLen  uint16 // Record length
	NameLen uint8  // Name length
	Type    uint8  // File type
	Name    []byte // Name (up to 255)
}

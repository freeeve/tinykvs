package tinykvs

import "time"

// Options configures the Store behavior.
type Options struct {
	// Dir is the base directory for all data files.
	Dir string

	// MemtableSize is the max memtable size in bytes before flush.
	// Default: 4MB
	MemtableSize int64

	// BlockCacheSize is the LRU cache size in bytes.
	// Set to 0 for minimal memory usage (no caching).
	// Default: 64MB
	BlockCacheSize int64

	// BlockSize is the target block size before compression.
	// Default: 16KB
	BlockSize int

	// CompressionType determines which compression algorithm to use.
	// Default: CompressionZstd
	CompressionType CompressionType

	// CompressionLevel is the zstd compression level (ignored for snappy).
	// 1 = fastest, 3 = default, higher = better compression.
	// Default: 1 (fastest)
	CompressionLevel int

	// BloomFPRate is the target false positive rate for bloom filters.
	// Default: 0.01 (1%)
	BloomFPRate float64

	// CompactionStyle determines the compaction strategy.
	// Default: CompactionStyleLeveled
	CompactionStyle CompactionStyle

	// L0CompactionTrigger is the number of L0 files that triggers compaction.
	// Default: 4
	L0CompactionTrigger int

	// L0CompactionBatchSize limits how many L0 files to compact at once.
	// Lower values mean faster individual compactions but more total compactions.
	// Default: 0 (no limit - compact all L0 files)
	L0CompactionBatchSize int

	// MaxLevels is the maximum number of LSM levels.
	// Default: 7
	MaxLevels int

	// LevelSizeMultiplier is the size ratio between adjacent levels.
	// Default: 10
	LevelSizeMultiplier int

	// WALSyncMode determines when WAL is synced to disk.
	// Default: WALSyncPerBatch
	WALSyncMode WALSyncMode

	// FlushInterval is the automatic flush interval.
	// Default: 30s
	FlushInterval time.Duration

	// CompactionInterval is how often to check for compaction.
	// Default: 1s
	CompactionInterval time.Duration

	// VerifyChecksums enables checksum verification on reads.
	// Default: true
	VerifyChecksums bool

	// DisableBloomFilter disables bloom filters for minimal memory.
	// Default: false
	DisableBloomFilter bool
}

// CompactionStyle determines the compaction strategy.
type CompactionStyle int

const (
	// CompactionStyleLeveled uses leveled compaction (read-optimized).
	CompactionStyleLeveled CompactionStyle = iota
	// CompactionStyleSizeTiered uses size-tiered compaction (write-optimized).
	CompactionStyleSizeTiered
)

// WALSyncMode determines when WAL is synced to disk.
type WALSyncMode int

const (
	// WALSyncNone never syncs. Fastest but may lose data on crash.
	WALSyncNone WALSyncMode = iota
	// WALSyncPerBatch syncs after each batch of writes. Good balance.
	WALSyncPerBatch
	// WALSyncPerWrite syncs after each write. Slowest but safest.
	WALSyncPerWrite
)

// CompressionType determines the compression algorithm.
type CompressionType int

const (
	// CompressionZstd uses zstd compression (good compression, fast).
	CompressionZstd CompressionType = iota
	// CompressionSnappy uses snappy compression (faster, less compression).
	CompressionSnappy
	// CompressionNone disables compression.
	CompressionNone
)

// DefaultOptions returns production-ready defaults for the given directory.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:                 dir,
		MemtableSize:        4 * 1024 * 1024,  // 4MB
		BlockCacheSize:      64 * 1024 * 1024, // 64MB
		BlockSize:           16 * 1024,        // 16KB - fast random access
		CompressionLevel:    1,                // zstd fastest
		BloomFPRate:         0.01,             // 1% false positive
		CompactionStyle:     CompactionStyleLeveled,
		L0CompactionTrigger: 4,
		MaxLevels:           7,
		LevelSizeMultiplier: 10,
		WALSyncMode:         WALSyncPerBatch,
		FlushInterval:       30 * time.Second,
		CompactionInterval:  time.Second,
		VerifyChecksums:     true,
	}
}

// LowMemoryOptions returns options for memory-constrained environments.
// Suitable for running billions of records on systems with <1GB RAM.
func LowMemoryOptions(dir string) Options {
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4 * 1024 * 1024 // 4MB (good balance of throughput vs memory)
	opts.BlockCacheSize = 0             // No cache
	opts.DisableBloomFilter = true      // No bloom filters
	opts.L0CompactionTrigger = 8        // Less frequent compaction
	return opts
}

// HighPerformanceOptions returns options optimized for performance.
func HighPerformanceOptions(dir string) Options {
	opts := DefaultOptions(dir)
	opts.MemtableSize = 64 * 1024 * 1024    // 64MB
	opts.BlockCacheSize = 512 * 1024 * 1024 // 512MB
	opts.WALSyncMode = WALSyncNone          // No sync (risk of data loss)
	return opts
}

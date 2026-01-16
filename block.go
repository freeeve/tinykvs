package tinykvs

import (
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// Pooled zstd decoder for efficient reuse
var zstdDecoderPool = sync.Pool{
	New: func() interface{} {
		decoder, _ := zstd.NewReader(nil)
		return decoder
	},
}

// Buffer pools for decompression - size classes to reduce fragmentation
// Sizes: 4KB, 16KB, 64KB, 256KB, 1MB
var decompressPools = [5]sync.Pool{
	{New: func() interface{} { return make([]byte, 0, 4*1024) }},
	{New: func() interface{} { return make([]byte, 0, 16*1024) }},
	{New: func() interface{} { return make([]byte, 0, 64*1024) }},
	{New: func() interface{} { return make([]byte, 0, 256*1024) }},
	{New: func() interface{} { return make([]byte, 0, 1024*1024) }},
}

var decompressPoolSizes = [5]int{4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024}

func getDecompressBuffer(size int) []byte {
	for i, poolSize := range decompressPoolSizes {
		if size <= poolSize {
			buf := decompressPools[i].Get().([]byte)
			return buf[:0]
		}
	}
	// Too large for pools, allocate directly
	return make([]byte, 0, size)
}

func putDecompressBuffer(buf []byte) {
	cap := cap(buf)
	for i, poolSize := range decompressPoolSizes {
		if cap == poolSize {
			decompressPools[i].Put(buf[:0])
			return
		}
	}
	// Not a pooled size, let GC handle it
}

// Compression buffer pools (for snappy encode output)
// Use same size classes as decompress pools
var compressPools = [5]sync.Pool{
	{New: func() interface{} { return make([]byte, 0, 4*1024) }},
	{New: func() interface{} { return make([]byte, 0, 16*1024) }},
	{New: func() interface{} { return make([]byte, 0, 64*1024) }},
	{New: func() interface{} { return make([]byte, 0, 256*1024) }},
	{New: func() interface{} { return make([]byte, 0, 1024*1024) }},
}

func getCompressBuffer(size int) []byte {
	for i, poolSize := range decompressPoolSizes {
		if size <= poolSize {
			buf := compressPools[i].Get().([]byte)
			return buf[:0]
		}
	}
	return make([]byte, 0, size)
}

func putCompressBuffer(buf []byte) {
	cap := cap(buf)
	for i, poolSize := range decompressPoolSizes {
		if cap == poolSize {
			compressPools[i].Put(buf[:0])
			return
		}
	}
}

// Channel-based encoder pools (won't be cleared by GC like sync.Pool)
// We use channels instead of sync.Pool to prevent GC from clearing encoders
// under memory pressure, since encoder initialization is expensive (~8MB).
var zstdEncoderPools [5]chan *zstd.Encoder

func init() {
	poolSize := 4 // Keep up to 4 encoders per level
	for i := 0; i <= 4; i++ {
		zstdEncoderPools[i] = make(chan *zstd.Encoder, poolSize)
	}
}

func getEncoder(level int) *zstd.Encoder {
	if level < 0 {
		level = 0
	} else if level > 4 {
		level = 4
	}
	select {
	case enc := <-zstdEncoderPools[level]:
		return enc
	default:
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
		return enc
	}
}

func putEncoder(level int, enc *zstd.Encoder) {
	if level < 0 {
		level = 0
	} else if level > 4 {
		level = 4
	}
	select {
	case zstdEncoderPools[level] <- enc:
	default:
		// Pool full, let it be GC'd
		enc.Close()
	}
}

// Block types
const (
	BlockTypeData  uint8 = 1
	BlockTypeIndex uint8 = 2
	BlockTypeBloom uint8 = 3
	BlockTypeMeta  uint8 = 4
)

// BlockFooterSize is the size of the block footer in bytes.
const BlockFooterSize = 13 // checksum(4) + uncompressed_size(4) + compressed_size(4) + compression_type(1)

// LegacyBlockFooterSize is the v0.3.x footer size (no compression type byte).
const LegacyBlockFooterSize = 12 // checksum(4) + uncompressed_size(4) + compressed_size(4)

// Compression type markers in block footer
const (
	compressionTypeZstd   uint8 = 0
	compressionTypeSnappy uint8 = 1
	compressionTypeNone   uint8 = 2
)

// BlockEntry represents a key-value pair within a block.
type BlockEntry struct {
	Key   []byte
	Value []byte // Encoded value bytes
}

// Block represents a decompressed data block.
type Block struct {
	Type    uint8
	Entries []BlockEntry
	buffer  []byte // buffer for decompressed data
	pooled  bool   // true if buffer came from pool and should be returned
}

// Release returns the block's buffer to the pool.
// Call this when the block is no longer needed.
func (b *Block) Release() {
	if b.buffer != nil {
		if b.pooled {
			putDecompressBuffer(b.buffer)
		}
		b.buffer = nil
		b.Entries = nil
	}
}

// BlockBuilder builds blocks from entries.
type BlockBuilder struct {
	entries   []BlockEntry
	size      int
	blockSize int

	// Arena for value copies to avoid per-entry allocations
	arena       []byte
	arenaOffset int

	// Reusable buffer for Build() (uncompressed data)
	buildBuf []byte

	// Reusable buffer for compressed output
	compressBuf []byte
}

// NewBlockBuilder creates a new block builder.
func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{
		entries:     make([]BlockEntry, 0, 64),
		blockSize:   blockSize,
		arena:       make([]byte, blockSize*2), // Pre-allocate arena
		buildBuf:    make([]byte, 0, blockSize+1024),
		compressBuf: make([]byte, 0, snappy.MaxEncodedLen(blockSize+1024)),
	}
}

// Add adds an entry to the block.
// Returns true if the entry was added, false if the block is full.
func (b *BlockBuilder) Add(key, value []byte) bool {
	entrySize := 4 + len(key) + 4 + len(value) // key_len + key + val_len + val
	if b.size > 0 && b.size+entrySize > b.blockSize {
		return false // Block is full
	}

	// Copy value to arena to avoid issues with reused buffers
	valueCopy := b.arenaAlloc(len(value))
	copy(valueCopy, value)

	b.entries = append(b.entries, BlockEntry{
		Key:   key,
		Value: valueCopy,
	})
	b.size += entrySize
	return true
}

// arenaAlloc allocates from the arena, growing if needed.
func (b *BlockBuilder) arenaAlloc(size int) []byte {
	if b.arenaOffset+size > len(b.arena) {
		// Grow arena
		newSize := len(b.arena) * 2
		if newSize < b.arenaOffset+size {
			newSize = b.arenaOffset + size
		}
		newArena := make([]byte, newSize)
		copy(newArena, b.arena[:b.arenaOffset])
		b.arena = newArena
	}
	result := b.arena[b.arenaOffset : b.arenaOffset+size]
	b.arenaOffset += size
	return result
}

// Build serializes and compresses the block.
func (b *BlockBuilder) Build(blockType uint8, compressionLevel int) ([]byte, error) {
	return b.BuildWithCompression(blockType, CompressionZstd, compressionLevel)
}

// BuildWithCompression serializes and compresses the block with the specified compression type.
func (b *BlockBuilder) BuildWithCompression(blockType uint8, compressionType CompressionType, compressionLevel int) ([]byte, error) {
	// Reuse build buffer
	buf := b.buildBuf[:0]

	// Serialize entries
	// Header: type(1) + num_entries(2)
	buf = append(buf, blockType)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(b.entries)))

	// Entries: [key_len(4)][key][val_len(4)][val]...
	for _, entry := range b.entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Key)))
		buf = append(buf, entry.Key...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Value)))
		buf = append(buf, entry.Value...)
	}

	// Save buffer for reuse (may have grown)
	b.buildBuf = buf

	uncompressedSize := len(buf)

	// Compress based on type
	var compressed []byte
	var compType uint8

	switch compressionType {
	case CompressionSnappy:
		// Reuse compressBuf, grow if needed
		maxLen := snappy.MaxEncodedLen(len(buf))
		if cap(b.compressBuf) < maxLen {
			b.compressBuf = make([]byte, 0, maxLen)
		}
		compressed = snappy.Encode(b.compressBuf[:maxLen], buf)
		compType = compressionTypeSnappy
	case CompressionNone:
		// Reuse compressBuf for uncompressed copy
		if cap(b.compressBuf) < len(buf) {
			b.compressBuf = make([]byte, len(buf))
		}
		compressed = b.compressBuf[:len(buf)]
		copy(compressed, buf)
		compType = compressionTypeNone
	default: // CompressionZstd
		encoder := getEncoder(compressionLevel)
		// Reuse compressBuf for zstd output
		if cap(b.compressBuf) < len(buf) {
			b.compressBuf = make([]byte, 0, len(buf))
		}
		compressed = encoder.EncodeAll(buf, b.compressBuf[:0])
		putEncoder(compressionLevel, encoder)
		compType = compressionTypeZstd
	}

	// Append footer: checksum(4) + uncompressed_size(4) + compressed_size(4) + compression_type(1)
	checksum := crc32.ChecksumIEEE(compressed)
	footer := make([]byte, BlockFooterSize)
	binary.LittleEndian.PutUint32(footer[0:], checksum)
	binary.LittleEndian.PutUint32(footer[4:], uint32(uncompressedSize))
	binary.LittleEndian.PutUint32(footer[8:], uint32(len(compressed)))
	footer[12] = compType

	return append(compressed, footer...), nil
}

// Reset clears the builder for reuse.
func (b *BlockBuilder) Reset() {
	b.entries = b.entries[:0]
	b.size = 0
	b.arenaOffset = 0 // Reset arena for next block
}

// Count returns the number of entries in the block.
func (b *BlockBuilder) Count() int {
	return len(b.entries)
}

// Size returns the current uncompressed block size.
func (b *BlockBuilder) Size() int {
	return b.size
}

// FirstKey returns the first key in the block, or nil if empty.
func (b *BlockBuilder) FirstKey() []byte {
	if len(b.entries) == 0 {
		return nil
	}
	return b.entries[0].Key
}

// Entries returns the entries in the block.
func (b *BlockBuilder) Entries() []BlockEntry {
	return b.entries
}

// DecodeBlock decompresses and parses a block.
// The returned Block's entries reference the internal decompressed buffer,
// so the Block should not be modified and is only valid while cached.
// Call Block.Release() when done to return the buffer to the pool.
func DecodeBlock(data []byte, verifyChecksum bool) (*Block, error) {
	if len(data) < LegacyBlockFooterSize {
		return nil, ErrCorruptedData
	}

	// Try new format (13-byte footer) first, fall back to legacy (12-byte) if needed
	var checksum, uncompressedSize, compressedSize uint32
	var compType uint8
	var compressed []byte
	isLegacy := false

	if len(data) >= BlockFooterSize {
		// Try new format first
		footer := data[len(data)-BlockFooterSize:]
		checksum = binary.LittleEndian.Uint32(footer[0:])
		uncompressedSize = binary.LittleEndian.Uint32(footer[4:])
		compressedSize = binary.LittleEndian.Uint32(footer[8:])
		compType = footer[12]
		compressed = data[:len(data)-BlockFooterSize]

		// Check if this looks like new format:
		// - compressedSize must match
		// - compType must be valid (0, 1, or 2)
		if uint32(len(compressed)) != compressedSize || compType > compressionTypeNone {
			// Try legacy format
			isLegacy = true
		}
	} else {
		isLegacy = true
	}

	if isLegacy {
		if len(data) < LegacyBlockFooterSize {
			return nil, ErrCorruptedData
		}
		footer := data[len(data)-LegacyBlockFooterSize:]
		checksum = binary.LittleEndian.Uint32(footer[0:])
		uncompressedSize = binary.LittleEndian.Uint32(footer[4:])
		compressedSize = binary.LittleEndian.Uint32(footer[8:])
		compType = compressionTypeZstd // Legacy files used zstd
		compressed = data[:len(data)-LegacyBlockFooterSize]
	}

	if uint32(len(compressed)) != compressedSize {
		return nil, ErrCorruptedData
	}

	if verifyChecksum && crc32.ChecksumIEEE(compressed) != checksum {
		return nil, ErrChecksumMismatch
	}

	// Get pooled buffer for decompression
	buf := getDecompressBuffer(int(uncompressedSize))

	// Decompress based on compression type
	var decompressed []byte
	var err error

	// Track whether decompressed buffer came from pool
	pooled := true

	switch compType {
	case compressionTypeSnappy:
		// Validate decompressed length before allocating/decoding
		// This catches corrupted footers and invalid snappy data early
		decodedLen, err := snappy.DecodedLen(compressed)
		if err != nil {
			putDecompressBuffer(buf)
			return nil, err
		}
		if decodedLen != int(uncompressedSize) {
			putDecompressBuffer(buf)
			return nil, ErrCorruptedData
		}
		// snappy.Decode checks len(dst), not cap(dst), so pre-size it
		buf = buf[:uncompressedSize]
		decompressed, err = snappy.Decode(buf, compressed)
		if err != nil {
			putDecompressBuffer(buf)
			return nil, err
		}
		// Verify snappy used our buffer (defensive check)
		if len(decompressed) > 0 && len(buf) > 0 && &decompressed[0] != &buf[0] {
			// snappy allocated a new buffer - this shouldn't happen
			// after DecodedLen validation, but handle it safely
			putDecompressBuffer(buf)
			pooled = false // decompressed is not from pool
		}
	case compressionTypeNone:
		decompressed = buf[:len(compressed)]
		copy(decompressed, compressed)
	default: // compressionTypeZstd (0) or legacy
		decoder := zstdDecoderPool.Get().(*zstd.Decoder)
		decompressed, err = decoder.DecodeAll(compressed, buf)
		zstdDecoderPool.Put(decoder)
		if err != nil {
			putDecompressBuffer(buf)
			return nil, err
		}
		// Check if zstd allocated a new buffer (can happen if buf was too small)
		if len(decompressed) > 0 && len(buf) > 0 && &decompressed[0] != &buf[0] {
			putDecompressBuffer(buf)
			pooled = false
		}
	}

	// Parse header
	if len(decompressed) < 3 {
		if pooled {
			putDecompressBuffer(decompressed)
		}
		return nil, ErrCorruptedData
	}

	blockType := decompressed[0]
	numEntries := binary.LittleEndian.Uint16(decompressed[1:])

	// Parse entries with zero-copy (point directly into decompressed buffer)
	entries := make([]BlockEntry, numEntries)
	pos := 3

	for i := uint16(0); i < numEntries; i++ {
		if pos+4 > len(decompressed) {
			if pooled {
				putDecompressBuffer(decompressed)
			}
			return nil, ErrCorruptedData
		}
		keyLen := int(binary.LittleEndian.Uint32(decompressed[pos:]))
		pos += 4

		if pos+keyLen+4 > len(decompressed) {
			if pooled {
				putDecompressBuffer(decompressed)
			}
			return nil, ErrCorruptedData
		}
		// Zero-copy: slice into decompressed buffer
		entries[i].Key = decompressed[pos : pos+keyLen]
		pos += keyLen

		valLen := int(binary.LittleEndian.Uint32(decompressed[pos:]))
		pos += 4

		if pos+valLen > len(decompressed) {
			if pooled {
				putDecompressBuffer(decompressed)
			}
			return nil, ErrCorruptedData
		}
		// Zero-copy: slice into decompressed buffer
		entries[i].Value = decompressed[pos : pos+valLen]
		pos += valLen
	}

	return &Block{
		Type:    blockType,
		Entries: entries,
		buffer:  decompressed,
		pooled:  pooled,
	}, nil
}

// SearchBlock performs binary search within a block for a key.
// Returns the index of the matching entry, or -1 if not found.
func SearchBlock(block *Block, key []byte) int {
	lo, hi := 0, len(block.Entries)-1

	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := CompareKeys(block.Entries[mid].Key, key)

		if cmp == 0 {
			return mid
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return -1
}

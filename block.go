package tinykvs

import (
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minlz"
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

// maxBlockSize is the maximum allowed uncompressed block size (64MB).
// This prevents OOM from malformed blocks claiming huge uncompressed sizes.
const maxBlockSize = 64 * 1024 * 1024

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

// block types
const (
	blockTypeData  uint8 = 1
	blockTypeIndex uint8 = 2
	blockTypeBloom uint8 = 3
	blockTypeMeta  uint8 = 4
)

// blockFooterSize is the size of the block footer in bytes.
const blockFooterSize = 13 // checksum(4) + uncompressed_size(4) + compressed_size(4) + compression_type(1)

// Compression type markers in block footer
const (
	compressionTypeZstd   uint8 = 0
	compressionTypeSnappy uint8 = 1
	compressionTypeNone   uint8 = 2
	compressionTypeMinLZ  uint8 = 3
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

// blockBuilder builds blocks from entries.
type blockBuilder struct {
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

// newBlockBuilder creates a new block builder.
func newBlockBuilder(blockSize int) *blockBuilder {
	return &blockBuilder{
		entries:     make([]BlockEntry, 0, 64),
		blockSize:   blockSize,
		arena:       make([]byte, blockSize*2), // Pre-allocate arena
		buildBuf:    make([]byte, 0, blockSize+1024),
		compressBuf: make([]byte, 0, snappy.MaxEncodedLen(blockSize+1024)),
	}
}

// Add adds an entry to the block.
// Returns true if the entry was added, false if the block is full.
func (b *blockBuilder) Add(key, value []byte) bool {
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
func (b *blockBuilder) arenaAlloc(size int) []byte {
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
func (b *blockBuilder) Build(blockType uint8, compressionLevel int) ([]byte, error) {
	return b.BuildWithCompression(blockType, CompressionZstd, compressionLevel)
}

// BuildWithCompression serializes and compresses the block with the specified compression type.
func (b *blockBuilder) BuildWithCompression(blockType uint8, compressionType CompressionType, compressionLevel int) ([]byte, error) {
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
	case CompressionMinLZ:
		// Map compression level to minlz level
		level := minlz.LevelFastest
		if compressionLevel >= 3 {
			level = minlz.LevelSmallest
		} else if compressionLevel >= 2 {
			level = minlz.LevelBalanced
		}
		var err error
		compressed, err = minlz.Encode(b.compressBuf[:0], buf, level)
		if err != nil {
			return nil, err
		}
		// Save buffer if it was reallocated
		if cap(compressed) > cap(b.compressBuf) {
			b.compressBuf = compressed[:0]
		}
		compType = compressionTypeMinLZ
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
	footer := make([]byte, blockFooterSize)
	binary.LittleEndian.PutUint32(footer[0:], checksum)
	binary.LittleEndian.PutUint32(footer[4:], uint32(uncompressedSize))
	binary.LittleEndian.PutUint32(footer[8:], uint32(len(compressed)))
	footer[12] = compType

	return append(compressed, footer...), nil
}

// Reset clears the builder for reuse.
func (b *blockBuilder) Reset() {
	b.entries = b.entries[:0]
	b.size = 0
	b.arenaOffset = 0 // Reset arena for next block
}

// Count returns the number of entries in the block.
func (b *blockBuilder) Count() int {
	return len(b.entries)
}

// Size returns the current uncompressed block size.
func (b *blockBuilder) Size() int {
	return b.size
}

// FirstKey returns the first key in the block, or nil if empty.
func (b *blockBuilder) FirstKey() []byte {
	if len(b.entries) == 0 {
		return nil
	}
	return b.entries[0].Key
}

// Entries returns the entries in the block.
func (b *blockBuilder) Entries() []BlockEntry {
	return b.entries
}

// blockFooter holds parsed footer data.
type blockFooter struct {
	checksum         uint32
	uncompressedSize uint32
	compressedSize   uint32
	compType         uint8
	compressed       []byte
}

// DecodeBlock decompresses and parses a block.
// The returned Block's entries reference the internal decompressed buffer,
// so the Block should not be modified and is only valid while cached.
// Call Block.Release() when done to return the buffer to the pool.
func DecodeBlock(data []byte, verifyChecksum bool) (*Block, error) {
	footer, err := parseBlockFooter(data, verifyChecksum)
	if err != nil {
		return nil, err
	}

	decompressed, pooled, err := decompressBlockData(footer)
	if err != nil {
		return nil, err
	}

	return parseBlockContents(decompressed, pooled)
}

// parseBlockFooter validates and parses the block footer.
func parseBlockFooter(data []byte, verifyChecksum bool) (*blockFooter, error) {
	if len(data) < blockFooterSize {
		return nil, ErrCorruptedData
	}

	footer := data[len(data)-blockFooterSize:]
	f := &blockFooter{
		checksum:         binary.LittleEndian.Uint32(footer[0:]),
		uncompressedSize: binary.LittleEndian.Uint32(footer[4:]),
		compressedSize:   binary.LittleEndian.Uint32(footer[8:]),
		compType:         footer[12],
		compressed:       data[:len(data)-blockFooterSize],
	}

	if uint32(len(f.compressed)) != f.compressedSize {
		return nil, ErrCorruptedData
	}
	if f.compType > compressionTypeMinLZ {
		return nil, ErrCorruptedData
	}
	if verifyChecksum && crc32.ChecksumIEEE(f.compressed) != f.checksum {
		return nil, ErrChecksumMismatch
	}
	if f.uncompressedSize > maxBlockSize {
		return nil, ErrCorruptedData
	}
	return f, nil
}

// decompressBlockData decompresses block data based on compression type.
func decompressBlockData(f *blockFooter) (decompressed []byte, pooled bool, err error) {
	buf := getDecompressBuffer(int(f.uncompressedSize))
	pooled = true

	switch f.compType {
	case compressionTypeSnappy:
		decompressed, pooled, err = decompressSnappy(f, buf)
	case compressionTypeNone:
		decompressed = buf[:len(f.compressed)]
		copy(decompressed, f.compressed)
	case compressionTypeMinLZ:
		decompressed, pooled, err = decompressMinLZ(f, buf)
	default:
		decompressed, pooled, err = decompressZstd(f, buf)
	}

	if err != nil {
		putDecompressBuffer(buf)
	}
	return decompressed, pooled, err
}

// decompressSnappy handles snappy decompression.
func decompressSnappy(f *blockFooter, buf []byte) ([]byte, bool, error) {
	decodedLen, err := snappy.DecodedLen(f.compressed)
	if err != nil {
		return nil, true, err
	}
	if decodedLen != int(f.uncompressedSize) {
		return nil, true, ErrCorruptedData
	}
	buf = buf[:f.uncompressedSize]
	decompressed, err := snappy.Decode(buf, f.compressed)
	if err != nil {
		return nil, true, err
	}
	pooled := !(len(decompressed) > 0 && len(buf) > 0 && &decompressed[0] != &buf[0])
	if !pooled {
		putDecompressBuffer(buf)
	}
	return decompressed, pooled, nil
}

// decompressZstd handles zstd decompression.
func decompressZstd(f *blockFooter, buf []byte) ([]byte, bool, error) {
	decoder := zstdDecoderPool.Get().(*zstd.Decoder)
	decompressed, err := decoder.DecodeAll(f.compressed, buf)
	zstdDecoderPool.Put(decoder)
	if err != nil {
		return nil, true, err
	}
	pooled := !(len(decompressed) > 0 && len(buf) > 0 && &decompressed[0] != &buf[0])
	if !pooled {
		putDecompressBuffer(buf)
	}
	return decompressed, pooled, nil
}

// decompressMinLZ handles minlz decompression.
func decompressMinLZ(f *blockFooter, buf []byte) ([]byte, bool, error) {
	decodedLen, err := minlz.DecodedLen(f.compressed)
	if err != nil {
		return nil, true, err
	}
	if decodedLen != int(f.uncompressedSize) {
		return nil, true, ErrCorruptedData
	}
	buf = buf[:f.uncompressedSize]
	decompressed, err := minlz.Decode(buf, f.compressed)
	if err != nil {
		return nil, true, err
	}
	pooled := !(len(decompressed) > 0 && len(buf) > 0 && &decompressed[0] != &buf[0])
	if !pooled {
		putDecompressBuffer(buf)
	}
	return decompressed, pooled, nil
}

// parseBlockContents parses entries from decompressed block data.
func parseBlockContents(decompressed []byte, pooled bool) (*Block, error) {
	if len(decompressed) < 3 {
		releaseIfPooled(decompressed, pooled)
		return nil, ErrCorruptedData
	}

	blockType := decompressed[0]
	numEntries := binary.LittleEndian.Uint16(decompressed[1:])
	entries := make([]BlockEntry, numEntries)
	pos := 3

	for i := uint16(0); i < numEntries; i++ {
		var err error
		pos, err = parseBlockEntry(decompressed, pos, &entries[i])
		if err != nil {
			releaseIfPooled(decompressed, pooled)
			return nil, err
		}
	}

	return &Block{
		Type:    blockType,
		Entries: entries,
		buffer:  decompressed,
		pooled:  pooled,
	}, nil
}

// parseBlockEntry parses a single entry from decompressed data at pos.
func parseBlockEntry(data []byte, pos int, entry *BlockEntry) (int, error) {
	if pos+4 > len(data) {
		return 0, ErrCorruptedData
	}
	keyLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if pos+keyLen+4 > len(data) {
		return 0, ErrCorruptedData
	}
	entry.Key = data[pos : pos+keyLen]
	pos += keyLen

	valLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if pos+valLen > len(data) {
		return 0, ErrCorruptedData
	}
	entry.Value = data[pos : pos+valLen]
	pos += valLen

	return pos, nil
}

// releaseIfPooled returns buffer to pool if it was pooled.
func releaseIfPooled(buf []byte, pooled bool) {
	if pooled {
		putDecompressBuffer(buf)
	}
}

// searchBlock performs binary search within a block for a key.
// Returns the index of the matching entry, or -1 if not found.
func searchBlock(block *Block, key []byte) int {
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

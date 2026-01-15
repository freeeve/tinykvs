package tinykvs

import (
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Pooled zstd decoder for efficient reuse
var zstdDecoderPool = sync.Pool{
	New: func() interface{} {
		decoder, _ := zstd.NewReader(nil)
		return decoder
	},
}

// Pooled zstd encoders by compression level
var zstdEncoderPools [5]sync.Pool

func init() {
	for i := 0; i <= 4; i++ {
		level := i
		zstdEncoderPools[i] = sync.Pool{
			New: func() interface{} {
				encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
				return encoder
			},
		}
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
const BlockFooterSize = 12 // checksum(4) + uncompressed_size(4) + compressed_size(4)

// BlockEntry represents a key-value pair within a block.
type BlockEntry struct {
	Key   []byte
	Value []byte // Encoded value bytes
}

// Block represents a decompressed data block.
type Block struct {
	Type    uint8
	Entries []BlockEntry
}

// BlockBuilder builds blocks from entries.
type BlockBuilder struct {
	entries   []BlockEntry
	size      int
	blockSize int
}

// NewBlockBuilder creates a new block builder.
func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{
		entries:   make([]BlockEntry, 0, 64),
		blockSize: blockSize,
	}
}

// Add adds an entry to the block.
// Returns true if the entry was added, false if the block is full.
func (b *BlockBuilder) Add(key, value []byte) bool {
	entrySize := 4 + len(key) + 4 + len(value) // key_len + key + val_len + val
	if b.size > 0 && b.size+entrySize > b.blockSize {
		return false // Block is full
	}

	// Copy value to avoid issues with reused buffers
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	b.entries = append(b.entries, BlockEntry{
		Key:   key,
		Value: valueCopy,
	})
	b.size += entrySize
	return true
}

// Build serializes and compresses the block.
func (b *BlockBuilder) Build(blockType uint8, compressionLevel int) ([]byte, error) {
	// Serialize entries
	// Header: type(1) + num_entries(2)
	buf := make([]byte, 0, b.size+8)
	buf = append(buf, blockType)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(b.entries)))

	// Entries: [key_len(4)][key][val_len(4)][val]...
	for _, entry := range b.entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Key)))
		buf = append(buf, entry.Key...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Value)))
		buf = append(buf, entry.Value...)
	}

	uncompressedSize := len(buf)

	// Compress with pooled zstd encoder
	poolIdx := compressionLevel
	if poolIdx < 0 {
		poolIdx = 0
	} else if poolIdx > 4 {
		poolIdx = 4
	}
	encoder := zstdEncoderPools[poolIdx].Get().(*zstd.Encoder)
	compressed := encoder.EncodeAll(buf, make([]byte, 0, len(buf)))
	zstdEncoderPools[poolIdx].Put(encoder)

	// Append footer: checksum(4) + uncompressed_size(4) + compressed_size(4)
	checksum := crc32.ChecksumIEEE(compressed)
	footer := make([]byte, BlockFooterSize)
	binary.LittleEndian.PutUint32(footer[0:], checksum)
	binary.LittleEndian.PutUint32(footer[4:], uint32(uncompressedSize))
	binary.LittleEndian.PutUint32(footer[8:], uint32(len(compressed)))

	return append(compressed, footer...), nil
}

// Reset clears the builder for reuse.
func (b *BlockBuilder) Reset() {
	b.entries = b.entries[:0]
	b.size = 0
}

// Size returns the current uncompressed block size.
func (b *BlockBuilder) Size() int {
	return b.size
}

// Count returns the number of entries in the block.
func (b *BlockBuilder) Count() int {
	return len(b.entries)
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
func DecodeBlock(data []byte, verifyChecksum bool) (*Block, error) {
	if len(data) < BlockFooterSize {
		return nil, ErrCorruptedData
	}

	// Parse footer
	footer := data[len(data)-BlockFooterSize:]
	checksum := binary.LittleEndian.Uint32(footer[0:])
	uncompressedSize := binary.LittleEndian.Uint32(footer[4:])
	compressedSize := binary.LittleEndian.Uint32(footer[8:])

	compressed := data[:len(data)-BlockFooterSize]

	if uint32(len(compressed)) != compressedSize {
		return nil, ErrCorruptedData
	}

	if verifyChecksum && crc32.ChecksumIEEE(compressed) != checksum {
		return nil, ErrChecksumMismatch
	}

	// Decompress using pooled decoder
	decoder := zstdDecoderPool.Get().(*zstd.Decoder)
	decompressed, err := decoder.DecodeAll(compressed, make([]byte, 0, uncompressedSize))
	zstdDecoderPool.Put(decoder)
	if err != nil {
		return nil, err
	}

	// Parse header
	if len(decompressed) < 3 {
		return nil, ErrCorruptedData
	}

	blockType := decompressed[0]
	numEntries := binary.LittleEndian.Uint16(decompressed[1:])

	// Parse entries with zero-copy (point directly into decompressed buffer)
	entries := make([]BlockEntry, numEntries)
	pos := 3

	for i := uint16(0); i < numEntries; i++ {
		if pos+4 > len(decompressed) {
			return nil, ErrCorruptedData
		}
		keyLen := int(binary.LittleEndian.Uint32(decompressed[pos:]))
		pos += 4

		if pos+keyLen+4 > len(decompressed) {
			return nil, ErrCorruptedData
		}
		// Zero-copy: slice into decompressed buffer
		entries[i].Key = decompressed[pos : pos+keyLen]
		pos += keyLen

		valLen := int(binary.LittleEndian.Uint32(decompressed[pos:]))
		pos += 4

		if pos+valLen > len(decompressed) {
			return nil, ErrCorruptedData
		}
		// Zero-copy: slice into decompressed buffer
		entries[i].Value = decompressed[pos : pos+valLen]
		pos += valLen
	}

	return &Block{
		Type:    blockType,
		Entries: entries,
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

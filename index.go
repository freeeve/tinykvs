package tinykvs

import (
	"encoding/binary"

	"github.com/bits-and-blooms/bloom/v3"
)

// IndexEntry represents a sparse index entry pointing to a data block.
type IndexEntry struct {
	Key         []byte // First key in the block (separator key)
	BlockOffset uint64 // File offset to the block
	BlockSize   uint32 // Size of the compressed block
}

// Index provides efficient key lookup within an SSTable.
type Index struct {
	Entries []IndexEntry
	MinKey  []byte
	MaxKey  []byte
	NumKeys uint64
}

// IndexBuilder builds the sparse index during SSTable creation.
type IndexBuilder struct {
	entries []IndexEntry
	minKey  []byte
	maxKey  []byte
	numKeys uint64
}

// NewIndexBuilder creates an index builder.
func NewIndexBuilder() *IndexBuilder {
	return &IndexBuilder{
		entries: make([]IndexEntry, 0, 256),
	}
}

// Add adds a block reference to the index.
func (ib *IndexBuilder) Add(firstKey []byte, lastKey []byte, offset uint64, size uint32, keysInBlock int) {
	if ib.minKey == nil {
		ib.minKey = make([]byte, len(firstKey))
		copy(ib.minKey, firstKey)
	}
	ib.maxKey = make([]byte, len(lastKey))
	copy(ib.maxKey, lastKey)
	ib.numKeys += uint64(keysInBlock)

	keyCopy := make([]byte, len(firstKey))
	copy(keyCopy, firstKey)

	ib.entries = append(ib.entries, IndexEntry{
		Key:         keyCopy,
		BlockOffset: offset,
		BlockSize:   size,
	})
}

// Build creates the final index.
func (ib *IndexBuilder) Build() *Index {
	return &Index{
		Entries: ib.entries,
		MinKey:  ib.minKey,
		MaxKey:  ib.maxKey,
		NumKeys: ib.numKeys,
	}
}

// Search finds the block that may contain the key.
// Returns the index of the block, or -1 if key is out of range.
func (idx *Index) Search(key []byte) int {
	if len(idx.Entries) == 0 {
		return -1
	}

	// Key is less than all keys in this SSTable
	if CompareKeys(key, idx.MinKey) < 0 {
		return -1
	}

	// Key is greater than all keys in this SSTable
	if CompareKeys(key, idx.MaxKey) > 0 {
		return -1
	}

	// Binary search for the last entry with Key <= target
	lo, hi := 0, len(idx.Entries)-1
	result := 0

	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := CompareKeys(idx.Entries[mid].Key, key)

		if cmp <= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return result
}

// Serialize encodes the index for storage.
func (idx *Index) Serialize() []byte {
	// Calculate size
	size := 8 + 4 + len(idx.MinKey) + 4 + len(idx.MaxKey) + 4 // numKeys + minKey + maxKey + numEntries
	for _, e := range idx.Entries {
		size += 4 + len(e.Key) + 8 + 4 // keyLen + key + offset + size
	}

	buf := make([]byte, 0, size)

	// NumKeys
	buf = binary.LittleEndian.AppendUint64(buf, idx.NumKeys)

	// MinKey
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(idx.MinKey)))
	buf = append(buf, idx.MinKey...)

	// MaxKey
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(idx.MaxKey)))
	buf = append(buf, idx.MaxKey...)

	// Entries
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(idx.Entries)))
	for _, e := range idx.Entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Key)))
		buf = append(buf, e.Key...)
		buf = binary.LittleEndian.AppendUint64(buf, e.BlockOffset)
		buf = binary.LittleEndian.AppendUint32(buf, e.BlockSize)
	}

	return buf
}

// DeserializeIndex recreates an index from bytes.
func DeserializeIndex(data []byte) (*Index, error) {
	if len(data) < 8 {
		return nil, ErrCorruptedData
	}

	idx := &Index{}
	pos := 0

	// NumKeys
	idx.NumKeys = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// MinKey
	if pos+4 > len(data) {
		return nil, ErrCorruptedData
	}
	minKeyLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	if pos+int(minKeyLen) > len(data) {
		return nil, ErrCorruptedData
	}
	idx.MinKey = make([]byte, minKeyLen)
	copy(idx.MinKey, data[pos:pos+int(minKeyLen)])
	pos += int(minKeyLen)

	// MaxKey
	if pos+4 > len(data) {
		return nil, ErrCorruptedData
	}
	maxKeyLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	if pos+int(maxKeyLen) > len(data) {
		return nil, ErrCorruptedData
	}
	idx.MaxKey = make([]byte, maxKeyLen)
	copy(idx.MaxKey, data[pos:pos+int(maxKeyLen)])
	pos += int(maxKeyLen)

	// Entries
	if pos+4 > len(data) {
		return nil, ErrCorruptedData
	}
	numEntries := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	idx.Entries = make([]IndexEntry, 0, numEntries)
	for i := uint32(0); i < numEntries; i++ {
		if pos+4 > len(data) {
			return nil, ErrCorruptedData
		}
		keyLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		if pos+int(keyLen)+12 > len(data) {
			return nil, ErrCorruptedData
		}

		key := make([]byte, keyLen)
		copy(key, data[pos:pos+int(keyLen)])
		pos += int(keyLen)

		offset := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		size := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		idx.Entries = append(idx.Entries, IndexEntry{
			Key:         key,
			BlockOffset: offset,
			BlockSize:   size,
		})
	}

	return idx, nil
}

// BloomFilter wraps a bloom filter with serialization.
type BloomFilter struct {
	filter *bloom.BloomFilter
}

// NewBloomFilter creates a bloom filter for the expected number of keys.
func NewBloomFilter(numKeys uint, fpRate float64) *BloomFilter {
	return &BloomFilter{
		filter: bloom.NewWithEstimates(numKeys, fpRate),
	}
}

// Add adds a key to the bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	bf.filter.Add(key)
}

// MayContain returns true if the key might be in the set.
// False positives are possible, but false negatives are not.
func (bf *BloomFilter) MayContain(key []byte) bool {
	return bf.filter.Test(key)
}

// Serialize encodes the bloom filter for storage.
func (bf *BloomFilter) Serialize() ([]byte, error) {
	return bf.filter.MarshalBinary()
}

// DeserializeBloomFilter recreates a bloom filter from bytes.
func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	filter := &bloom.BloomFilter{}
	if err := filter.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return &BloomFilter{filter: filter}, nil
}

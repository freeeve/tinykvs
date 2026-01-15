package tinykvs

import (
	"encoding/binary"
	"errors"
	"os"
)

// SSTable magic number and version
const (
	SSTableMagic   uint64 = 0x544B5653_00000001 // "TKVS" + version 1
	SSTableVersion uint32 = 1
)

// SSTableFooterSize is the fixed size of the footer in bytes.
const SSTableFooterSize = 64

// SSTableFooter is the fixed-size footer at the end of each SSTable.
type SSTableFooter struct {
	BloomOffset   uint64 // Offset to bloom filter block
	BloomSize     uint32 // Size of bloom filter block
	IndexOffset   uint64 // Offset to index block
	IndexSize     uint32 // Size of index block
	MetaOffset    uint64 // Offset to metadata block
	MetaSize      uint32 // Size of metadata block
	NumDataBlocks uint32 // Number of data blocks
	NumKeys       uint64 // Total number of keys
	FileSize      uint64 // Total file size for validation
	Magic         uint64 // Magic number for validation
}

// SSTableMeta contains metadata about the SSTable.
type SSTableMeta struct {
	Level         int
	MinSequence   uint64
	MaxSequence   uint64
	NumTombstones uint64
	CreatedAt     int64
}

// SSTable represents an on-disk sorted string table.
type SSTable struct {
	ID          uint32
	Path        string
	Level       int
	Footer      SSTableFooter
	Meta        SSTableMeta
	Index       *Index
	BloomFilter *BloomFilter

	file     *os.File
	fileSize int64
}

// OpenSSTable opens an existing SSTable file.
func OpenSSTable(id uint32, path string) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	fileSize := stat.Size()

	if fileSize < SSTableFooterSize {
		file.Close()
		return nil, ErrInvalidSSTable
	}

	// Read footer
	footerBuf := make([]byte, SSTableFooterSize)
	if _, err := file.ReadAt(footerBuf, fileSize-SSTableFooterSize); err != nil {
		file.Close()
		return nil, err
	}

	footer := parseFooter(footerBuf)

	// Validate magic
	if footer.Magic != SSTableMagic {
		file.Close()
		return nil, ErrInvalidSSTable
	}

	// Read and parse bloom filter (if present)
	var bloomFilter *BloomFilter
	if footer.BloomSize > 0 {
		bloomBuf := make([]byte, footer.BloomSize)
		if _, err := file.ReadAt(bloomBuf, int64(footer.BloomOffset)); err != nil {
			file.Close()
			return nil, err
		}
		bloomFilter, err = DeserializeBloomFilter(bloomBuf)
		if err != nil {
			file.Close()
			return nil, err
		}
	}

	// Read and parse index
	indexBuf := make([]byte, footer.IndexSize)
	if _, err := file.ReadAt(indexBuf, int64(footer.IndexOffset)); err != nil {
		file.Close()
		return nil, err
	}
	index, err := DeserializeIndex(indexBuf)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Read and parse metadata
	metaBuf := make([]byte, footer.MetaSize)
	if _, err := file.ReadAt(metaBuf, int64(footer.MetaOffset)); err != nil {
		file.Close()
		return nil, err
	}
	meta, err := deserializeMeta(metaBuf)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &SSTable{
		ID:          id,
		Path:        path,
		Level:       meta.Level,
		Footer:      footer,
		Meta:        meta,
		Index:       index,
		BloomFilter: bloomFilter,
		file:        file,
		fileSize:    fileSize,
	}, nil
}

// Get retrieves a value from the SSTable.
// Returns the entry, whether it was found, and any error.
func (sst *SSTable) Get(key []byte, cache *LRUCache, verifyChecksum bool) (Entry, bool, error) {
	// Check bloom filter first (if present)
	if sst.BloomFilter != nil && !sst.BloomFilter.MayContain(key) {
		return Entry{}, false, nil
	}

	// Find the block that may contain the key
	blockIdx := sst.Index.Search(key)
	if blockIdx < 0 {
		return Entry{}, false, nil
	}

	indexEntry := sst.Index.Entries[blockIdx]
	cacheKey := CacheKey{FileID: sst.ID, BlockOffset: indexEntry.BlockOffset}

	// Try cache first
	var block *Block
	if cache != nil {
		block, _ = cache.Get(cacheKey)
	}

	// Read from disk if not cached
	if block == nil {
		blockData := make([]byte, indexEntry.BlockSize)
		if _, err := sst.file.ReadAt(blockData, int64(indexEntry.BlockOffset)); err != nil {
			return Entry{}, false, err
		}

		var err error
		block, err = DecodeBlock(blockData, verifyChecksum)
		if err != nil {
			return Entry{}, false, err
		}

		// Add to cache
		if cache != nil {
			cache.Put(cacheKey, block)
		}
	}

	// Binary search within block
	idx := SearchBlock(block, key)
	if idx < 0 {
		return Entry{}, false, nil
	}

	// Decode the value
	value, _, err := DecodeValue(block.Entries[idx].Value)
	if err != nil {
		return Entry{}, false, err
	}

	return Entry{
		Key:   key,
		Value: value,
	}, true, nil
}

// MinKey returns the minimum key in this SSTable.
func (sst *SSTable) MinKey() []byte {
	return sst.Index.MinKey
}

// MaxKey returns the maximum key in this SSTable.
func (sst *SSTable) MaxKey() []byte {
	return sst.Index.MaxKey
}

// Close closes the SSTable file.
func (sst *SSTable) Close() error {
	return sst.file.Close()
}

// Size returns the file size in bytes.
func (sst *SSTable) Size() int64 {
	return sst.fileSize
}

// SSTableWriter builds an SSTable file.
type SSTableWriter struct {
	file *os.File
	path string
	opts Options
	id   uint32

	blockBuilder *BlockBuilder
	indexBuilder *IndexBuilder
	bloomFilter  *BloomFilter

	dataOffset    uint64
	numKeys       uint64
	lastKey       []byte
	minSeq        uint64
	maxSeq        uint64
	numTombstones uint64

	// Reusable buffer for encoding values
	encodeBuf []byte
}

// NewSSTableWriter creates a new SSTable writer.
func NewSSTableWriter(id uint32, path string, numKeys uint, opts Options) (*SSTableWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	w := &SSTableWriter{
		file:         file,
		path:         path,
		opts:         opts,
		id:           id,
		blockBuilder: NewBlockBuilder(opts.BlockSize),
		indexBuilder: NewIndexBuilder(),
		encodeBuf:    make([]byte, 0, 128),
	}

	// Only create bloom filter if not disabled
	if !opts.DisableBloomFilter {
		w.bloomFilter = NewBloomFilter(numKeys, opts.BloomFPRate)
	}

	return w, nil
}

// Add adds an entry to the SSTable.
func (w *SSTableWriter) Add(entry Entry) error {
	// Update bloom filter if enabled
	if w.bloomFilter != nil {
		w.bloomFilter.Add(entry.Key)
	}

	// Update statistics
	w.numKeys++
	if w.minSeq == 0 || entry.Sequence < w.minSeq {
		w.minSeq = entry.Sequence
	}
	if entry.Sequence > w.maxSeq {
		w.maxSeq = entry.Sequence
	}
	if entry.Value.IsTombstone() {
		w.numTombstones++
	}

	// Encode value using reusable buffer
	w.encodeBuf = AppendEncodedValue(w.encodeBuf[:0], entry.Value)

	// Try to add to current block
	if !w.blockBuilder.Add(entry.Key, w.encodeBuf) {
		// Block is full, flush it
		if err := w.flushDataBlock(); err != nil {
			return err
		}
		w.blockBuilder.Add(entry.Key, w.encodeBuf)
	}

	w.lastKey = entry.Key
	return nil
}

func (w *SSTableWriter) flushDataBlock() error {
	if w.blockBuilder.Count() == 0 {
		return nil
	}

	// Record first and last key for index
	entries := w.blockBuilder.Entries()
	firstKey := entries[0].Key
	lastKey := entries[len(entries)-1].Key
	keysInBlock := w.blockBuilder.Count()

	// Build and write block
	blockData, err := w.blockBuilder.Build(BlockTypeData, w.opts.CompressionLevel)
	if err != nil {
		return err
	}

	if _, err := w.file.Write(blockData); err != nil {
		return err
	}

	// Add to index
	w.indexBuilder.Add(firstKey, lastKey, w.dataOffset, uint32(len(blockData)), keysInBlock)

	w.dataOffset += uint64(len(blockData))
	w.blockBuilder.Reset()

	return nil
}

// Finish completes the SSTable and writes the footer.
func (w *SSTableWriter) Finish(level int) error {
	// Flush any remaining data
	if err := w.flushDataBlock(); err != nil {
		return err
	}

	// Write bloom filter (if enabled)
	bloomOffset := w.dataOffset
	var bloomData []byte
	if w.bloomFilter != nil {
		var err error
		bloomData, err = w.bloomFilter.Serialize()
		if err != nil {
			return err
		}
		if _, err := w.file.Write(bloomData); err != nil {
			return err
		}
	}

	// Write index
	indexOffset := bloomOffset + uint64(len(bloomData))
	index := w.indexBuilder.Build()
	indexData := index.Serialize()
	if _, err := w.file.Write(indexData); err != nil {
		return err
	}

	// Write metadata
	metaOffset := indexOffset + uint64(len(indexData))
	metaData := serializeMeta(SSTableMeta{
		Level:         level,
		MinSequence:   w.minSeq,
		MaxSequence:   w.maxSeq,
		NumTombstones: w.numTombstones,
	})
	if _, err := w.file.Write(metaData); err != nil {
		return err
	}

	// Write footer
	footer := SSTableFooter{
		BloomOffset:   bloomOffset,
		BloomSize:     uint32(len(bloomData)),
		IndexOffset:   indexOffset,
		IndexSize:     uint32(len(indexData)),
		MetaOffset:    metaOffset,
		MetaSize:      uint32(len(metaData)),
		NumDataBlocks: uint32(len(index.Entries)),
		NumKeys:       w.numKeys,
		FileSize:      metaOffset + uint64(len(metaData)) + SSTableFooterSize,
		Magic:         SSTableMagic,
	}

	footerData := serializeFooter(footer)
	if _, err := w.file.Write(footerData); err != nil {
		return err
	}

	return w.file.Sync()
}

// Close closes the writer without finishing.
func (w *SSTableWriter) Close() error {
	return w.file.Close()
}

// Abort closes and removes the incomplete SSTable file.
func (w *SSTableWriter) Abort() error {
	w.file.Close()
	return os.Remove(w.path)
}

// ID returns the SSTable ID.
func (w *SSTableWriter) ID() uint32 {
	return w.id
}

// Path returns the SSTable file path.
func (w *SSTableWriter) Path() string {
	return w.path
}

func parseFooter(data []byte) SSTableFooter {
	return SSTableFooter{
		BloomOffset:   binary.LittleEndian.Uint64(data[0:]),
		BloomSize:     binary.LittleEndian.Uint32(data[8:]),
		IndexOffset:   binary.LittleEndian.Uint64(data[12:]),
		IndexSize:     binary.LittleEndian.Uint32(data[20:]),
		MetaOffset:    binary.LittleEndian.Uint64(data[24:]),
		MetaSize:      binary.LittleEndian.Uint32(data[32:]),
		NumDataBlocks: binary.LittleEndian.Uint32(data[36:]),
		NumKeys:       binary.LittleEndian.Uint64(data[40:]),
		FileSize:      binary.LittleEndian.Uint64(data[48:]),
		Magic:         binary.LittleEndian.Uint64(data[56:]),
	}
}

func serializeFooter(f SSTableFooter) []byte {
	buf := make([]byte, SSTableFooterSize)
	binary.LittleEndian.PutUint64(buf[0:], f.BloomOffset)
	binary.LittleEndian.PutUint32(buf[8:], f.BloomSize)
	binary.LittleEndian.PutUint64(buf[12:], f.IndexOffset)
	binary.LittleEndian.PutUint32(buf[20:], f.IndexSize)
	binary.LittleEndian.PutUint64(buf[24:], f.MetaOffset)
	binary.LittleEndian.PutUint32(buf[32:], f.MetaSize)
	binary.LittleEndian.PutUint32(buf[36:], f.NumDataBlocks)
	binary.LittleEndian.PutUint64(buf[40:], f.NumKeys)
	binary.LittleEndian.PutUint64(buf[48:], f.FileSize)
	binary.LittleEndian.PutUint64(buf[56:], f.Magic)
	return buf
}

func serializeMeta(m SSTableMeta) []byte {
	// level(4) + minSeq(8) + maxSeq(8) + numTombstones(8) + createdAt(8)
	buf := make([]byte, 36)
	binary.LittleEndian.PutUint32(buf[0:], uint32(m.Level))
	binary.LittleEndian.PutUint64(buf[4:], m.MinSequence)
	binary.LittleEndian.PutUint64(buf[12:], m.MaxSequence)
	binary.LittleEndian.PutUint64(buf[20:], m.NumTombstones)
	binary.LittleEndian.PutUint64(buf[28:], uint64(m.CreatedAt))
	return buf
}

func deserializeMeta(data []byte) (SSTableMeta, error) {
	if len(data) < 36 {
		return SSTableMeta{}, ErrCorruptedData
	}
	return SSTableMeta{
		Level:         int(binary.LittleEndian.Uint32(data[0:])),
		MinSequence:   binary.LittleEndian.Uint64(data[4:]),
		MaxSequence:   binary.LittleEndian.Uint64(data[12:]),
		NumTombstones: binary.LittleEndian.Uint64(data[20:]),
		CreatedAt:     int64(binary.LittleEndian.Uint64(data[28:])),
	}, nil
}

// Errors
var (
	ErrInvalidSSTable = errors.New("invalid sstable format")
)

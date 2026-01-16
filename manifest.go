package tinykvs

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Manifest tracks all SSTables and their metadata.
// It's an append-only log that allows fast store recovery
// without reading every SSTable file.
type Manifest struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	tables   map[uint32]*TableMeta // id -> metadata
	maxID    uint32
	writer   *bufio.Writer
	sequence uint64 // Manifest sequence number
}

// TableMeta contains SSTable metadata stored in the manifest.
// This allows opening the store without reading each SSTable's index.
type TableMeta struct {
	ID       uint32
	Level    int
	MinKey   []byte
	MaxKey   []byte
	NumKeys  uint64
	FileSize int64

	// Offsets for lazy loading (relative to SSTable file)
	IndexOffset uint64
	IndexSize   uint32
	BloomOffset uint64
	BloomSize   uint32
}

// Manifest entry types
const (
	manifestEntryAdd      uint8 = 1
	manifestEntryDelete   uint8 = 2
	manifestEntrySnapshot uint8 = 3
)

// Manifest file magic and version
const (
	manifestMagic   uint32 = 0x4D414E49 // "MANI"
	manifestVersion uint32 = 1
)

var (
	ErrInvalidManifest  = errors.New("invalid manifest file")
	ErrManifestCorrupt  = errors.New("manifest file corrupted")
	ErrManifestNotFound = errors.New("manifest file not found")
)

// OpenManifest opens or creates a manifest file.
func OpenManifest(path string) (*Manifest, error) {
	m := &Manifest{
		path:   path,
		tables: make(map[uint32]*TableMeta),
	}

	// Try to open existing manifest
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		// Create new manifest
		return m.create()
	}
	if err != nil {
		return nil, err
	}

	m.file = file

	// Read and recover manifest
	if err := m.recover(); err != nil {
		file.Close()
		return nil, err
	}

	// Position at end for appending
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}

	m.writer = bufio.NewWriter(file)
	return m, nil
}

// create creates a new manifest file with header.
func (m *Manifest) create() (*Manifest, error) {
	file, err := os.OpenFile(m.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	m.file = file

	// Write header: magic + version
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:], manifestMagic)
	binary.LittleEndian.PutUint32(header[4:], manifestVersion)

	if _, err := file.Write(header); err != nil {
		file.Close()
		return nil, err
	}

	m.writer = bufio.NewWriter(file)
	return m, nil
}

// recover reads the manifest and rebuilds the table map.
func (m *Manifest) recover() error {
	// Read header
	header := make([]byte, 8)
	if _, err := m.file.ReadAt(header, 0); err != nil {
		return ErrInvalidManifest
	}

	magic := binary.LittleEndian.Uint32(header[0:])
	version := binary.LittleEndian.Uint32(header[4:])

	if magic != manifestMagic {
		return ErrInvalidManifest
	}
	if version != manifestVersion {
		return ErrInvalidManifest
	}

	// Read entries
	reader := bufio.NewReader(m.file)
	m.file.Seek(8, io.SeekStart) // Skip header

	for {
		entry, err := m.readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Truncate at corruption point (crash recovery)
			break
		}

		m.applyEntry(entry)
	}

	return nil
}

// manifestEntry is the in-memory representation of a manifest record.
type manifestEntry struct {
	Type     uint8
	Sequence uint64
	TableID  uint32
	Meta     *TableMeta   // For Add entries
	TableIDs []uint32     // For Snapshot entries
	Metas    []*TableMeta // For Snapshot entries
}

// readEntry reads a single manifest entry.
// Entry format: [length:4][crc:4][type:1][sequence:8][data...][crc:4]
func (m *Manifest) readEntry(r *bufio.Reader) (*manifestEntry, error) {
	// Read length
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(lengthBuf)

	if length == 0 || length > 10*1024*1024 { // Sanity check: max 10MB entry
		return nil, ErrManifestCorrupt
	}

	// Read entry data
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	// Verify checksum (last 4 bytes)
	if len(data) < 4 {
		return nil, ErrManifestCorrupt
	}
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	actualCRC := crc32.ChecksumIEEE(data[:len(data)-4])
	if storedCRC != actualCRC {
		return nil, ErrManifestCorrupt
	}

	// Parse entry
	return m.parseEntry(data[:len(data)-4])
}

// parseEntry parses entry data into a manifestEntry.
func (m *Manifest) parseEntry(data []byte) (*manifestEntry, error) {
	if len(data) < 9 { // type(1) + sequence(8)
		return nil, ErrManifestCorrupt
	}

	entry := &manifestEntry{
		Type:     data[0],
		Sequence: binary.LittleEndian.Uint64(data[1:9]),
	}

	pos := 9

	switch entry.Type {
	case manifestEntryAdd:
		meta, n, err := m.parseTableMeta(data[pos:])
		if err != nil {
			return nil, err
		}
		entry.Meta = meta
		entry.TableID = meta.ID
		pos += n

	case manifestEntryDelete:
		if pos+4 > len(data) {
			return nil, ErrManifestCorrupt
		}
		entry.TableID = binary.LittleEndian.Uint32(data[pos:])

	case manifestEntrySnapshot:
		if pos+4 > len(data) {
			return nil, ErrManifestCorrupt
		}
		numTables := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		entry.Metas = make([]*TableMeta, 0, numTables)
		for i := uint32(0); i < numTables; i++ {
			meta, n, err := m.parseTableMeta(data[pos:])
			if err != nil {
				return nil, err
			}
			entry.Metas = append(entry.Metas, meta)
			pos += n
		}
	}

	return entry, nil
}

// parseTableMeta parses TableMeta from bytes.
func (m *Manifest) parseTableMeta(data []byte) (*TableMeta, int, error) {
	if len(data) < 48 { // Minimum size
		return nil, 0, ErrManifestCorrupt
	}

	pos := 0
	meta := &TableMeta{}

	meta.ID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	meta.Level = int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// MinKey
	minKeyLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	if pos+int(minKeyLen) > len(data) {
		return nil, 0, ErrManifestCorrupt
	}
	meta.MinKey = make([]byte, minKeyLen)
	copy(meta.MinKey, data[pos:pos+int(minKeyLen)])
	pos += int(minKeyLen)

	// MaxKey
	if pos+4 > len(data) {
		return nil, 0, ErrManifestCorrupt
	}
	maxKeyLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	if pos+int(maxKeyLen) > len(data) {
		return nil, 0, ErrManifestCorrupt
	}
	meta.MaxKey = make([]byte, maxKeyLen)
	copy(meta.MaxKey, data[pos:pos+int(maxKeyLen)])
	pos += int(maxKeyLen)

	// Fixed fields
	if pos+40 > len(data) {
		return nil, 0, ErrManifestCorrupt
	}
	meta.NumKeys = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.FileSize = int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8
	meta.IndexOffset = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.IndexSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	meta.BloomOffset = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.BloomSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	return meta, pos, nil
}

// applyEntry applies a manifest entry to the in-memory state.
func (m *Manifest) applyEntry(entry *manifestEntry) {
	if entry.Sequence > m.sequence {
		m.sequence = entry.Sequence
	}

	switch entry.Type {
	case manifestEntryAdd:
		m.tables[entry.Meta.ID] = entry.Meta
		if entry.Meta.ID > m.maxID {
			m.maxID = entry.Meta.ID
		}

	case manifestEntryDelete:
		delete(m.tables, entry.TableID)

	case manifestEntrySnapshot:
		// Replace all tables with snapshot
		m.tables = make(map[uint32]*TableMeta)
		for _, meta := range entry.Metas {
			m.tables[meta.ID] = meta
			if meta.ID > m.maxID {
				m.maxID = meta.ID
			}
		}
	}
}

// AddTable records a new SSTable in the manifest.
func (m *Manifest) AddTable(meta *TableMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sequence++
	entry := &manifestEntry{
		Type:     manifestEntryAdd,
		Sequence: m.sequence,
		Meta:     meta,
	}

	if err := m.writeEntry(entry); err != nil {
		return err
	}

	m.tables[meta.ID] = meta
	if meta.ID > m.maxID {
		m.maxID = meta.ID
	}

	return m.writer.Flush()
}

// DeleteTable records removal of an SSTable.
func (m *Manifest) DeleteTable(id uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sequence++
	entry := &manifestEntry{
		Type:     manifestEntryDelete,
		Sequence: m.sequence,
		TableID:  id,
	}

	if err := m.writeEntry(entry); err != nil {
		return err
	}

	delete(m.tables, id)

	return m.writer.Flush()
}

// DeleteTables records removal of multiple SSTables atomically.
func (m *Manifest) DeleteTables(ids []uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, id := range ids {
		m.sequence++
		entry := &manifestEntry{
			Type:     manifestEntryDelete,
			Sequence: m.sequence,
			TableID:  id,
		}

		if err := m.writeEntry(entry); err != nil {
			return err
		}

		delete(m.tables, id)
	}

	return m.writer.Flush()
}

// writeEntry writes a manifest entry to the file.
func (m *Manifest) writeEntry(entry *manifestEntry) error {
	data := m.serializeEntry(entry)

	// Write length + data
	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, uint32(len(data)))

	if _, err := m.writer.Write(lengthBuf); err != nil {
		return err
	}
	if _, err := m.writer.Write(data); err != nil {
		return err
	}

	return nil
}

// serializeEntry serializes a manifest entry to bytes.
func (m *Manifest) serializeEntry(entry *manifestEntry) []byte {
	var buf []byte

	// Type + Sequence
	buf = append(buf, entry.Type)
	seqBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBuf, entry.Sequence)
	buf = append(buf, seqBuf...)

	switch entry.Type {
	case manifestEntryAdd:
		buf = append(buf, m.serializeTableMeta(entry.Meta)...)

	case manifestEntryDelete:
		idBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(idBuf, entry.TableID)
		buf = append(buf, idBuf...)

	case manifestEntrySnapshot:
		numBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(numBuf, uint32(len(entry.Metas)))
		buf = append(buf, numBuf...)
		for _, meta := range entry.Metas {
			buf = append(buf, m.serializeTableMeta(meta)...)
		}
	}

	// Append CRC
	crc := crc32.ChecksumIEEE(buf)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	buf = append(buf, crcBuf...)

	return buf
}

// serializeTableMeta serializes TableMeta to bytes.
func (m *Manifest) serializeTableMeta(meta *TableMeta) []byte {
	size := 4 + 4 + 4 + len(meta.MinKey) + 4 + len(meta.MaxKey) + 8 + 8 + 8 + 4 + 8 + 4
	buf := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint32(buf[pos:], meta.ID)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:], uint32(meta.Level))
	pos += 4

	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(meta.MinKey)))
	pos += 4
	copy(buf[pos:], meta.MinKey)
	pos += len(meta.MinKey)

	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(meta.MaxKey)))
	pos += 4
	copy(buf[pos:], meta.MaxKey)
	pos += len(meta.MaxKey)

	binary.LittleEndian.PutUint64(buf[pos:], meta.NumKeys)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:], uint64(meta.FileSize))
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:], meta.IndexOffset)
	pos += 8
	binary.LittleEndian.PutUint32(buf[pos:], meta.IndexSize)
	pos += 4
	binary.LittleEndian.PutUint64(buf[pos:], meta.BloomOffset)
	pos += 8
	binary.LittleEndian.PutUint32(buf[pos:], meta.BloomSize)

	return buf
}

// Tables returns all tracked tables.
func (m *Manifest) Tables() map[uint32]*TableMeta {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[uint32]*TableMeta, len(m.tables))
	for id, meta := range m.tables {
		result[id] = meta
	}
	return result
}

// MaxID returns the highest SSTable ID seen.
func (m *Manifest) MaxID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxID
}

// Close closes the manifest file.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writer != nil {
		m.writer.Flush()
	}
	if m.file != nil {
		return m.file.Close()
	}
	return nil
}

// Sync flushes and syncs the manifest to disk.
func (m *Manifest) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			return err
		}
	}
	if m.file != nil {
		return m.file.Sync()
	}
	return nil
}

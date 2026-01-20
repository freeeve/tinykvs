package tinykvs

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// wal record types for fragmentation
const (
	walRecordFull   uint8 = 1 // Complete record in one block
	walRecordFirst  uint8 = 2 // First fragment
	walRecordMiddle uint8 = 3 // Middle fragment
	walRecordLast   uint8 = 4 // Last fragment
)

// wal operation types
const (
	opPut    uint8 = 1
	opDelete uint8 = 2
)

const (
	walBlockSize  = 32 * 1024 // 32KB blocks
	walHeaderSize = 7         // CRC(4) + Length(2) + Type(1)
)

// walEntry represents a logged operation.
type walEntry struct {
	Operation uint8
	Key       []byte
	Value     Value // Empty for delete
	Sequence  uint64
}

// wal implements write-ahead logging for durability.
type wal struct {
	file     *os.File
	path     string
	syncMode WALSyncMode

	// Current block buffer
	block    []byte
	blockPos int

	// Reusable encode buffer
	encodeBuf []byte

	mu sync.Mutex
}

// openWAL opens or creates a wal file.
func openWAL(path string, syncMode WALSyncMode) (*wal, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &wal{
		file:      file,
		path:      path,
		syncMode:  syncMode,
		block:     make([]byte, walBlockSize),
		blockPos:  0,
		encodeBuf: make([]byte, 0, 512),
	}, nil
}

// Append writes an entry to the wal.
func (w *wal) Append(entry walEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendUnlocked(entry)
}

// appendUnlocked is the internal unlocked version of Append.
func (w *wal) appendUnlocked(entry walEntry) error {
	// Encode entry
	data := w.encodeEntry(entry)

	// Write potentially across multiple blocks
	remaining := data
	isFirst := true

	for len(remaining) > 0 {
		availableInBlock := walBlockSize - w.blockPos - walHeaderSize

		if availableInBlock <= 0 {
			// Flush current block
			if err := w.flushBlock(); err != nil {
				return err
			}
			availableInBlock = walBlockSize - walHeaderSize
		}

		var recordType uint8
		var fragment []byte

		if len(remaining) <= availableInBlock {
			// Fits in current block
			fragment = remaining
			remaining = nil
			if isFirst {
				recordType = walRecordFull
			} else {
				recordType = walRecordLast
			}
		} else {
			// Need to fragment
			fragment = remaining[:availableInBlock]
			remaining = remaining[availableInBlock:]
			if isFirst {
				recordType = walRecordFirst
			} else {
				recordType = walRecordMiddle
			}
		}

		isFirst = false

		// Write header: CRC(4) + Length(2) + Type(1)
		checksum := crc32.ChecksumIEEE(fragment)
		binary.LittleEndian.PutUint32(w.block[w.blockPos:], checksum)
		binary.LittleEndian.PutUint16(w.block[w.blockPos+4:], uint16(len(fragment)))
		w.block[w.blockPos+6] = recordType
		copy(w.block[w.blockPos+walHeaderSize:], fragment)

		w.blockPos += walHeaderSize + len(fragment)
	}

	if w.syncMode == WALSyncPerWrite {
		return w.sync()
	}
	return nil
}

// Sync forces wal data to disk.
func (w *wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

func (w *wal) sync() error {
	if w.blockPos > 0 {
		if _, err := w.file.Write(w.block[:w.blockPos]); err != nil {
			return err
		}
		w.blockPos = 0
	}
	return w.file.Sync()
}

func (w *wal) flushBlock() error {
	if w.blockPos > 0 {
		// Pad with zeros
		for i := w.blockPos; i < walBlockSize; i++ {
			w.block[i] = 0
		}
		if _, err := w.file.Write(w.block); err != nil {
			return err
		}
		w.blockPos = 0
	}
	return nil
}

// Recover reads all entries from the wal for crash recovery.
func (w *wal) Recover() ([]walEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	entries, err := w.recoverUnlocked()
	if err != nil {
		return nil, err
	}

	// Seek to end for appending
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return entries, nil
}

// walRecoveryState holds state during WAL recovery.
type walRecoveryState struct {
	entries        []walEntry
	fragmentBuffer []byte
}

// recoverUnlocked is the internal unlocked version of Recover.
// Caller must hold w.mu and position the file at the start.
func (w *wal) recoverUnlocked() ([]walEntry, error) {
	state := &walRecoveryState{}
	block := make([]byte, walBlockSize)

	for {
		n, done, err := w.readWALBlock(block)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}

		if !w.parseWALBlock(block[:n], state) {
			break // Corruption detected, stop recovery
		}
	}

	return state.entries, nil
}

// readWALBlock reads the next block from the WAL file.
// Returns bytes read, whether EOF was reached, and any error.
func (w *wal) readWALBlock(block []byte) (int, bool, error) {
	n, err := io.ReadFull(w.file, block)
	if err == io.EOF || n == 0 {
		return 0, true, nil
	}
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, false, err
	}
	return n, false, nil
}

// parseWALBlock parses all records from a WAL block.
// Returns false if corruption is detected.
func (w *wal) parseWALBlock(block []byte, state *walRecoveryState) bool {
	pos := 0
	for pos+walHeaderSize <= len(block) {
		record, size, ok := w.parseWALRecord(block[pos:])
		if !ok {
			return true // End of valid records in block
		}

		if !w.processWALRecord(record, state) {
			return false // Checksum mismatch
		}

		pos += size
	}
	return true
}

// walRecord represents a parsed WAL record header and data.
type walRecord struct {
	checksum   uint32
	recordType uint8
	data       []byte
}

// parseWALRecord extracts a single record from block data.
// Returns the record, total size consumed, and whether parsing succeeded.
func (w *wal) parseWALRecord(block []byte) (walRecord, int, bool) {
	if len(block) < walHeaderSize {
		return walRecord{}, 0, false
	}

	checksum := binary.LittleEndian.Uint32(block)
	length := binary.LittleEndian.Uint16(block[4:])
	recordType := block[6]

	if length == 0 {
		return walRecord{}, 0, false // End of records marker
	}

	totalSize := walHeaderSize + int(length)
	if totalSize > len(block) {
		return walRecord{}, 0, false // Incomplete record
	}

	return walRecord{
		checksum:   checksum,
		recordType: recordType,
		data:       block[walHeaderSize:totalSize],
	}, totalSize, true
}

// processWALRecord handles a single WAL record based on its type.
// Returns false if checksum verification fails.
func (w *wal) processWALRecord(record walRecord, state *walRecoveryState) bool {
	if crc32.ChecksumIEEE(record.data) != record.checksum {
		return false
	}

	switch record.recordType {
	case walRecordFull:
		w.appendDecodedEntry(record.data, state)
	case walRecordFirst:
		state.fragmentBuffer = make([]byte, len(record.data))
		copy(state.fragmentBuffer, record.data)
	case walRecordMiddle:
		state.fragmentBuffer = append(state.fragmentBuffer, record.data...)
	case walRecordLast:
		state.fragmentBuffer = append(state.fragmentBuffer, record.data...)
		w.appendDecodedEntry(state.fragmentBuffer, state)
		state.fragmentBuffer = nil
	}
	return true
}

// appendDecodedEntry decodes and appends a WAL entry to the recovery state.
func (w *wal) appendDecodedEntry(data []byte, state *walRecoveryState) {
	entry, err := w.decodeEntry(data)
	if err == nil {
		state.entries = append(state.entries, entry)
	}
}

// Close closes the wal file.
func (w *wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// Truncate clears the wal after a successful flush.
func (w *wal) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekStart)
	w.blockPos = 0
	return err
}

// TruncateBefore removes wal entries with sequence < minSeq.
// This is used for partial wal cleanup when some memtables are still pending flush.
func (w *wal) TruncateBefore(minSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Read all entries
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	entries, err := w.recoverUnlocked()
	if err != nil {
		return err
	}

	// Filter entries to keep
	var kept []walEntry
	for _, e := range entries {
		if e.Sequence >= minSeq {
			kept = append(kept, e)
		}
	}

	// Truncate and rewrite
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.blockPos = 0

	// Rewrite kept entries
	for _, e := range kept {
		if err := w.appendUnlocked(e); err != nil {
			return err
		}
	}

	// Sync to ensure durability
	return w.file.Sync()
}

// encodeEntry serializes a wal entry into the reusable buffer.
func (w *wal) encodeEntry(entry walEntry) []byte {
	// Format: op(1) + seq(8) + key_len(4) + key + value
	size := 1 + 8 + 4 + len(entry.Key) + entry.Value.EncodedSize()

	// Reuse buffer, grow if needed
	if cap(w.encodeBuf) < size {
		w.encodeBuf = make([]byte, 0, size*2)
	}
	buf := w.encodeBuf[:0]

	buf = append(buf, entry.Operation)
	buf = binary.LittleEndian.AppendUint64(buf, entry.Sequence)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Key)))
	buf = append(buf, entry.Key...)
	buf = appendEncodedValue(buf, entry.Value)

	w.encodeBuf = buf
	return buf
}

// decodeEntry deserializes a wal entry.
func (w *wal) decodeEntry(data []byte) (walEntry, error) {
	if len(data) < 13 { // op(1) + seq(8) + key_len(4)
		return walEntry{}, ErrCorruptedData
	}

	entry := walEntry{
		Operation: data[0],
		Sequence:  binary.LittleEndian.Uint64(data[1:]),
	}

	keyLen := binary.LittleEndian.Uint32(data[9:])
	if len(data) < 13+int(keyLen) {
		return walEntry{}, ErrCorruptedData
	}

	entry.Key = make([]byte, keyLen)
	copy(entry.Key, data[13:13+keyLen])

	if entry.Operation == opPut {
		value, _, err := DecodeValue(data[13+keyLen:])
		if err != nil {
			return walEntry{}, err
		}
		entry.Value = value
	}

	return entry, nil
}

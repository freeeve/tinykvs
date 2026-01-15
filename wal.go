package tinykvs

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WAL record types for fragmentation
const (
	walRecordFull   uint8 = 1 // Complete record in one block
	walRecordFirst  uint8 = 2 // First fragment
	walRecordMiddle uint8 = 3 // Middle fragment
	walRecordLast   uint8 = 4 // Last fragment
)

// WAL operation types
const (
	OpPut    uint8 = 1
	OpDelete uint8 = 2
)

const (
	walBlockSize  = 32 * 1024 // 32KB blocks
	walHeaderSize = 7         // CRC(4) + Length(2) + Type(1)
)

// WALEntry represents a logged operation.
type WALEntry struct {
	Operation uint8
	Key       []byte
	Value     Value // Empty for delete
	Sequence  uint64
}

// WAL implements write-ahead logging for durability.
type WAL struct {
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

// OpenWAL opens or creates a WAL file.
func OpenWAL(path string, syncMode WALSyncMode) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:      file,
		path:      path,
		syncMode:  syncMode,
		block:     make([]byte, walBlockSize),
		blockPos:  0,
		encodeBuf: make([]byte, 0, 512),
	}, nil
}

// Append writes an entry to the WAL.
func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

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

// Sync forces WAL data to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

func (w *WAL) sync() error {
	if w.blockPos > 0 {
		if _, err := w.file.Write(w.block[:w.blockPos]); err != nil {
			return err
		}
		w.blockPos = 0
	}
	return w.file.Sync()
}

func (w *WAL) flushBlock() error {
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

// Recover reads all entries from the WAL for crash recovery.
func (w *WAL) Recover() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []WALEntry
	var fragmentBuffer []byte

	block := make([]byte, walBlockSize)

	for {
		n, err := io.ReadFull(w.file, block)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		if n == 0 {
			break
		}

		pos := 0
		for pos+walHeaderSize <= n {
			checksum := binary.LittleEndian.Uint32(block[pos:])
			length := binary.LittleEndian.Uint16(block[pos+4:])
			recordType := block[pos+6]

			if length == 0 {
				break // End of records in block
			}

			if pos+walHeaderSize+int(length) > n {
				break // Incomplete record
			}

			data := block[pos+walHeaderSize : pos+walHeaderSize+int(length)]

			// Verify checksum
			if crc32.ChecksumIEEE(data) != checksum {
				// Corrupted record, stop recovery here
				break
			}

			switch recordType {
			case walRecordFull:
				entry, err := w.decodeEntry(data)
				if err == nil {
					entries = append(entries, entry)
				}
			case walRecordFirst:
				fragmentBuffer = make([]byte, len(data))
				copy(fragmentBuffer, data)
			case walRecordMiddle:
				fragmentBuffer = append(fragmentBuffer, data...)
			case walRecordLast:
				fragmentBuffer = append(fragmentBuffer, data...)
				entry, err := w.decodeEntry(fragmentBuffer)
				if err == nil {
					entries = append(entries, entry)
				}
				fragmentBuffer = nil
			}

			pos += walHeaderSize + int(length)
		}
	}

	// Seek to end for appending
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return entries, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// Truncate clears the WAL after a successful flush.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekStart)
	w.blockPos = 0
	return err
}

// encodeEntry serializes a WAL entry into the reusable buffer.
func (w *WAL) encodeEntry(entry WALEntry) []byte {
	valueBytes := EncodeValue(entry.Value)

	// Format: op(1) + seq(8) + key_len(4) + key + value
	size := 1 + 8 + 4 + len(entry.Key) + len(valueBytes)

	// Reuse buffer, grow if needed
	if cap(w.encodeBuf) < size {
		w.encodeBuf = make([]byte, 0, size*2)
	}
	buf := w.encodeBuf[:0]

	buf = append(buf, entry.Operation)
	buf = binary.LittleEndian.AppendUint64(buf, entry.Sequence)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Key)))
	buf = append(buf, entry.Key...)
	buf = append(buf, valueBytes...)

	w.encodeBuf = buf
	return buf
}

// decodeEntry deserializes a WAL entry.
func (w *WAL) decodeEntry(data []byte) (WALEntry, error) {
	if len(data) < 13 { // op(1) + seq(8) + key_len(4)
		return WALEntry{}, ErrCorruptedData
	}

	entry := WALEntry{
		Operation: data[0],
		Sequence:  binary.LittleEndian.Uint64(data[1:]),
	}

	keyLen := binary.LittleEndian.Uint32(data[9:])
	if len(data) < 13+int(keyLen) {
		return WALEntry{}, ErrCorruptedData
	}

	entry.Key = make([]byte, keyLen)
	copy(entry.Key, data[13:13+keyLen])

	if entry.Operation == OpPut {
		value, _, err := DecodeValue(data[13+keyLen:])
		if err != nil {
			return WALEntry{}, err
		}
		entry.Value = value
	}

	return entry, nil
}

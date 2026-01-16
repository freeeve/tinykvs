package tinykvs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

// ValueType represents the type of stored value.
type ValueType uint8

const (
	ValueTypeInt64 ValueType = iota + 1
	ValueTypeFloat64
	ValueTypeBool
	ValueTypeString
	ValueTypeBytes
	ValueTypeTombstone // Special type for deletions
)

// inlineThreshold determines when to store data inline vs pointer.
// Values smaller than this are stored directly in the record.
const inlineThreshold = 64

// DataPointer references variable-length data stored in data blocks.
// Used for strings/bytes larger than inlineThreshold.
type dataPointer struct {
	FileID      uint32 // SSTable file identifier
	BlockOffset uint32 // Offset to the data block within file
	DataOffset  uint16 // Offset within the decompressed block
	Length      uint32 // Total length of the data
}

// dataPointerSize is the serialized size of a DataPointer.
const dataPointerSize = 14 // 4 + 4 + 2 + 4

// Value represents a typed value in the store.
type Value struct {
	Type ValueType

	// Inline storage for primitives
	Int64   int64
	Float64 float64
	Bool    bool

	// For strings/bytes - either inline data or pointer to block
	Bytes   []byte       // Used for inline storage (small values)
	Pointer *dataPointer // Used for large values stored in data blocks
}

// Entry represents a key-value pair with metadata.
type Entry struct {
	Key      []byte
	Value    Value
	Sequence uint64 // Monotonic sequence number for ordering
}

// Common errors
var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrInvalidValue     = errors.New("invalid value encoding")
	ErrCorruptedData    = errors.New("corrupted data")
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

// CompareKeys performs lexicographic comparison of two keys.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Uses bytes.Compare which is assembly-optimized on most platforms.
func CompareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}

// EncodedSize returns the serialized size of a value.
func (v *Value) EncodedSize() int {
	switch v.Type {
	case ValueTypeInt64:
		return 1 + 8 // type + int64
	case ValueTypeFloat64:
		return 1 + 8 // type + float64
	case ValueTypeBool:
		return 1 + 1 // type + bool
	case ValueTypeString, ValueTypeBytes:
		if v.Pointer != nil {
			return 1 + 1 + dataPointerSize // type + flag + pointer
		}
		return 1 + 1 + 4 + len(v.Bytes) // type + flag + length + data
	case ValueTypeTombstone:
		return 1 // type only
	default:
		return 0
	}
}

// EncodeValue serializes a value to bytes.
func EncodeValue(v Value) []byte {
	return appendEncodedValue(make([]byte, 0, v.EncodedSize()), v)
}

// appendEncodedValue appends the encoded value to dst and returns the result.
// This avoids allocation when dst has sufficient capacity.
func appendEncodedValue(dst []byte, v Value) []byte {
	dst = append(dst, byte(v.Type))

	switch v.Type {
	case ValueTypeInt64:
		dst = binary.LittleEndian.AppendUint64(dst, uint64(v.Int64))
	case ValueTypeFloat64:
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(v.Float64))
	case ValueTypeBool:
		if v.Bool {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
	case ValueTypeString, ValueTypeBytes:
		if v.Pointer != nil {
			dst = append(dst, 1) // Flag: 1 = pointer
			dst = binary.LittleEndian.AppendUint32(dst, v.Pointer.FileID)
			dst = binary.LittleEndian.AppendUint32(dst, v.Pointer.BlockOffset)
			dst = binary.LittleEndian.AppendUint16(dst, v.Pointer.DataOffset)
			dst = binary.LittleEndian.AppendUint32(dst, v.Pointer.Length)
		} else {
			dst = append(dst, 0) // Flag: 0 = inline data
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(v.Bytes)))
			dst = append(dst, v.Bytes...)
		}
	case ValueTypeTombstone:
		// No additional data
	}

	return dst
}

// DecodeValueZeroCopy deserializes a value without copying byte data.
// The returned Value's Bytes field points into the input data slice.
// Caller must ensure data outlives the returned Value.
// This is faster but the Value is only valid while data is valid.
func DecodeValueZeroCopy(data []byte) (Value, int, error) {
	if len(data) < 1 {
		return Value{}, 0, ErrInvalidValue
	}

	v := Value{Type: ValueType(data[0])}
	var consumed int

	switch v.Type {
	case ValueTypeInt64:
		if len(data) < 9 {
			return Value{}, 0, ErrInvalidValue
		}
		v.Int64 = int64(binary.LittleEndian.Uint64(data[1:]))
		consumed = 9

	case ValueTypeFloat64:
		if len(data) < 9 {
			return Value{}, 0, ErrInvalidValue
		}
		v.Float64 = math.Float64frombits(binary.LittleEndian.Uint64(data[1:]))
		consumed = 9

	case ValueTypeBool:
		if len(data) < 2 {
			return Value{}, 0, ErrInvalidValue
		}
		v.Bool = data[1] != 0
		consumed = 2

	case ValueTypeString, ValueTypeBytes:
		if len(data) < 2 {
			return Value{}, 0, ErrInvalidValue
		}
		isPointer := data[1] == 1
		if isPointer {
			if len(data) < 2+dataPointerSize {
				return Value{}, 0, ErrInvalidValue
			}
			// Embed pointer data directly to avoid allocation
			v.Pointer = &dataPointer{
				FileID:      binary.LittleEndian.Uint32(data[2:]),
				BlockOffset: binary.LittleEndian.Uint32(data[6:]),
				DataOffset:  binary.LittleEndian.Uint16(data[10:]),
				Length:      binary.LittleEndian.Uint32(data[12:]),
			}
			consumed = 2 + dataPointerSize
		} else {
			if len(data) < 6 {
				return Value{}, 0, ErrInvalidValue
			}
			length := binary.LittleEndian.Uint32(data[2:])
			if len(data) < 6+int(length) {
				return Value{}, 0, ErrInvalidValue
			}
			// Zero-copy: slice into source buffer
			v.Bytes = data[6 : 6+length]
			consumed = 6 + int(length)
		}

	case ValueTypeTombstone:
		consumed = 1

	default:
		return Value{}, 0, ErrInvalidValue
	}

	return v, consumed, nil
}

// DecodeValue deserializes a value from bytes.
// Returns the value and number of bytes consumed.
func DecodeValue(data []byte) (Value, int, error) {
	if len(data) < 1 {
		return Value{}, 0, ErrInvalidValue
	}

	v := Value{Type: ValueType(data[0])}
	var consumed int

	switch v.Type {
	case ValueTypeInt64:
		if len(data) < 9 {
			return Value{}, 0, ErrInvalidValue
		}
		v.Int64 = int64(binary.LittleEndian.Uint64(data[1:]))
		consumed = 9

	case ValueTypeFloat64:
		if len(data) < 9 {
			return Value{}, 0, ErrInvalidValue
		}
		v.Float64 = math.Float64frombits(binary.LittleEndian.Uint64(data[1:]))
		consumed = 9

	case ValueTypeBool:
		if len(data) < 2 {
			return Value{}, 0, ErrInvalidValue
		}
		v.Bool = data[1] != 0
		consumed = 2

	case ValueTypeString, ValueTypeBytes:
		if len(data) < 2 {
			return Value{}, 0, ErrInvalidValue
		}
		isPointer := data[1] == 1
		if isPointer {
			// Pointer to data block
			if len(data) < 2+dataPointerSize {
				return Value{}, 0, ErrInvalidValue
			}
			v.Pointer = &dataPointer{
				FileID:      binary.LittleEndian.Uint32(data[2:]),
				BlockOffset: binary.LittleEndian.Uint32(data[6:]),
				DataOffset:  binary.LittleEndian.Uint16(data[10:]),
				Length:      binary.LittleEndian.Uint32(data[12:]),
			}
			consumed = 2 + dataPointerSize
		} else {
			// Inline data
			if len(data) < 6 {
				return Value{}, 0, ErrInvalidValue
			}
			length := binary.LittleEndian.Uint32(data[2:])
			if len(data) < 6+int(length) {
				return Value{}, 0, ErrInvalidValue
			}
			v.Bytes = make([]byte, length)
			copy(v.Bytes, data[6:6+length])
			consumed = 6 + int(length)
		}

	case ValueTypeTombstone:
		// No additional data
		consumed = 1

	default:
		return Value{}, 0, ErrInvalidValue
	}

	return v, consumed, nil
}

// EncodeEntry serializes an entry (key + value + sequence) to bytes.
func EncodeEntry(e Entry) []byte {
	valueBytes := EncodeValue(e.Value)
	// Format: key_len(4) + key + seq(8) + value
	buf := make([]byte, 0, 4+len(e.Key)+8+len(valueBytes))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Key)))
	buf = append(buf, e.Key...)
	buf = binary.LittleEndian.AppendUint64(buf, e.Sequence)
	buf = append(buf, valueBytes...)
	return buf
}

// DecodeEntry deserializes an entry from bytes.
func DecodeEntry(data []byte) (Entry, int, error) {
	if len(data) < 4 {
		return Entry{}, 0, ErrInvalidValue
	}

	keyLen := binary.LittleEndian.Uint32(data[0:])
	if len(data) < 4+int(keyLen)+8 {
		return Entry{}, 0, ErrInvalidValue
	}

	key := make([]byte, keyLen)
	copy(key, data[4:4+keyLen])

	seq := binary.LittleEndian.Uint64(data[4+keyLen:])

	value, valueLen, err := DecodeValue(data[4+int(keyLen)+8:])
	if err != nil {
		return Entry{}, 0, err
	}

	return Entry{
		Key:      key,
		Value:    value,
		Sequence: seq,
	}, 4 + int(keyLen) + 8 + valueLen, nil
}

// Int64Value creates a Value containing an int64.
func Int64Value(v int64) Value {
	return Value{Type: ValueTypeInt64, Int64: v}
}

// Float64Value creates a Value containing a float64.
func Float64Value(v float64) Value {
	return Value{Type: ValueTypeFloat64, Float64: v}
}

// BoolValue creates a Value containing a bool.
func BoolValue(v bool) Value {
	return Value{Type: ValueTypeBool, Bool: v}
}

// StringValue creates a Value containing a string.
func StringValue(v string) Value {
	return Value{Type: ValueTypeString, Bytes: []byte(v)}
}

// BytesValue creates a Value containing bytes.
func BytesValue(v []byte) Value {
	return Value{Type: ValueTypeBytes, Bytes: v}
}

// TombstoneValue creates a tombstone Value for deletions.
func TombstoneValue() Value {
	return Value{Type: ValueTypeTombstone}
}

// IsTombstone returns true if this value represents a deletion.
func (v Value) IsTombstone() bool {
	return v.Type == ValueTypeTombstone
}

// String returns the string representation for string values.
func (v Value) String() string {
	if v.Type == ValueTypeString && v.Bytes != nil {
		return string(v.Bytes)
	}
	return ""
}

// GetBytes returns the byte slice for string/bytes values.
func (v Value) GetBytes() []byte {
	if (v.Type == ValueTypeString || v.Type == ValueTypeBytes) && v.Bytes != nil {
		return v.Bytes
	}
	return nil
}

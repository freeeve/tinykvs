package tinykvs

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"

	"github.com/freeeve/msgpck"
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
	ValueTypeRecord    // Structured record with named fields
	ValueTypeMsgpack   // Raw msgpack bytes for efficient struct storage
)

// inlineThreshold determines when to store data inline vs pointer.
// Values smaller than this are stored directly in the record.
const inlineThreshold = 64

// maxDecodeLength is the maximum length we'll decode for variable-length values.
// This prevents OOM from malicious inputs (e.g., crafted msgpack with huge lengths).
const maxDecodeLength = 100 * 1024 * 1024 // 100MB

// maxRecordLength is the maximum size for msgpack record data.
// Records should be reasonably sized structured data, not massive blobs.
const maxRecordLength = 1024 * 1024 // 1MB

// validateMsgpackSize checks that msgpack data doesn't declare element counts
// that are impossible given the data length. This prevents OOM attacks where
// a small payload declares billions of entries.
func validateMsgpackSize(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	b := data[0]
	switch {
	case b >= 0x80 && b <= 0x8f: // fixmap: 0-15 entries
		count := int(b & 0x0f)
		return len(data) >= 1+count*2 // min 1 byte key + 1 byte value each
	case b >= 0x90 && b <= 0x9f: // fixarray: 0-15 elements
		count := int(b & 0x0f)
		return len(data) >= 1+count // min 1 byte each
	case b == 0xdc: // array16
		if len(data) < 3 {
			return false
		}
		count := int(data[1])<<8 | int(data[2])
		return len(data) >= 3+count
	case b == 0xdd: // array32
		if len(data) < 5 {
			return false
		}
		count := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])
		return len(data) >= 5+count
	case b == 0xde: // map16
		if len(data) < 3 {
			return false
		}
		count := int(data[1])<<8 | int(data[2])
		return len(data) >= 3+count*2
	case b == 0xdf: // map32
		if len(data) < 5 {
			return false
		}
		count := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])
		return len(data) >= 5+count*2
	}
	return true // other types (strings, ints, etc.) are ok
}

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

	// For records - map of field name to value
	Record map[string]any // Used for ValueTypeRecord
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
	case ValueTypeRecord:
		// For records, we need to encode first to know size
		if v.Record == nil {
			return 1 + 4 // type + length(0)
		}
		encoded, _ := msgpck.MarshalCopy(v.Record)
		return 1 + 4 + len(encoded) // type + length + msgpack data
	case ValueTypeMsgpack:
		return 1 + 4 + len(v.Bytes) // type + length + raw msgpack data
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
	case ValueTypeRecord:
		if v.Record == nil {
			dst = binary.LittleEndian.AppendUint32(dst, 0)
		} else {
			encoded, _ := msgpck.MarshalCopy(v.Record)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(encoded)))
			dst = append(dst, encoded...)
		}
	case ValueTypeMsgpack:
		// Raw msgpack bytes - store directly with length prefix
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(v.Bytes)))
		dst = append(dst, v.Bytes...)
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
	return decodeValueInternal(data, false)
}

// DecodeValue deserializes a value from bytes.
// Returns the value and number of bytes consumed.
func DecodeValue(data []byte) (Value, int, error) {
	return decodeValueInternal(data, true)
}

// decodeValueInternal is the shared implementation for value decoding.
// When copyBytes is true, byte data is copied for safety.
// When copyBytes is false, the returned Value's Bytes field points into data.
func decodeValueInternal(data []byte, copyBytes bool) (Value, int, error) {
	if len(data) < 1 {
		return Value{}, 0, ErrInvalidValue
	}

	v := Value{Type: ValueType(data[0])}

	switch v.Type {
	case ValueTypeInt64:
		return decodeInt64(data, v)
	case ValueTypeFloat64:
		return decodeFloat64(data, v)
	case ValueTypeBool:
		return decodeBool(data, v)
	case ValueTypeString, ValueTypeBytes:
		return decodeStringOrBytes(data, v, copyBytes)
	case ValueTypeRecord:
		return decodeRecord(data, v)
	case ValueTypeMsgpack:
		return decodeMsgpack(data, v, copyBytes)
	case ValueTypeTombstone:
		return v, 1, nil
	default:
		return Value{}, 0, ErrInvalidValue
	}
}

func decodeInt64(data []byte, v Value) (Value, int, error) {
	if len(data) < 9 {
		return Value{}, 0, ErrInvalidValue
	}
	v.Int64 = int64(binary.LittleEndian.Uint64(data[1:]))
	return v, 9, nil
}

func decodeFloat64(data []byte, v Value) (Value, int, error) {
	if len(data) < 9 {
		return Value{}, 0, ErrInvalidValue
	}
	v.Float64 = math.Float64frombits(binary.LittleEndian.Uint64(data[1:]))
	return v, 9, nil
}

func decodeBool(data []byte, v Value) (Value, int, error) {
	if len(data) < 2 {
		return Value{}, 0, ErrInvalidValue
	}
	v.Bool = data[1] != 0
	return v, 2, nil
}

func decodeStringOrBytes(data []byte, v Value, copyBytes bool) (Value, int, error) {
	if len(data) < 2 {
		return Value{}, 0, ErrInvalidValue
	}
	isPointer := data[1] == 1
	if isPointer {
		if len(data) < 2+dataPointerSize {
			return Value{}, 0, ErrInvalidValue
		}
		v.Pointer = &dataPointer{
			FileID:      binary.LittleEndian.Uint32(data[2:]),
			BlockOffset: binary.LittleEndian.Uint32(data[6:]),
			DataOffset:  binary.LittleEndian.Uint16(data[10:]),
			Length:      binary.LittleEndian.Uint32(data[12:]),
		}
		return v, 2 + dataPointerSize, nil
	}

	if len(data) < 6 {
		return Value{}, 0, ErrInvalidValue
	}
	length := binary.LittleEndian.Uint32(data[2:])
	if length > maxDecodeLength || len(data) < 6+int(length) {
		return Value{}, 0, ErrInvalidValue
	}
	if copyBytes {
		v.Bytes = make([]byte, length)
		copy(v.Bytes, data[6:6+length])
	} else {
		v.Bytes = data[6 : 6+length]
	}
	return v, 6 + int(length), nil
}

func decodeRecord(data []byte, v Value) (Value, int, error) {
	if len(data) < 5 {
		return Value{}, 0, ErrInvalidValue
	}
	length := binary.LittleEndian.Uint32(data[1:])
	if length > maxRecordLength || len(data) < 5+int(length) {
		return Value{}, 0, ErrInvalidValue
	}
	if length > 0 {
		msgpackData := data[5 : 5+length]
		if !validateMsgpackSize(msgpackData) {
			return Value{}, 0, ErrInvalidValue
		}
		var err error
		v.Record, err = msgpck.UnmarshalMapStringAny(msgpackData, false)
		if err != nil {
			return Value{}, 0, ErrInvalidValue
		}
	}
	return v, 5 + int(length), nil
}

func decodeMsgpack(data []byte, v Value, copyBytes bool) (Value, int, error) {
	if len(data) < 5 {
		return Value{}, 0, ErrInvalidValue
	}
	length := binary.LittleEndian.Uint32(data[1:])
	if length > maxDecodeLength || len(data) < 5+int(length) {
		return Value{}, 0, ErrInvalidValue
	}
	if copyBytes {
		v.Bytes = make([]byte, length)
		copy(v.Bytes, data[5:5+length])
	} else {
		v.Bytes = data[5 : 5+length]
	}
	return v, 5 + int(length), nil
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

// RecordValue creates a Value containing a structured record.
func RecordValue(fields map[string]any) Value {
	return Value{Type: ValueTypeRecord, Record: fields}
}

// MsgpackValue creates a Value containing raw msgpack bytes.
// This is more efficient than RecordValue for storing structs.
func MsgpackValue(data []byte) Value {
	return Value{Type: ValueTypeMsgpack, Bytes: data}
}

// DecodeMsgpack decodes msgpack bytes into a record map.
func DecodeMsgpack(data []byte) (map[string]any, error) {
	return msgpck.UnmarshalMapStringAny(data, false)
}

// EncodeMsgpack encodes a record map to msgpack bytes.
func EncodeMsgpack(record map[string]any) ([]byte, error) {
	return msgpck.MarshalCopy(record)
}

// EncodeJson encodes any value to JSON bytes.
func EncodeJson(data any) ([]byte, error) {
	return json.Marshal(data)
}

// DecodeJson decodes JSON bytes into the provided destination.
func DecodeJson(data []byte, dest any) error {
	return json.Unmarshal(data, dest)
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

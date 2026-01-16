package tinykvs

import (
	"bytes"
	"testing"
)

func TestCompareKeys(t *testing.T) {
	tests := []struct {
		a, b []byte
		want int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("a"), []byte("a"), 0},
		{[]byte("aa"), []byte("a"), 1},
		{[]byte("a"), []byte("aa"), -1},
		{[]byte(""), []byte("a"), -1},
		{[]byte("a"), []byte(""), 1},
		{[]byte(""), []byte(""), 0},
		{[]byte{0x00}, []byte{0x01}, -1},
		{[]byte{0xFF}, []byte{0x00}, 1},
	}

	for _, tt := range tests {
		got := CompareKeys(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("CompareKeys(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestValueEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value Value
	}{
		{"int64 positive", Int64Value(12345)},
		{"int64 negative", Int64Value(-12345)},
		{"int64 zero", Int64Value(0)},
		{"float64", Float64Value(3.14159)},
		{"float64 negative", Float64Value(-273.15)},
		{"bool true", BoolValue(true)},
		{"bool false", BoolValue(false)},
		{"string", StringValue("hello world")},
		{"string empty", StringValue("")},
		{"bytes", BytesValue([]byte{0x01, 0x02, 0x03})},
		{"bytes empty", BytesValue([]byte{})},
		{"tombstone", TombstoneValue()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeValue(tt.value)
			decoded, consumed, err := DecodeValue(encoded)
			if err != nil {
				t.Fatalf("DecodeValue failed: %v", err)
			}
			if consumed != len(encoded) {
				t.Errorf("consumed %d bytes, encoded %d bytes", consumed, len(encoded))
			}

			if decoded.Type != tt.value.Type {
				t.Errorf("type mismatch: got %d, want %d", decoded.Type, tt.value.Type)
			}

			switch tt.value.Type {
			case ValueTypeInt64:
				if decoded.Int64 != tt.value.Int64 {
					t.Errorf("int64 mismatch: got %d, want %d", decoded.Int64, tt.value.Int64)
				}
			case ValueTypeFloat64:
				if decoded.Float64 != tt.value.Float64 {
					t.Errorf("float64 mismatch: got %f, want %f", decoded.Float64, tt.value.Float64)
				}
			case ValueTypeBool:
				if decoded.Bool != tt.value.Bool {
					t.Errorf("bool mismatch: got %v, want %v", decoded.Bool, tt.value.Bool)
				}
			case ValueTypeString, ValueTypeBytes:
				if !bytes.Equal(decoded.Bytes, tt.value.Bytes) {
					t.Errorf("bytes mismatch: got %v, want %v", decoded.Bytes, tt.value.Bytes)
				}
			}
		})
	}
}

func TestEntryEncodeDecode(t *testing.T) {
	entry := Entry{
		Key:      []byte("test-key"),
		Value:    StringValue("test-value"),
		Sequence: 12345,
	}

	encoded := EncodeEntry(entry)
	decoded, consumed, err := DecodeEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeEntry failed: %v", err)
	}
	if consumed != len(encoded) {
		t.Errorf("consumed %d bytes, encoded %d bytes", consumed, len(encoded))
	}

	if !bytes.Equal(decoded.Key, entry.Key) {
		t.Errorf("key mismatch: got %s, want %s", decoded.Key, entry.Key)
	}
	if decoded.Sequence != entry.Sequence {
		t.Errorf("sequence mismatch: got %d, want %d", decoded.Sequence, entry.Sequence)
	}
	if decoded.Value.Type != entry.Value.Type {
		t.Errorf("value type mismatch")
	}
}

func TestValueHelpers(t *testing.T) {
	// Test IsTombstone
	if !TombstoneValue().IsTombstone() {
		t.Error("TombstoneValue should return true for IsTombstone")
	}
	if Int64Value(42).IsTombstone() {
		t.Error("Int64Value should return false for IsTombstone")
	}

	// Test String method
	sv := StringValue("hello")
	if sv.String() != "hello" {
		t.Errorf("String() = %q, want %q", sv.String(), "hello")
	}

	// Test GetBytes
	bv := BytesValue([]byte{1, 2, 3})
	if !bytes.Equal(bv.GetBytes(), []byte{1, 2, 3}) {
		t.Error("GetBytes mismatch")
	}
}

func BenchmarkCompareKeys(b *testing.B) {
	key1 := []byte("user:12345:profile:settings")
	key2 := []byte("user:12345:profile:data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompareKeys(key1, key2)
	}
}

func BenchmarkEncodeValue(b *testing.B) {
	v := StringValue("benchmark test value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeValue(v)
	}
}

func BenchmarkDecodeValue(b *testing.B) {
	v := StringValue("benchmark test value")
	encoded := EncodeValue(v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeValue(encoded)
	}
}

func TestDecodeValueZeroCopy(t *testing.T) {
	tests := []struct {
		name  string
		value Value
	}{
		{"int64", Int64Value(12345)},
		{"float64", Float64Value(3.14159)},
		{"bool true", BoolValue(true)},
		{"bool false", BoolValue(false)},
		{"string", StringValue("hello world")},
		{"bytes", BytesValue([]byte{0x01, 0x02, 0x03})},
		{"tombstone", TombstoneValue()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeValue(tt.value)
			decoded, consumed, err := DecodeValueZeroCopy(encoded)
			if err != nil {
				t.Fatalf("DecodeValueZeroCopy failed: %v", err)
			}
			if consumed != len(encoded) {
				t.Errorf("consumed %d bytes, encoded %d bytes", consumed, len(encoded))
			}
			if decoded.Type != tt.value.Type {
				t.Errorf("type mismatch: got %d, want %d", decoded.Type, tt.value.Type)
			}
		})
	}
}

func TestDecodeValueErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"int64 truncated", []byte{byte(ValueTypeInt64), 1, 2, 3}},                 // needs 9 bytes
		{"float64 truncated", []byte{byte(ValueTypeFloat64), 1, 2, 3, 4}},          // needs 9 bytes
		{"bool truncated", []byte{byte(ValueTypeBool)}},                            // needs 2 bytes
		{"string no flag", []byte{byte(ValueTypeString)}},                          // needs flag byte
		{"string inline truncated", []byte{byte(ValueTypeString), 0, 10, 0, 0, 0}}, // claims 10 bytes
		{"invalid type", []byte{99}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeValue(tt.data)
			if err != ErrInvalidValue {
				t.Errorf("expected ErrInvalidValue, got %v", err)
			}
		})
	}
}

func TestDecodeValueZeroCopyErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"int64 truncated", []byte{byte(ValueTypeInt64), 1, 2, 3}},
		{"string pointer truncated", []byte{byte(ValueTypeString), 1, 1, 2}}, // pointer flag but not enough data
		{"invalid type", []byte{99}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeValueZeroCopy(tt.data)
			if err != ErrInvalidValue {
				t.Errorf("expected ErrInvalidValue, got %v", err)
			}
		})
	}
}

func TestDecodeEntryErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too short for key len", []byte{1, 2}},
		{"key len exceeds data", []byte{10, 0, 0, 0, 1, 2, 3}}, // claims 10 byte key
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeEntry(tt.data)
			if err != ErrInvalidValue {
				t.Errorf("expected ErrInvalidValue, got %v", err)
			}
		})
	}
}

func TestValueStringAndGetBytes(t *testing.T) {
	// Test String() on non-string types
	iv := Int64Value(42)
	if iv.String() != "" {
		t.Errorf("Int64Value.String() should return empty, got %q", iv.String())
	}

	// Test GetBytes() on non-bytes types
	if iv.GetBytes() != nil {
		t.Error("Int64Value.GetBytes() should return nil")
	}

	// Test GetBytes on string type
	sv := StringValue("test")
	if string(sv.GetBytes()) != "test" {
		t.Errorf("StringValue.GetBytes() = %q, want test", sv.GetBytes())
	}
}

func TestDataPointerEncoding(t *testing.T) {
	// Test encoding/decoding of values with pointers (external data references)
	pointer := &dataPointer{
		FileID:      12345,
		BlockOffset: 67890,
		DataOffset:  100,
		Length:      500,
	}

	v := Value{
		Type:    ValueTypeString,
		Pointer: pointer,
	}

	encoded := EncodeValue(v)
	decoded, consumed, err := DecodeValue(encoded)
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}
	if consumed != len(encoded) {
		t.Errorf("consumed %d bytes, encoded %d bytes", consumed, len(encoded))
	}

	if decoded.Pointer == nil {
		t.Fatal("decoded pointer is nil")
	}
	if decoded.Pointer.FileID != pointer.FileID {
		t.Errorf("FileID = %d, want %d", decoded.Pointer.FileID, pointer.FileID)
	}
	if decoded.Pointer.BlockOffset != pointer.BlockOffset {
		t.Errorf("BlockOffset = %d, want %d", decoded.Pointer.BlockOffset, pointer.BlockOffset)
	}
	if decoded.Pointer.DataOffset != pointer.DataOffset {
		t.Errorf("DataOffset = %d, want %d", decoded.Pointer.DataOffset, pointer.DataOffset)
	}
	if decoded.Pointer.Length != pointer.Length {
		t.Errorf("Length = %d, want %d", decoded.Pointer.Length, pointer.Length)
	}
}

func TestDataPointerZeroCopy(t *testing.T) {
	pointer := &dataPointer{
		FileID:      1,
		BlockOffset: 2,
		DataOffset:  3,
		Length:      4,
	}

	v := Value{
		Type:    ValueTypeBytes,
		Pointer: pointer,
	}

	encoded := EncodeValue(v)
	decoded, _, err := DecodeValueZeroCopy(encoded)
	if err != nil {
		t.Fatalf("DecodeValueZeroCopy failed: %v", err)
	}

	if decoded.Pointer == nil {
		t.Fatal("decoded pointer is nil")
	}
	if decoded.Pointer.FileID != pointer.FileID {
		t.Errorf("FileID = %d, want %d", decoded.Pointer.FileID, pointer.FileID)
	}
}

func TestEncodedSizeWithPointer(t *testing.T) {
	// Inline value
	inline := StringValue("test")
	inlineSize := inline.EncodedSize()

	// Pointer value
	pointer := Value{
		Type: ValueTypeString,
		Pointer: &dataPointer{
			FileID:      1,
			BlockOffset: 2,
			DataOffset:  3,
			Length:      4,
		},
	}
	pointerSize := pointer.EncodedSize()

	// Pointer encoding: 1 (type) + 1 (flag) + 4 + 4 + 2 + 4 = 16 bytes
	if pointerSize != 16 {
		t.Errorf("pointer EncodedSize = %d, want 16", pointerSize)
	}

	// Inline encoding: 1 (type) + 1 (flag) + 4 (len) + 4 (data) = 10 bytes
	if inlineSize != 10 {
		t.Errorf("inline EncodedSize = %d, want 10", inlineSize)
	}
}

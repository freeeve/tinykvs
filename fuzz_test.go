package tinykvs

import (
	"testing"
)

// FuzzDecodeValue tests that DecodeValue doesn't panic on arbitrary input
func FuzzDecodeValue(f *testing.F) {
	// Add seed corpus with valid encoded values
	f.Add([]byte{byte(ValueTypeInt64), 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{byte(ValueTypeFloat64), 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{byte(ValueTypeBool), 1})
	f.Add([]byte{byte(ValueTypeString), 5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{byte(ValueTypeBytes), 3, 0, 0, 0, 1, 2, 3})
	f.Add([]byte{byte(ValueTypeTombstone)})
	// Invalid/edge cases
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{255})
	f.Add([]byte{byte(ValueTypeString), 255, 255, 255, 255}) // huge length

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic
		_, _, _ = DecodeValue(data)
	})
}

// FuzzDecodeValueZeroCopy tests the zero-copy variant
func FuzzDecodeValueZeroCopy(f *testing.F) {
	f.Add([]byte{byte(ValueTypeInt64), 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{byte(ValueTypeString), 5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{})
	f.Add([]byte{byte(ValueTypeString), 255, 255, 255, 255})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeValueZeroCopy(data)
	})
}

// FuzzDecodeBlock tests that DecodeBlock doesn't panic on arbitrary input
func FuzzDecodeBlock(f *testing.F) {
	// Valid block with minimal data (zstd compressed)
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("key"), []byte{byte(ValueTypeInt64), 1, 0, 0, 0, 0, 0, 0, 0})
	validBlock, _ := builder.Build(BlockTypeData, 1)
	f.Add(validBlock)

	// Valid snappy block
	builder.Reset()
	builder.Add([]byte("key"), []byte{byte(ValueTypeInt64), 1, 0, 0, 0, 0, 0, 0, 0})
	snappyBlock, _ := builder.BuildWithCompression(BlockTypeData, CompressionSnappy, 0)
	f.Add(snappyBlock)

	// Invalid/edge cases
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) // 13 bytes (footer size)
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})    // 12 bytes (legacy footer)

	f.Fuzz(func(t *testing.T, data []byte) {
		block, err := DecodeBlock(data, false)
		if err == nil && block != nil {
			block.Release()
		}
	})
}

// FuzzValueRoundTrip tests encode/decode round-trip consistency
func FuzzValueRoundTrip(f *testing.F) {
	f.Add(int64(0), float64(0), true, []byte("hello"))
	f.Add(int64(-1), float64(3.14), false, []byte{})
	f.Add(int64(9223372036854775807), float64(-1e308), true, []byte{0, 255})

	f.Fuzz(func(t *testing.T, i int64, fl float64, b bool, bs []byte) {
		// Test int64
		v1 := Int64Value(i)
		encoded := EncodeValue(v1)
		decoded, _, err := DecodeValue(encoded)
		if err != nil {
			t.Fatalf("failed to decode int64: %v", err)
		}
		if decoded.Int64 != i {
			t.Fatalf("int64 mismatch: got %d, want %d", decoded.Int64, i)
		}

		// Test float64
		v2 := Float64Value(fl)
		encoded = EncodeValue(v2)
		decoded, _, err = DecodeValue(encoded)
		if err != nil {
			t.Fatalf("failed to decode float64: %v", err)
		}
		// NaN != NaN, so handle specially
		if fl != decoded.Float64 && !(fl != fl && decoded.Float64 != decoded.Float64) {
			t.Fatalf("float64 mismatch: got %v, want %v", decoded.Float64, fl)
		}

		// Test bool
		v3 := BoolValue(b)
		encoded = EncodeValue(v3)
		decoded, _, err = DecodeValue(encoded)
		if err != nil {
			t.Fatalf("failed to decode bool: %v", err)
		}
		if decoded.Bool != b {
			t.Fatalf("bool mismatch: got %v, want %v", decoded.Bool, b)
		}

		// Test bytes
		v4 := BytesValue(bs)
		encoded = EncodeValue(v4)
		decoded, _, err = DecodeValue(encoded)
		if err != nil {
			t.Fatalf("failed to decode bytes: %v", err)
		}
		if string(decoded.Bytes) != string(bs) {
			t.Fatalf("bytes mismatch: got %v, want %v", decoded.Bytes, bs)
		}
	})
}

// FuzzBlockRoundTrip tests block encode/decode round-trip
func FuzzBlockRoundTrip(f *testing.F) {
	f.Add([]byte("key1"), []byte("value1"), uint8(0))
	f.Add([]byte{}, []byte{}, uint8(1))
	f.Add([]byte{0, 255, 128}, []byte{1, 2, 3, 4, 5}, uint8(2))

	f.Fuzz(func(t *testing.T, key, value []byte, compType uint8) {
		if len(key) == 0 {
			return // Skip empty keys
		}

		compression := CompressionType(compType % 3)
		builder := NewBlockBuilder(4096)
		builder.Add(key, value)

		data, err := builder.BuildWithCompression(BlockTypeData, compression, 1)
		if err != nil {
			t.Fatalf("failed to build block: %v", err)
		}

		block, err := DecodeBlock(data, true)
		if err != nil {
			t.Fatalf("failed to decode block: %v", err)
		}
		defer block.Release()

		if len(block.Entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(block.Entries))
		}
		if string(block.Entries[0].Key) != string(key) {
			t.Fatalf("key mismatch")
		}
		if string(block.Entries[0].Value) != string(value) {
			t.Fatalf("value mismatch")
		}
	})
}

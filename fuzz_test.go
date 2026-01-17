package tinykvs

import (
	"bytes"
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
	builder := newBlockBuilder(4096)
	builder.Add([]byte("key"), []byte{byte(ValueTypeInt64), 1, 0, 0, 0, 0, 0, 0, 0})
	validBlock, _ := builder.Build(blockTypeData, 1)
	f.Add(validBlock)

	// Valid snappy block
	builder.Reset()
	builder.Add([]byte("key"), []byte{byte(ValueTypeInt64), 1, 0, 0, 0, 0, 0, 0, 0})
	snappyBlock, _ := builder.BuildWithCompression(blockTypeData, CompressionSnappy, 0)
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

// FuzzDecodeEntry tests that DecodeEntry doesn't panic on arbitrary input
func FuzzDecodeEntry(f *testing.F) {
	// Valid entry: key_len(4) + key + seq(8) + value
	validEntry := []byte{
		5, 0, 0, 0, // key_len = 5
		'h', 'e', 'l', 'l', 'o', // key
		1, 0, 0, 0, 0, 0, 0, 0, // sequence = 1
		byte(ValueTypeInt64), 42, 0, 0, 0, 0, 0, 0, 0, // value
	}
	f.Add(validEntry)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})         // zero-length key
	f.Add([]byte{255, 255, 255, 255}) // huge key length

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeEntry(data)
	})
}

// FuzzEntryRoundTrip tests entry encode/decode consistency
func FuzzEntryRoundTrip(f *testing.F) {
	f.Add([]byte("key"), int64(1), int64(42))
	f.Add([]byte{}, int64(0), int64(0))
	f.Add([]byte{0, 255}, int64(9223372036854775807), int64(-1))

	f.Fuzz(func(t *testing.T, key []byte, seq int64, val int64) {
		if len(key) == 0 {
			return
		}
		entry := Entry{
			Key:      key,
			Sequence: uint64(seq),
			Value:    Int64Value(val),
		}
		encoded := EncodeEntry(entry)
		decoded, _, err := DecodeEntry(encoded)
		if err != nil {
			t.Fatalf("failed to decode entry: %v", err)
		}
		if string(decoded.Key) != string(key) {
			t.Fatalf("key mismatch")
		}
		if decoded.Sequence != uint64(seq) {
			t.Fatalf("sequence mismatch")
		}
		if decoded.Value.Int64 != val {
			t.Fatalf("value mismatch")
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
		builder := newBlockBuilder(4096)
		builder.Add(key, value)

		data, err := builder.BuildWithCompression(blockTypeData, compression, 1)
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

// FuzzDecodeMsgpack tests that DecodeMsgpack doesn't panic on arbitrary input
// Note: We skip inputs with map16/map32 markers (0xde, 0xdf) because they can
// declare huge element counts that cause OOM in the underlying msgpack library.
func FuzzDecodeMsgpack(f *testing.F) {
	// Valid msgpack maps using fixmap (0x80-0x8f) only
	f.Add([]byte{0x80})                                                                                     // empty map (fixmap)
	f.Add([]byte{0x81, 0xa3, 'k', 'e', 'y', 0xa5, 'v', 'a', 'l', 'u', 'e'})                                 // {"key":"value"}
	f.Add([]byte{0x82, 0xa4, 'n', 'a', 'm', 'e', 0xa5, 'A', 'l', 'i', 'c', 'e', 0xa3, 'a', 'g', 'e', 0x1e}) // {"name":"Alice","age":30}
	// Invalid/edge cases
	f.Add([]byte{})
	f.Add([]byte{0x82}) // truncated map
	f.Add([]byte{0xff}) // invalid marker
	f.Add([]byte{0x90}) // array, not map

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip inputs with map16/map32/array16/array32 markers that can declare
		// huge element counts causing OOM (library limitation)
		for _, b := range data {
			if b == 0xde || b == 0xdf || b == 0xdc || b == 0xdd {
				return
			}
		}
		// Should not panic
		_, _ = DecodeMsgpack(data)
	})
}

// FuzzRecordValueEncode tests Record encoding doesn't panic
func FuzzRecordValueEncode(f *testing.F) {
	f.Add("name", "Alice", "age", int64(30))
	f.Add("", "", "", int64(0))
	f.Add("key with spaces", "value\nwith\nnewlines", "number", int64(-9999))

	f.Fuzz(func(t *testing.T, k1, v1, k2 string, v2 int64) {
		record := map[string]any{
			k1: v1,
			k2: v2,
		}
		val := RecordValue(record)

		// Encode should not panic
		encoded := EncodeValue(val)

		// Decode should not panic and should round-trip
		decoded, _, err := DecodeValue(encoded)
		if err != nil {
			t.Fatalf("failed to decode record: %v", err)
		}
		if decoded.Type != ValueTypeRecord {
			t.Fatalf("expected record type, got %v", decoded.Type)
		}
	})
}

// FuzzStoreOperations tests basic store operations with random data
func FuzzStoreOperations(f *testing.F) {
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte(""), []byte(""))
	f.Add([]byte{0, 255, 128}, []byte{1, 2, 3, 4, 5})
	f.Add([]byte("user:123"), []byte(`{"name":"test"}`))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		if len(key) == 0 {
			return // Skip empty keys
		}

		dir := t.TempDir()
		opts := DefaultOptions(dir)
		opts.MemtableSize = 1024 * 1024 // Small memtable for faster tests

		store, err := Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()

		// Put should not panic
		err = store.PutBytes(key, value)
		if err != nil {
			return // Some keys might fail, that's ok
		}

		// Get should not panic and should return the value
		got, err := store.GetBytes(key)
		if err != nil {
			t.Fatalf("failed to get key after put: %v", err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value mismatch: got %v, want %v", got, value)
		}

		// Delete should not panic
		_ = store.Delete(key)
	})
}

// FuzzPrefixCompare tests key prefix operations
func FuzzPrefixCompare(f *testing.F) {
	f.Add([]byte("user:"), []byte("user:123"))
	f.Add([]byte(""), []byte("anything"))
	f.Add([]byte("exact"), []byte("exact"))
	f.Add([]byte("longer"), []byte("short"))
	f.Add([]byte{0, 0, 0}, []byte{0, 0, 0, 1})

	f.Fuzz(func(t *testing.T, prefix, key []byte) {
		// Should not panic
		_ = bytes.HasPrefix(key, prefix)
		_ = bytes.Compare(prefix, key)
	})
}

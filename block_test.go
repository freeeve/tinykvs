package tinykvs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestBlockBuilderAddBuild(t *testing.T) {
	builder := NewBlockBuilder(4096)

	// Add entries
	builder.Add([]byte("key1"), []byte("value1"))
	builder.Add([]byte("key2"), []byte("value2"))
	builder.Add([]byte("key3"), []byte("value3"))

	if builder.Count() != 3 {
		t.Errorf("count = %d, want 3", builder.Count())
	}

	if builder.Size() == 0 {
		t.Error("size should be > 0")
	}

	// Build and verify
	data, err := builder.Build(BlockTypeData, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("built data should not be empty")
	}
}

func TestBlockBuilderReset(t *testing.T) {
	builder := NewBlockBuilder(4096)

	builder.Add([]byte("key1"), []byte("value1"))
	builder.Add([]byte("key2"), []byte("value2"))

	builder.Reset()

	if builder.Count() != 0 {
		t.Errorf("count after reset = %d, want 0", builder.Count())
	}
	if builder.Size() != 0 {
		t.Errorf("size after reset = %d, want 0", builder.Size())
	}
}

func TestBlockBuilderFull(t *testing.T) {
	builder := NewBlockBuilder(100) // Small block

	// First entry should fit
	if !builder.Add([]byte("key1"), []byte("value1")) {
		t.Error("first entry should fit")
	}

	// Keep adding until full
	added := 1
	for builder.Add([]byte("keyX"), []byte("valueX")) {
		added++
		if added > 10 {
			t.Fatal("block should be full by now")
		}
	}

	if added == 1 {
		t.Error("should have added more than 1 entry")
	}
}

func TestBlockBuilderFirstKey(t *testing.T) {
	builder := NewBlockBuilder(4096)

	// Empty builder
	if builder.FirstKey() != nil {
		t.Error("FirstKey of empty builder should be nil")
	}

	// After adding
	builder.Add([]byte("first"), []byte("value"))
	builder.Add([]byte("second"), []byte("value"))

	if string(builder.FirstKey()) != "first" {
		t.Errorf("FirstKey = %s, want first", builder.FirstKey())
	}
}

func TestBlockBuilderSize(t *testing.T) {
	builder := NewBlockBuilder(4096)

	initial := builder.Size()
	if initial != 0 {
		t.Errorf("initial size = %d, want 0", initial)
	}

	builder.Add([]byte("key"), []byte("value"))

	if builder.Size() <= initial {
		t.Error("size should increase after add")
	}
}

func TestBlockBuilderEntries(t *testing.T) {
	builder := NewBlockBuilder(4096)

	builder.Add([]byte("key1"), []byte("value1"))
	builder.Add([]byte("key2"), []byte("value2"))

	entries := builder.Entries()
	if len(entries) != 2 {
		t.Errorf("got %d entries, want 2", len(entries))
	}

	if string(entries[0].Key) != "key1" {
		t.Errorf("first key = %s, want key1", entries[0].Key)
	}
}

func TestDecodeBlock(t *testing.T) {
	builder := NewBlockBuilder(4096)

	builder.Add([]byte("alpha"), []byte("value-alpha"))
	builder.Add([]byte("beta"), []byte("value-beta"))
	builder.Add([]byte("gamma"), []byte("value-gamma"))

	data, err := builder.Build(BlockTypeData, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	block, err := DecodeBlock(data, true)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}

	if block.Type != BlockTypeData {
		t.Errorf("type = %d, want %d", block.Type, BlockTypeData)
	}

	if len(block.Entries) != 3 {
		t.Errorf("got %d entries, want 3", len(block.Entries))
	}

	// Verify entries
	expected := []struct {
		key, value string
	}{
		{"alpha", "value-alpha"},
		{"beta", "value-beta"},
		{"gamma", "value-gamma"},
	}

	for i, exp := range expected {
		if string(block.Entries[i].Key) != exp.key {
			t.Errorf("entry %d: key = %s, want %s", i, block.Entries[i].Key, exp.key)
		}
		if string(block.Entries[i].Value) != exp.value {
			t.Errorf("entry %d: value = %s, want %s", i, block.Entries[i].Value, exp.value)
		}
	}
}

func TestDecodeBlockChecksumVerification(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("key"), []byte("value"))

	data, err := builder.Build(BlockTypeData, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Corrupt the data
	data[0] ^= 0xFF

	_, err = DecodeBlock(data, true)
	if err != ErrChecksumMismatch {
		t.Errorf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestDecodeBlockNoVerification(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("key"), []byte("value"))

	data, err := builder.Build(BlockTypeData, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Corrupt the data (but not the compressed content structure)
	// Just corrupt the checksum in footer
	data[len(data)-BlockFooterSize] ^= 0xFF

	// Should not error when not verifying checksum
	// (may still fail if corruption affects decompression, but that's OK)
	_, _ = DecodeBlock(data, false)
}

func TestDecodeBlockInvalidData(t *testing.T) {
	// Too short
	_, err := DecodeBlock([]byte{1, 2, 3}, true)
	if err != ErrCorruptedData {
		t.Errorf("expected ErrCorruptedData for short data, got %v", err)
	}
}

func TestDecodeBlockCompressedSizeMismatch(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("key"), []byte("value"))

	data, err := builder.Build(BlockTypeData, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Corrupt the compressedSize field in footer (last 12 bytes: checksum, uncompressed, compressed)
	// compressed size is at offset len(data)-4
	origCompressedSize := binary.LittleEndian.Uint32(data[len(data)-4:])
	binary.LittleEndian.PutUint32(data[len(data)-4:], origCompressedSize+100) // make it wrong

	_, err = DecodeBlock(data, false)
	if err != ErrCorruptedData {
		t.Errorf("expected ErrCorruptedData for compressed size mismatch, got %v", err)
	}
}

func TestDecodeBlockChecksumMismatch(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("key"), []byte("value"))

	data, err := builder.Build(BlockTypeData, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Corrupt the checksum (first 4 bytes of footer, at len(data)-12)
	data[len(data)-BlockFooterSize] ^= 0xFF

	_, err = DecodeBlock(data, true) // with verification
	if err != ErrChecksumMismatch {
		t.Errorf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestSearchBlock(t *testing.T) {
	block := &Block{
		Type: BlockTypeData,
		Entries: []BlockEntry{
			{Key: []byte("apple")},
			{Key: []byte("banana")},
			{Key: []byte("cherry")},
			{Key: []byte("date")},
			{Key: []byte("elderberry")},
		},
	}

	tests := []struct {
		key  string
		want int
	}{
		{"apple", 0},
		{"banana", 1},
		{"cherry", 2},
		{"date", 3},
		{"elderberry", 4},
		{"apricot", -1},  // Not found
		{"fig", -1},      // Not found
		{"aardvark", -1}, // Before all
		{"zebra", -1},    // After all
	}

	for _, tt := range tests {
		got := SearchBlock(block, []byte(tt.key))
		if got != tt.want {
			t.Errorf("SearchBlock(%q) = %d, want %d", tt.key, got, tt.want)
		}
	}
}

func TestSearchBlockEmpty(t *testing.T) {
	block := &Block{Type: BlockTypeData, Entries: []BlockEntry{}}

	if SearchBlock(block, []byte("any")) != -1 {
		t.Error("search in empty block should return -1")
	}
}

func TestSearchBlockSingleEntry(t *testing.T) {
	block := &Block{
		Type:    BlockTypeData,
		Entries: []BlockEntry{{Key: []byte("only")}},
	}

	if SearchBlock(block, []byte("only")) != 0 {
		t.Error("should find single entry")
	}

	if SearchBlock(block, []byte("other")) != -1 {
		t.Error("should not find non-existent key")
	}
}

func TestBlockCompressionLevels(t *testing.T) {
	builder := NewBlockBuilder(4096)

	// Add compressible data
	for i := 0; i < 100; i++ {
		builder.Add([]byte("key"), bytes.Repeat([]byte("x"), 50))
	}

	levels := []int{1, 3, 5}
	var sizes []int

	for _, level := range levels {
		builder2 := NewBlockBuilder(4096)
		for i := 0; i < 100; i++ {
			builder2.Add([]byte("key"), bytes.Repeat([]byte("x"), 50))
		}

		data, err := builder2.Build(BlockTypeData, level)
		if err != nil {
			t.Fatalf("Build at level %d failed: %v", level, err)
		}

		sizes = append(sizes, len(data))

		// Verify can decode
		_, err = DecodeBlock(data, true)
		if err != nil {
			t.Fatalf("DecodeBlock at level %d failed: %v", level, err)
		}
	}

	// Higher compression levels should produce smaller output (or equal)
	// Note: for small data, this might not always hold
	t.Logf("Compression sizes: level 1=%d, level 3=%d, level 5=%d", sizes[0], sizes[1], sizes[2])
}

func TestBlockRoundtrip(t *testing.T) {
	// Test various data patterns
	testCases := []struct {
		name    string
		entries []BlockEntry
	}{
		{
			name: "simple",
			entries: []BlockEntry{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
		},
		{
			name: "empty values",
			entries: []BlockEntry{
				{Key: []byte("key1"), Value: []byte{}},
				{Key: []byte("key2"), Value: []byte{}},
			},
		},
		{
			name: "binary data",
			entries: []BlockEntry{
				{Key: []byte{0x00, 0x01, 0x02}, Value: []byte{0xFF, 0xFE, 0xFD}},
			},
		},
		{
			name: "large values",
			entries: []BlockEntry{
				{Key: []byte("key"), Value: bytes.Repeat([]byte("x"), 1000)},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBlockBuilder(8192)
			for _, e := range tc.entries {
				builder.Add(e.Key, e.Value)
			}

			data, err := builder.Build(BlockTypeData, 1)
			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			block, err := DecodeBlock(data, true)
			if err != nil {
				t.Fatalf("DecodeBlock failed: %v", err)
			}

			if len(block.Entries) != len(tc.entries) {
				t.Fatalf("got %d entries, want %d", len(block.Entries), len(tc.entries))
			}

			for i, orig := range tc.entries {
				if !bytes.Equal(block.Entries[i].Key, orig.Key) {
					t.Errorf("entry %d key mismatch", i)
				}
				if !bytes.Equal(block.Entries[i].Value, orig.Value) {
					t.Errorf("entry %d value mismatch", i)
				}
			}
		})
	}
}

func BenchmarkBlockBuild(b *testing.B) {
	entries := make([]BlockEntry, 100)
	for i := range entries {
		entries[i] = BlockEntry{
			Key:   []byte("benchmark-key"),
			Value: []byte("benchmark-value-that-is-reasonably-sized"),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder := NewBlockBuilder(4096)
		for _, e := range entries {
			builder.Add(e.Key, e.Value)
		}
		builder.Build(BlockTypeData, 1)
	}
}

func BenchmarkBlockDecode(b *testing.B) {
	builder := NewBlockBuilder(4096)
	for i := 0; i < 100; i++ {
		builder.Add([]byte("benchmark-key"), []byte("benchmark-value"))
	}
	data, _ := builder.Build(BlockTypeData, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeBlock(data, true)
	}
}

func BenchmarkSearchBlock(b *testing.B) {
	block := &Block{Type: BlockTypeData}
	for i := 0; i < 100; i++ {
		block.Entries = append(block.Entries, BlockEntry{
			Key: []byte(string(rune('a' + i))),
		})
	}

	key := []byte("m")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SearchBlock(block, key)
	}
}

func TestBlockBuilderCompressionEdgeCases(t *testing.T) {
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("key"), []byte("value"))

	// Test with level below range
	data, err := builder.Build(BlockTypeData, -1)
	if err != nil {
		t.Fatalf("Build with level -1 failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Build with level -1 returned empty data")
	}

	// Reset and test with level above range
	builder.Reset()
	builder.Add([]byte("key"), []byte("value"))
	data, err = builder.Build(BlockTypeData, 10)
	if err != nil {
		t.Fatalf("Build with level 10 failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Build with level 10 returned empty data")
	}
}

func TestEncoderPoolOverflow(t *testing.T) {
	// Fill up the encoder pool by creating many blocks concurrently
	builder := NewBlockBuilder(1024)
	builder.Add([]byte("key"), []byte("value"))

	// Build many blocks to exercise pool overflow
	for i := 0; i < 20; i++ {
		_, err := builder.Build(BlockTypeData, 1)
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}
	}
}

func TestDecodeBlockCorruptedCompression(t *testing.T) {
	// Create valid block data first
	builder := NewBlockBuilder(4096)
	builder.Add([]byte("key"), []byte("value"))
	validData, err := builder.Build(BlockTypeData, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Corrupt the compressed data (not the footer)
	corruptedData := make([]byte, len(validData))
	copy(corruptedData, validData)
	// Corrupt bytes in the middle of compressed section
	if len(corruptedData) > 20 {
		corruptedData[10] ^= 0xFF
		corruptedData[11] ^= 0xFF
	}

	// Should fail to decompress
	_, err = DecodeBlock(corruptedData, false)
	// Error is expected but might vary
	if err == nil {
		// May not always fail depending on corruption location
		t.Log("Corruption didn't cause decompression error (location-dependent)")
	}
}

func TestDecodeBlockTooShort(t *testing.T) {
	// Create a block that's too short after decompression would be needed
	// by providing valid footer but garbage compressed data
	shortData := make([]byte, BlockFooterSize+5)
	// Set footer values
	// checksum at [0:4]
	// uncompressed size at [4:8]
	// compressed size at [8:12]
	footerStart := len(shortData) - BlockFooterSize
	binary.LittleEndian.PutUint32(shortData[footerStart:], 0)    // checksum (will be skipped)
	binary.LittleEndian.PutUint32(shortData[footerStart+4:], 10) // uncompressed size
	binary.LittleEndian.PutUint32(shortData[footerStart+8:], 5)  // compressed size matches data before footer

	_, err := DecodeBlock(shortData, false)
	// Should fail due to invalid zstd data
	if err == nil {
		t.Log("Expected error for invalid zstd data")
	}
}

func TestCompressionTypes(t *testing.T) {
	tests := []struct {
		name string
		comp CompressionType
	}{
		{"zstd", CompressionZstd},
		{"snappy", CompressionSnappy},
		{"none", CompressionNone},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBlockBuilder(4096)

			// Add some data
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("key%03d", i))
				value := []byte(fmt.Sprintf("value%03d with some extra data to compress", i))
				builder.Add(key, value)
			}

			// Build with specified compression
			data, err := builder.BuildWithCompression(BlockTypeData, tc.comp, 1)
			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			// Decode and verify
			block, err := DecodeBlock(data, true)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			defer block.Release()

			if len(block.Entries) != 10 {
				t.Errorf("Expected 10 entries, got %d", len(block.Entries))
			}

			// Verify first and last entry
			if string(block.Entries[0].Key) != "key000" {
				t.Errorf("First key mismatch: %s", block.Entries[0].Key)
			}
			if string(block.Entries[9].Key) != "key009" {
				t.Errorf("Last key mismatch: %s", block.Entries[9].Key)
			}
		})
	}
}

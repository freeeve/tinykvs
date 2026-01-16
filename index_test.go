package tinykvs

import (
	"bytes"
	"testing"
)

func TestIndexBuilderBuild(t *testing.T) {
	builder := newIndexBuilder()

	builder.Add([]byte("aaa"), []byte("azz"), 0, 1000, 50)
	builder.Add([]byte("baa"), []byte("bzz"), 1000, 1000, 50)
	builder.Add([]byte("caa"), []byte("czz"), 2000, 1000, 50)

	index := builder.Build()

	if len(index.Entries) != 3 {
		t.Errorf("entries = %d, want 3", len(index.Entries))
	}

	if string(index.MinKey) != "aaa" {
		t.Errorf("MinKey = %s, want aaa", index.MinKey)
	}

	if string(index.MaxKey) != "czz" {
		t.Errorf("MaxKey = %s, want czz", index.MaxKey)
	}

	if index.NumKeys != 150 {
		t.Errorf("NumKeys = %d, want 150", index.NumKeys)
	}
}

func TestIndexSearch(t *testing.T) {
	builder := newIndexBuilder()

	// Each block covers a key range
	builder.Add([]byte("aaa"), []byte("azz"), 0, 1000, 10)
	builder.Add([]byte("baa"), []byte("bzz"), 1000, 1000, 10)
	builder.Add([]byte("caa"), []byte("czz"), 2000, 1000, 10)
	builder.Add([]byte("daa"), []byte("dzz"), 3000, 1000, 10)

	index := builder.Build()

	tests := []struct {
		key  string
		want int
	}{
		{"aaa", 0},
		{"amm", 0},
		{"azz", 0},
		{"baa", 1},
		{"bbb", 1},
		{"ccc", 2},
		{"ddd", 3},
		{"dzz", 3},
	}

	for _, tt := range tests {
		got := index.Search([]byte(tt.key))
		if got != tt.want {
			t.Errorf("Search(%s) = %d, want %d", tt.key, got, tt.want)
		}
	}
}

func TestIndexSearchOutOfRange(t *testing.T) {
	builder := newIndexBuilder()
	builder.Add([]byte("baa"), []byte("bzz"), 0, 1000, 10)
	builder.Add([]byte("caa"), []byte("czz"), 1000, 1000, 10)

	index := builder.Build()

	// Before all keys
	if index.Search([]byte("aaa")) != -1 {
		t.Error("key before range should return -1")
	}

	// After all keys
	if index.Search([]byte("zzz")) != -1 {
		t.Error("key after range should return -1")
	}
}

func TestIndexSearchEmpty(t *testing.T) {
	builder := newIndexBuilder()
	index := builder.Build()

	if index.Search([]byte("any")) != -1 {
		t.Error("search in empty index should return -1")
	}
}

func TestIndexSerializeDeserialize(t *testing.T) {
	builder := newIndexBuilder()
	builder.Add([]byte("alpha"), []byte("azulu"), 0, 1000, 25)
	builder.Add([]byte("bravo"), []byte("bzulu"), 1000, 2000, 50)
	builder.Add([]byte("charlie"), []byte("czulu"), 3000, 1500, 30)

	original := builder.Build()

	// Serialize
	data := original.Serialize()

	// Deserialize
	restored, err := DeserializeIndex(data)
	if err != nil {
		t.Fatalf("DeserializeIndex failed: %v", err)
	}

	// Compare
	if restored.NumKeys != original.NumKeys {
		t.Errorf("NumKeys = %d, want %d", restored.NumKeys, original.NumKeys)
	}

	if !bytes.Equal(restored.MinKey, original.MinKey) {
		t.Errorf("MinKey mismatch")
	}

	if !bytes.Equal(restored.MaxKey, original.MaxKey) {
		t.Errorf("MaxKey mismatch")
	}

	if len(restored.Entries) != len(original.Entries) {
		t.Fatalf("entries count = %d, want %d", len(restored.Entries), len(original.Entries))
	}

	for i, e := range restored.Entries {
		if !bytes.Equal(e.Key, original.Entries[i].Key) {
			t.Errorf("entry %d key mismatch", i)
		}
		if e.BlockOffset != original.Entries[i].BlockOffset {
			t.Errorf("entry %d offset = %d, want %d", i, e.BlockOffset, original.Entries[i].BlockOffset)
		}
		if e.BlockSize != original.Entries[i].BlockSize {
			t.Errorf("entry %d size = %d, want %d", i, e.BlockSize, original.Entries[i].BlockSize)
		}
	}
}

func TestIndexDeserializeInvalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2, 3}},
		{"minkey length truncated", []byte{0, 0, 0, 0, 0, 0, 0, 0}},            // numKeys but no minKeyLen
		{"minkey data truncated", []byte{0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0}}, // claims 10 byte minkey
		{"maxkey length truncated", []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // numKeys
			1, 0, 0, 0, // minKeyLen = 1
			'a', // minKey
		}}, // no maxKeyLen
		{"maxkey data truncated", []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // numKeys
			1, 0, 0, 0, // minKeyLen = 1
			'a',         // minKey
			10, 0, 0, 0, // maxKeyLen = 10 (but only 0 bytes follow)
		}},
		{"numEntries truncated", []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // numKeys
			1, 0, 0, 0, 'a', // minKey
			1, 0, 0, 0, 'z', // maxKey
			// no numEntries
		}},
		{"entry keyLen truncated", []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // numKeys
			1, 0, 0, 0, 'a', // minKey
			1, 0, 0, 0, 'z', // maxKey
			1, 0, 0, 0, // numEntries = 1
			// no entry keyLen
		}},
		{"entry data truncated", []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // numKeys
			1, 0, 0, 0, 'a', // minKey
			1, 0, 0, 0, 'z', // maxKey
			1, 0, 0, 0, // numEntries = 1
			3, 0, 0, 0, // keyLen = 3
			'k', 'e', 'y', // key but no offset/size (needs 12 more bytes)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeserializeIndex(tt.data)
			if err != ErrCorruptedData {
				t.Errorf("expected ErrCorruptedData, got %v", err)
			}
		})
	}
}

func TestBloomFilterAddContains(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		bf.Add([]byte(k))
	}

	// All added keys should be found
	for _, k := range keys {
		if !bf.MayContain([]byte(k)) {
			t.Errorf("bloom filter should contain %s", k)
		}
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	n := uint(10000)
	fpRate := 0.01
	bf := NewBloomFilter(n, fpRate)

	// Add n keys
	for i := uint(0); i < n; i++ {
		key := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		bf.Add(key)
	}

	// Test n non-existent keys
	falsePositives := 0
	for i := uint(n); i < 2*n; i++ {
		key := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if bf.MayContain(key) {
			falsePositives++
		}
	}

	actualRate := float64(falsePositives) / float64(n)
	// Allow 3x the target rate for statistical variance
	if actualRate > fpRate*3 {
		t.Errorf("false positive rate = %.3f, want < %.3f", actualRate, fpRate*3)
	}
}

func TestBloomFilterSerializeDeserialize(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, k := range keys {
		bf.Add([]byte(k))
	}

	// Serialize
	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	restored, err := DeserializeBloomFilter(data)
	if err != nil {
		t.Fatalf("DeserializeBloomFilter failed: %v", err)
	}

	// Verify all keys are still found
	for _, k := range keys {
		if !restored.MayContain([]byte(k)) {
			t.Errorf("restored filter should contain %s", k)
		}
	}
}

func TestBloomFilterDeserializeInvalid(t *testing.T) {
	_, err := DeserializeBloomFilter([]byte{1, 2, 3})
	if err == nil {
		t.Error("should error on invalid data")
	}
}

func BenchmarkIndexSearch(b *testing.B) {
	builder := newIndexBuilder()
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i / 256), byte(i % 256)}
		builder.Add(key, key, uint64(i*1000), 1000, 10)
	}
	index := builder.Build()

	key := []byte{1, 244} // Somewhere in the middle

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Search(key)
	}
}

func BenchmarkBloomFilterMayContain(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	for i := 0; i < 100000; i++ {
		key := []byte{byte(i >> 16), byte(i >> 8), byte(i)}
		bf.Add(key)
	}

	key := []byte{0, 128, 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain(key)
	}
}

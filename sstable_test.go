package tinykvs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0644)
}

func TestSSTableWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	// Write SSTable
	writer, err := NewSSTableWriter(1, path, 100, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	// Entries must be in sorted order for SSTable
	entries := []Entry{
		{Key: []byte("alpha"), Value: StringValue("value-alpha"), Sequence: 1},
		{Key: []byte("beta"), Value: Int64Value(42), Sequence: 2},
		{Key: []byte("delta"), Value: BytesValue([]byte{1, 2, 3}), Sequence: 3},
		{Key: []byte("gamma"), Value: BoolValue(true), Sequence: 4},
	}

	for _, e := range entries {
		if err := writer.Add(e); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := writer.Finish(0); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	writer.Close()

	// Read SSTable
	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	// Verify metadata
	if sst.Footer.NumKeys != 4 {
		t.Errorf("NumKeys = %d, want 4", sst.Footer.NumKeys)
	}

	// Verify we can get each key
	cache := NewLRUCache(1024 * 1024)
	for _, e := range entries {
		got, found, err := sst.Get(e.Key, cache, true)
		if err != nil {
			t.Errorf("Get(%s) error: %v", e.Key, err)
			continue
		}
		if !found {
			t.Errorf("Get(%s) not found", e.Key)
			continue
		}
		if got.Value.Type != e.Value.Type {
			t.Errorf("Get(%s) type = %d, want %d", e.Key, got.Value.Type, e.Value.Type)
		}
	}

	// Verify non-existent key
	_, found, err := sst.Get([]byte("nonexistent"), cache, true)
	if err != nil {
		t.Errorf("Get(nonexistent) error: %v", err)
	}
	if found {
		t.Error("Get(nonexistent) should not find anything")
	}
}

func TestSSTableBloomFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(1, path, 1000, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	// Add many keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%04d", i)
		writer.Add(Entry{Key: []byte(key), Value: Int64Value(int64(i)), Sequence: uint64(i)})
	}

	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	// All existing keys should pass bloom filter
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%04d", i*10)
		if !sst.BloomFilter.MayContain([]byte(key)) {
			t.Errorf("bloom filter should contain %s", key)
		}
	}

	// Non-existent keys should mostly be filtered (with some false positives)
	falsePositives := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("notkey%04d", i)
		if sst.BloomFilter.MayContain([]byte(key)) {
			falsePositives++
		}
	}

	// With 1% FP rate, expect ~10 false positives, allow up to 30
	if falsePositives > 30 {
		t.Errorf("too many false positives: %d", falsePositives)
	}
}

func TestSSTableMinMaxKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(1, path, 100, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	writer.Add(Entry{Key: []byte("apple"), Value: Int64Value(1), Sequence: 1})
	writer.Add(Entry{Key: []byte("banana"), Value: Int64Value(2), Sequence: 2})
	writer.Add(Entry{Key: []byte("cherry"), Value: Int64Value(3), Sequence: 3})

	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	if string(sst.MinKey()) != "apple" {
		t.Errorf("MinKey = %s, want apple", sst.MinKey())
	}
	if string(sst.MaxKey()) != "cherry" {
		t.Errorf("MaxKey = %s, want cherry", sst.MaxKey())
	}
}

func TestSSTableLevel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(1, path, 10, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	writer.Add(Entry{Key: []byte("key"), Value: Int64Value(1), Sequence: 1})
	writer.Finish(3) // Level 3
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	if sst.Level != 3 {
		t.Errorf("Level = %d, want 3", sst.Level)
	}
}

func TestSSTableAbort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(1, path, 10, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	writer.Add(Entry{Key: []byte("key"), Value: Int64Value(1), Sequence: 1})

	// Abort instead of finish
	if err := writer.Abort(); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// File should be deleted
	if _, err := OpenSSTable(1, path); err == nil {
		t.Error("file should not exist after abort")
	}
}

func TestSSTableLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	n := 10000
	writer, err := NewSSTableWriter(1, path, uint(n), opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		writer.Add(Entry{Key: []byte(key), Value: StringValue(value), Sequence: uint64(i)})
	}

	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	cache := NewLRUCache(64 * 1024 * 1024)

	// Random access
	for i := 0; i < 100; i++ {
		idx := (i * 97) % n
		key := fmt.Sprintf("key%08d", idx)
		expectedValue := fmt.Sprintf("value%08d", idx)

		entry, found, err := sst.Get([]byte(key), cache, true)
		if err != nil {
			t.Errorf("Get(%s) error: %v", key, err)
			continue
		}
		if !found {
			t.Errorf("Get(%s) not found", key)
			continue
		}
		if entry.Value.String() != expectedValue {
			t.Errorf("Get(%s) = %s, want %s", key, entry.Value.String(), expectedValue)
		}
	}
}

func TestSSTableTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(1, path, 10, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	writer.Add(Entry{Key: []byte("key1"), Value: StringValue("value1"), Sequence: 1})
	writer.Add(Entry{Key: []byte("key2"), Value: TombstoneValue(), Sequence: 2})
	writer.Add(Entry{Key: []byte("key3"), Value: StringValue("value3"), Sequence: 3})

	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	cache := NewLRUCache(1024 * 1024)

	// key2 should return tombstone
	entry, found, err := sst.Get([]byte("key2"), cache, true)
	if err != nil {
		t.Fatalf("Get(key2) error: %v", err)
	}
	if !found {
		t.Fatal("Get(key2) should find tombstone")
	}
	if !entry.Value.IsTombstone() {
		t.Error("key2 value should be tombstone")
	}
}

func TestSSTableMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)
	opts.BlockSize = 256 // Small blocks to force multiple

	writer, err := NewSSTableWriter(1, path, 100, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	// Add entries that will span multiple blocks
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := bytes.Repeat([]byte("x"), 50)
		writer.Add(Entry{Key: []byte(key), Value: BytesValue(value), Sequence: uint64(i)})
	}

	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	// Should have multiple data blocks
	if sst.Footer.NumDataBlocks <= 1 {
		t.Errorf("expected multiple blocks, got %d", sst.Footer.NumDataBlocks)
	}

	// Verify all keys are readable
	cache := NewLRUCache(1024 * 1024)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		_, found, err := sst.Get([]byte(key), cache, true)
		if err != nil {
			t.Errorf("Get(%s) error: %v", key, err)
		}
		if !found {
			t.Errorf("Get(%s) not found", key)
		}
	}
}

func TestSSTableNoCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(1, path, 10, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	writer.Add(Entry{Key: []byte("key"), Value: StringValue("value"), Sequence: 1})
	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	// Get with nil cache
	entry, found, err := sst.Get([]byte("key"), nil, true)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !found {
		t.Fatal("key not found")
	}
	if entry.Value.String() != "value" {
		t.Errorf("value = %s, want value", entry.Value.String())
	}
}

func TestSSTableSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(1, path, 10, opts)
	if err != nil {
		t.Fatalf("NewSSTableWriter failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		writer.Add(Entry{Key: []byte(key), Value: StringValue("value"), Sequence: uint64(i)})
	}

	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	if sst.Size() == 0 {
		t.Error("Size should be > 0")
	}
}

func TestOpenSSTableInvalid(t *testing.T) {
	dir := t.TempDir()

	// Test non-existent file
	_, err := OpenSSTable(1, filepath.Join(dir, "nonexistent.sst"))
	if err == nil {
		t.Error("OpenSSTable should fail for non-existent file")
	}

	// Test file too small
	smallPath := filepath.Join(dir, "small.sst")
	if err := writeFile(smallPath, []byte("too small")); err != nil {
		t.Fatalf("Failed to create small file: %v", err)
	}
	_, err = OpenSSTable(1, smallPath)
	if err != ErrInvalidSSTable {
		t.Errorf("OpenSSTable should fail with ErrInvalidSSTable for small file, got %v", err)
	}

	// Test file with wrong magic
	wrongMagicPath := filepath.Join(dir, "wrongmagic.sst")
	wrongMagic := make([]byte, SSTableFooterSize)
	if err := writeFile(wrongMagicPath, wrongMagic); err != nil {
		t.Fatalf("Failed to create wrong magic file: %v", err)
	}
	_, err = OpenSSTable(1, wrongMagicPath)
	if err != ErrInvalidSSTable {
		t.Errorf("OpenSSTable should fail with ErrInvalidSSTable for wrong magic, got %v", err)
	}
}

func BenchmarkSSTableWrite(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, fmt.Sprintf("test%d.sst", i))
		writer, _ := NewSSTableWriter(uint32(i), path, 1000, opts)

		for j := 0; j < 1000; j++ {
			key := fmt.Sprintf("key%08d", j)
			writer.Add(Entry{Key: []byte(key), Value: StringValue("value"), Sequence: uint64(j)})
		}

		writer.Finish(0)
		writer.Close()
	}
}

func BenchmarkSSTableGet(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := DefaultOptions(dir)

	writer, _ := NewSSTableWriter(1, path, 10000, opts)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		writer.Add(Entry{Key: []byte(key), Value: StringValue("value"), Sequence: uint64(i)})
	}
	writer.Finish(0)
	writer.Close()

	sst, _ := OpenSSTable(1, path)
	defer sst.Close()

	cache := NewLRUCache(64 * 1024 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%10000)
		sst.Get([]byte(key), cache, false)
	}
}

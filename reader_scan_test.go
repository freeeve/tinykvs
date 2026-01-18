package tinykvs

import (
	"fmt"
	"testing"
)

func TestPrefixScanAcrossBlocks(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 256 // Small blocks to span multiple

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write enough entries to span multiple blocks
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("prefix_%04d", i)
		store.PutString([]byte(key), fmt.Sprintf("value_%04d", i))
	}
	// Add some non-matching entries
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("other_%04d", i)
		store.PutString([]byte(key), "other")
	}
	store.Flush()

	// Scan prefix that spans blocks
	count := 0
	store.ScanPrefix([]byte("prefix_"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 100 {
		t.Errorf("prefix_ scan got %d, want 100", count)
	}

	// Scan prefix in the middle
	count = 0
	store.ScanPrefix([]byte("prefix_005"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 10 { // 0050-0059
		t.Errorf("prefix_005 scan got %d, want 10", count)
	}
}

func TestPrefixScanBlockBoundary(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 128 // Very small blocks

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create entries that will likely split across blocks
	entries := []string{
		"aaa", "aab", "aac",
		"bbb", "bbc", "bbd",
		"ccc", "ccd", "cce",
	}
	for _, e := range entries {
		store.PutString([]byte(e), "value-"+e)
	}
	store.Flush()

	// Test prefix scan starting in middle
	count := 0
	store.ScanPrefix([]byte("bb"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 3 {
		t.Errorf("bb prefix got %d, want 3", count)
	}
}

func TestPrefixScanEarlyStop(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write entries
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("scan_%04d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Stop after 5 entries
	count := 0
	store.ScanPrefix([]byte("scan_"), func(key []byte, value Value) bool {
		count++
		return count < 5
	})
	if count != 5 {
		t.Errorf("early stop got %d, want 5", count)
	}
}

func TestPrefixScanEarlyStopFromSSTable(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write entries and flush to SSTable
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("sst_%04d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Clear memtable by closing and reopening
	store.Close()
	store, _ = Open(dir, DefaultOptions(dir))
	defer store.Close()

	// Now scan from SSTable only, stop after 3 entries
	// This exercises sstablePrefixSource.close() when heap isn't empty
	count := 0
	store.ScanPrefix([]byte("sst_"), func(key []byte, value Value) bool {
		count++
		return count < 3
	})
	if count != 3 {
		t.Errorf("early stop from SSTable got %d, want 3", count)
	}
}

func TestPrefixScanWithTombstones(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write entries
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("tomb_%04d", i)
		store.PutString([]byte(key), "value")
	}
	// Delete half
	for i := 0; i < 20; i += 2 {
		key := fmt.Sprintf("tomb_%04d", i)
		store.Delete([]byte(key))
	}
	store.Flush()

	// Scan should only return non-deleted entries
	count := 0
	store.ScanPrefix([]byte("tomb_"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 10 {
		t.Errorf("tombstone scan got %d, want 10", count)
	}
}

func TestPrefixScanBeforeAllKeys(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write keys with prefix "zzz"
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("zzz_%04d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Search for prefix "aaa" which is before all keys
	// and minKey (zzz_0000) does NOT have the prefix "aaa"
	count := 0
	store.ScanPrefix([]byte("aaa"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("aaa prefix got %d, want 0", count)
	}
}

func TestPrefixScanNotFoundInBlock(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 128 // Small blocks
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write keys with gaps
	store.PutString([]byte("aaa_001"), "value")
	store.PutString([]byte("aaa_002"), "value")
	store.PutString([]byte("ccc_001"), "value")
	store.PutString([]byte("ccc_002"), "value")
	store.Flush()

	// Search for prefix "bbb" - not found in first block, try next
	count := 0
	store.ScanPrefix([]byte("bbb"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("bbb prefix got %d, want 0", count)
	}
}

func TestPrefixScanPrefixNotInBlock(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 64 // Very small blocks to force key distribution
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write entries that will span blocks with a gap in the middle
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("aa%02d", i)
		store.PutString([]byte(key), "value")
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("zz%02d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Search for prefix "mm" which is between aa and zz blocks
	count := 0
	store.ScanPrefix([]byte("mm"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("mm prefix got %d, want 0", count)
	}
}

// TestScanPrefixBlockSkipBug tests for the bug where seekToPrefix could skip blocks
// when the prefix isn't found in the first block.
// Bug: seekToPrefix calls blockIdx++ then next(), but next() also does blockIdx++,
// causing a block to be skipped.
func TestScanPrefixBlockSkipBug(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 128 // Very small blocks to force multiple blocks

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys in sorted order:
	// Block 0: keys starting with "aaa" (before prefix "mmm")
	// Block 1: keys starting with "mmm" (the prefix we'll search)
	// Block 2: keys starting with "zzz" (after prefix "mmm")
	for i := 0; i < 10; i++ {
		store.PutString([]byte(fmt.Sprintf("aaa%02d", i)), "before-value")
	}
	for i := 0; i < 10; i++ {
		store.PutString([]byte(fmt.Sprintf("mmm%02d", i)), "target-value")
	}
	for i := 0; i < 10; i++ {
		store.PutString([]byte(fmt.Sprintf("zzz%02d", i)), "after-value")
	}
	store.Flush()

	// Scan for prefix "mmm" - should find all 10 keys
	// With the bug, block 1 would be skipped and we'd get 0 keys (or only partial)
	count := 0
	var foundKeys []string
	err = store.ScanPrefix([]byte("mmm"), func(key []byte, value Value) bool {
		count++
		foundKeys = append(foundKeys, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 10 {
		t.Errorf("count = %d, want 10. Found keys: %v", count, foundKeys)
	}

	// Verify we found the right keys
	for i := 0; i < 10; i++ {
		expected := fmt.Sprintf("mmm%02d", i)
		found := false
		for _, k := range foundKeys {
			if k == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Missing expected key: %s", expected)
		}
	}
}

// TestScanPrefixNonMatchingKeysFromSource tests that non-matching keys
// don't get pushed to the heap after a matching key.
func TestScanPrefixNonMatchingKeysFromSource(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 64 // Very small blocks

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Create data that spans multiple blocks with mixed prefixes
	// This tests that when we exhaust matching keys in a block,
	// we don't accidentally return non-matching keys from the next block
	for i := 0; i < 20; i++ {
		store.PutString([]byte(fmt.Sprintf("aa%02d", i)), "value")
	}
	for i := 0; i < 20; i++ {
		store.PutString([]byte(fmt.Sprintf("bb%02d", i)), "value")
	}
	for i := 0; i < 20; i++ {
		store.PutString([]byte(fmt.Sprintf("cc%02d", i)), "value")
	}
	store.Flush()

	// Scan for prefix "bb" - should get exactly 20 keys
	count := 0
	var foundKeys []string
	err = store.ScanPrefix([]byte("bb"), func(key []byte, value Value) bool {
		count++
		foundKeys = append(foundKeys, string(key))
		// Verify this key actually has the prefix
		if !hasPrefix(key, []byte("bb")) {
			t.Errorf("Got non-matching key: %s", string(key))
		}
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 20 {
		t.Errorf("count = %d, want 20. Found keys: %v", count, foundKeys)
	}
}

func TestScanRangeMultipleSSTables(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4 * 1024 // 4KB to force multiple flushes

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys in multiple batches to create multiple SSTables
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("batch_%d_key_%03d", batch, i)
			store.PutInt64([]byte(key), int64(batch*100+i))
		}
		store.Flush()
	}

	// Test: range scan across multiple SSTables
	count := 0
	var keys []string
	store.ScanRange([]byte("batch_0"), []byte("batch_3"), func(key []byte, val Value) bool {
		count++
		keys = append(keys, string(key))
		return true
	})
	if count != 150 {
		t.Errorf("ScanRange across SSTables got %d keys, want 150", count)
	}

	// Verify ordering
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Errorf("Keys not sorted: %s >= %s", keys[i-1], keys[i])
			break
		}
	}

	// Test: range that spans partial batches
	count = 0
	store.ScanRange([]byte("batch_1_key_025"), []byte("batch_2_key_025"), func(key []byte, val Value) bool {
		count++
		return true
	})
	// Should get batch_1_key_025 to batch_1_key_049 (25 keys) + batch_2_key_000 to batch_2_key_024 (25 keys) = 50 keys
	if count != 50 {
		t.Errorf("ScanRange partial batches got %d keys, want 50", count)
	}
}

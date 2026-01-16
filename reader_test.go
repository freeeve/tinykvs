package tinykvs

import (
	"fmt"
	"testing"
)

func TestHasKeyInRange(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		minKey []byte
		maxKey []byte
		want   bool
	}{
		{
			name:   "prefix within range",
			prefix: []byte("key1"),
			minKey: []byte("key0"),
			maxKey: []byte("key2"),
			want:   true,
		},
		{
			name:   "prefix equals minKey",
			prefix: []byte("key"),
			minKey: []byte("key"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "prefix before minKey",
			prefix: []byte("aaa"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   false,
		},
		{
			name:   "prefix after maxKey",
			prefix: []byte("zzz"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   false,
		},
		{
			name:   "prefix longer than maxKey - match",
			prefix: []byte("key123"),
			minKey: []byte("key"),
			maxKey: []byte("key2"),
			want:   true,
		},
		{
			name:   "prefix longer than maxKey - no match",
			prefix: []byte("key999"),
			minKey: []byte("key0"),
			maxKey: []byte("key1"),
			want:   false,
		},
		{
			name:   "maxKey shorter and prefix greater",
			prefix: []byte("zzz"),
			minKey: []byte("a"),
			maxKey: []byte("b"),
			want:   false,
		},
		{
			name:   "empty prefix matches all",
			prefix: []byte{},
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "minKey longer than prefix - match",
			prefix: []byte("key"),
			minKey: []byte("key123"),
			maxKey: []byte("key999"),
			want:   true,
		},
		{
			name:   "minKey longer than prefix - no match",
			prefix: []byte("aaa"),
			minKey: []byte("bbb123"),
			maxKey: []byte("ccc999"),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasKeyInRange(tt.prefix, tt.minKey, tt.maxKey)
			if got != tt.want {
				t.Errorf("hasKeyInRange(%q, %q, %q) = %v, want %v",
					tt.prefix, tt.minKey, tt.maxKey, got, tt.want)
			}
		})
	}
}

func TestScanPrefixEmpty(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Scan empty store
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixStopEarly(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write 100 keys
	for i := 0; i < 100; i++ {
		store.PutInt64([]byte("key"+string(rune('0'+i/10))+string(rune('0'+i%10))), int64(i))
	}
	store.Flush()

	// Stop after 5 keys
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return count < 5
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

func TestScanPrefixWithTombstones(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys then delete some
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("key"+string(rune('0'+i))), int64(i))
	}
	store.Delete([]byte("key3"))
	store.Delete([]byte("key7"))
	store.Flush()

	// Scan should skip tombstones
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 8 {
		t.Errorf("count = %d, want 8 (10 - 2 deleted)", count)
	}
}

func TestScanPrefixNoMatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys with different prefix
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("abc"+string(rune('0'+i))), int64(i))
	}
	store.Flush()

	// Scan with non-matching prefix
	count := 0
	err = store.ScanPrefix([]byte("xyz"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixClosed(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	store.Close()

	// Scan closed store should return error
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		return true
	})
	if err != ErrStoreClosed {
		t.Errorf("ScanPrefix on closed store = %v, want ErrStoreClosed", err)
	}
}

func TestScanPrefixMultipleSSTables(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Very small to force multiple SSTables
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write enough data to create multiple SSTables
	for i := 0; i < 500; i++ {
		key := []byte("key" + string(rune('0'+i/100)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i%10)))
		store.PutInt64(key, int64(i))
		if i%50 == 49 {
			store.Flush()
		}
	}
	store.Flush()

	// Scan all keys
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 500 {
		t.Errorf("count = %d, want 500", count)
	}

	// Scan subset with specific prefix
	count = 0
	err = store.ScanPrefix([]byte("key1"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix key1 failed: %v", err)
	}
	if count != 100 {
		t.Errorf("count for key1 = %d, want 100", count)
	}
}

func TestScanPrefixAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write data
	for i := 0; i < 200; i++ {
		key := []byte("key" + string(rune('0'+i/100)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i%10)))
		store.PutInt64(key, int64(i))
	}
	store.Flush()

	// Compact
	store.Compact()

	// Scan should still work after compaction
	count := 0
	err = store.ScanPrefix([]byte("key0"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix after compact failed: %v", err)
	}
	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
}

func TestScanPrefixPrefixBeforeAllKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys starting with 'm'
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("mkey"+string(rune('0'+i))), int64(i))
	}
	store.Flush()

	// Scan with prefix 'a' which is before all keys
	count := 0
	err = store.ScanPrefix([]byte("a"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixPrefixAfterAllKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys starting with 'a'
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("akey"+string(rune('0'+i))), int64(i))
	}
	store.Flush()

	// Scan with prefix 'z' which is after all keys
	count := 0
	err = store.ScanPrefix([]byte("z"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixSpansMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 256 // Very small blocks
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys that will span multiple blocks
	for i := 0; i < 100; i++ {
		key := []byte("prefix" + string(rune('0'+i/10)) + string(rune('0'+i%10)))
		store.PutString(key, "value-that-takes-some-space-in-block")
	}
	store.Flush()

	// Scan subset
	count := 0
	err = store.ScanPrefix([]byte("prefix5"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 10 {
		t.Errorf("count = %d, want 10", count)
	}
}

func TestScanPrefixExactMatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	store.PutString([]byte("exact"), "value1")
	store.PutString([]byte("exactmatch"), "value2")
	store.PutString([]byte("other"), "value3")
	store.Flush()

	// Scan with exact key as prefix
	count := 0
	err = store.ScanPrefix([]byte("exact"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2 (exact and exactmatch)", count)
	}
}

func TestScanPrefixEmptyStore(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Empty prefix on empty store
	count := 0
	err = store.ScanPrefix([]byte{}, func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixWithCacheDisabled(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockCacheSize = 0 // No cache
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	for i := 0; i < 50; i++ {
		store.PutString([]byte("key"+string(rune('0'+i/10))+string(rune('0'+i%10))), "value")
	}
	store.Flush()

	count := 0
	err = store.ScanPrefix([]byte("key2"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 10 {
		t.Errorf("count = %d, want 10", count)
	}
}

func TestScanPrefixMemtableAndSSTableMerge(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write some to SSTable
	for i := 0; i < 10; i++ {
		store.PutString([]byte("key"+string(rune('0'+i))), "sstable")
	}
	store.Flush()

	// Write more to memtable (overlapping)
	for i := 5; i < 15; i++ {
		store.PutString([]byte("key"+string(rune('0'+i%10))), "memtable")
	}

	// Scan - should see merged results with memtable taking precedence
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	// Should see keys 0-9 from SSTable + keys 5-14 from memtable = unique keys 0-14 = 15 keys
	// Wait, memtable has keys 5-14 which is key5 through key14, but key14 is actually "key4" since i%10
	// Let me recalculate: memtable has keys for i=5..14, which is "key5", "key6", "key7", "key8", "key9", "key0", "key1", "key2", "key3", "key4"
	// SSTable has keys "key0" through "key9"
	// Unique keys: key0-key9 = 10 keys
	if count != 10 {
		t.Errorf("count = %d, want 10", count)
	}
}

func TestScanPrefixPrefixBeforeIndexMinKey(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys: bbb00, bbb01, ..., bbb19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("bbb%02d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Scan with prefix 'b' - should find all keys since they all start with 'b'
	count := 0
	err = store.ScanPrefix([]byte("b"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 20 {
		t.Errorf("count = %d, want 20", count)
	}

	// Scan with prefix 'bb' - should find all keys
	count = 0
	err = store.ScanPrefix([]byte("bb"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 20 {
		t.Errorf("count for 'bb' = %d, want 20", count)
	}

	// Scan with prefix 'bbb' - should find all keys
	count = 0
	err = store.ScanPrefix([]byte("bbb"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 20 {
		t.Errorf("count for 'bbb' = %d, want 20", count)
	}

	// Scan with prefix 'bbb0' - should find bbb00-bbb09 (10 keys)
	count = 0
	err = store.ScanPrefix([]byte("bbb0"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 10 {
		t.Errorf("count for 'bbb0' = %d, want 10", count)
	}
}

func TestScanPrefixNextBlockTransition(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 128 // Very small blocks to force transitions
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys that will span multiple blocks
	for i := 0; i < 100; i++ {
		key := []byte("prefix" + string(rune('0'+i/10)) + string(rune('0'+i%10)))
		store.PutString(key, "value-that-takes-space")
	}
	store.Flush()

	// Scan prefix that spans multiple blocks
	count := 0
	var lastKey []byte
	err = store.ScanPrefix([]byte("prefix"), func(key []byte, value Value) bool {
		count++
		if lastKey != nil && CompareKeys(key, lastKey) <= 0 {
			t.Errorf("Keys not in order: %s came after %s", key, lastKey)
		}
		lastKey = append([]byte{}, key...)
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
}

func TestScanPrefixEntryNotInFirstPosition(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 512 // Medium blocks
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys with gaps
	store.PutString([]byte("aaa"), "value")
	store.PutString([]byte("bbb"), "value")
	store.PutString([]byte("ccc"), "value")
	store.PutString([]byte("ddd"), "value")
	store.PutString([]byte("zzz"), "value")
	store.Flush()

	// Scan for prefix that's after the first entry in its block
	count := 0
	err = store.ScanPrefix([]byte("ddd"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	// Scan for prefix that has no matches
	count = 0
	err = store.ScanPrefix([]byte("eee"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count for 'eee' = %d, want 0", count)
	}
}

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

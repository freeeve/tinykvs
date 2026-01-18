package tinykvs

import (
	"fmt"
	"testing"
)

func TestStoreUpdateAcrossLevels(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Small memtable to trigger frequent flushes

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Phase 1: Write initial values and flush to L0
	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, key := range keys {
		store.PutString([]byte(key), "version1")
	}
	store.Flush()

	// Verify initial values
	for _, key := range keys {
		val, err := store.GetString([]byte(key))
		if err != nil || val != "version1" {
			t.Errorf("Get(%s) = %q, want version1", key, val)
		}
	}

	// Phase 2: Compact L0 to L1
	store.Compact()

	// Verify values still correct after compaction
	for _, key := range keys {
		val, err := store.GetString([]byte(key))
		if err != nil || val != "version1" {
			t.Errorf("After compact, Get(%s) = %q, want version1", key, val)
		}
	}

	// Phase 3: Update some keys (new values go to memtable)
	store.PutString([]byte("alpha"), "version2")
	store.PutString([]byte("gamma"), "version2")

	// Verify updates are visible (memtable shadows L1)
	if val, _ := store.GetString([]byte("alpha")); val != "version2" {
		t.Errorf("Get(alpha) = %q, want version2", val)
	}
	if val, _ := store.GetString([]byte("gamma")); val != "version2" {
		t.Errorf("Get(gamma) = %q, want version2", val)
	}
	// Unchanged keys still return old value
	if val, _ := store.GetString([]byte("beta")); val != "version1" {
		t.Errorf("Get(beta) = %q, want version1", val)
	}

	// Phase 4: Flush updates to L0
	store.Flush()

	// Verify values still correct
	if val, _ := store.GetString([]byte("alpha")); val != "version2" {
		t.Errorf("After flush, Get(alpha) = %q, want version2", val)
	}
	if val, _ := store.GetString([]byte("beta")); val != "version1" {
		t.Errorf("After flush, Get(beta) = %q, want version1", val)
	}

	// Phase 5: Update again and compact
	store.PutString([]byte("alpha"), "version3")
	store.PutString([]byte("beta"), "version3")
	store.Flush()
	store.Compact()

	// Final verification
	expected := map[string]string{
		"alpha": "version3",
		"beta":  "version3",
		"gamma": "version2",
		"delta": "version1",
	}
	for key, want := range expected {
		got, err := store.GetString([]byte(key))
		if err != nil || got != want {
			t.Errorf("Final Get(%s) = %q, want %q", key, got, want)
		}
	}

	store.Close()

	// Phase 6: Reopen and verify persistence
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	for key, want := range expected {
		got, err := store.GetString([]byte(key))
		if err != nil || got != want {
			t.Errorf("After reopen, Get(%s) = %q, want %q", key, got, want)
		}
	}
}

func TestStoreDeleteAcrossLevels(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write and flush to L0, then compact to L1
	store.PutString([]byte("key1"), "value1")
	store.PutString([]byte("key2"), "value2")
	store.PutString([]byte("key3"), "value3")
	store.Flush()
	store.Compact()

	// Delete key2 (tombstone goes to memtable)
	store.Delete([]byte("key2"))

	// Verify delete is visible
	_, err = store.Get([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Errorf("Get(key2) after delete: got %v, want ErrKeyNotFound", err)
	}

	// key1 and key3 still exist
	if _, err := store.Get([]byte("key1")); err != nil {
		t.Errorf("Get(key1): %v", err)
	}
	if _, err := store.Get([]byte("key3")); err != nil {
		t.Errorf("Get(key3): %v", err)
	}

	// Flush and compact (tombstone merges with L1)
	store.Flush()
	store.Compact()

	// Delete should persist
	_, err = store.Get([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Errorf("After compact, Get(key2): got %v, want ErrKeyNotFound", err)
	}

	store.Close()

	// Reopen and verify
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	_, err = store.Get([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Errorf("After reopen, Get(key2): got %v, want ErrKeyNotFound", err)
	}
}

func TestStoreMultipleUpdates(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.MemtableSize = 512 // Very small to trigger many flushes

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Repeatedly update the same key with flushes in between
	key := []byte("counter")
	for i := 1; i <= 10; i++ {
		store.PutInt64(key, int64(i))
		store.Flush()

		got, err := store.GetInt64(key)
		if err != nil || got != int64(i) {
			t.Errorf("After update %d: got %d, want %d", i, got, i)
		}
	}

	// Compact everything
	store.Compact()

	// Final value should be 10
	got, err := store.GetInt64(key)
	if err != nil || got != 10 {
		t.Errorf("After compact: got %d, want 10", got)
	}
}

func TestStoreReadFromL1(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Small memtable

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough data to trigger flush and create L0 tables
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Compact to move data to L1 (non-overlapping, uses binary search)
	store.Compact()

	// Read keys - this exercises findTableForKey binary search
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.GetString([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) from L1 failed: %v", key, err)
		}
		if val != "value" {
			t.Errorf("Get(%s) = %q, want value", key, val)
		}
	}

	// Read non-existent keys (exercises binary search miss paths)
	_, err = store.Get([]byte("aaa"))
	if err != ErrKeyNotFound {
		t.Errorf("Get(aaa) should be ErrKeyNotFound, got %v", err)
	}
	_, err = store.Get([]byte("zzz"))
	if err != ErrKeyNotFound {
		t.Errorf("Get(zzz) should be ErrKeyNotFound, got %v", err)
	}
	_, err = store.Get([]byte("key00050x"))
	if err != ErrKeyNotFound {
		t.Errorf("Get(key00050x) should be ErrKeyNotFound, got %v", err)
	}

	store.Close()
}

func TestStoreCompactWithTombstones(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.MemtableSize = 512

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%03d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Delete some keys
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%03d", i)
		store.Delete([]byte(key))
	}
	store.Flush()

	// Compact - tombstones should be preserved (not at last level)
	store.Compact()

	// Deleted keys should still return ErrKeyNotFound
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%03d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) should be deleted, got %v", key, err)
		}
	}

	// Non-deleted keys should still exist
	for i := 25; i < 50; i++ {
		key := fmt.Sprintf("key%03d", i)
		_, err := store.GetString([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		}
	}

	store.Close()
}

func TestReaderAddSSTableLevelExpansion(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	// Create a reader with no levels
	mt := newMemtable()
	cache := newLRUCache(1024 * 1024)
	reader := newReader(mt, nil, cache, opts)

	// Create a test SSTable
	path := dir + "/test.sst"
	writer, err := newSSTableWriter(1, path, 10, opts, true)
	if err != nil {
		t.Fatalf("newSSTableWriter failed: %v", err)
	}
	writer.Add(Entry{Key: []byte("key"), Value: Int64Value(42), Sequence: 1})
	writer.Finish(0)
	writer.Close()

	sst, err := OpenSSTable(1, path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	defer sst.Close()

	// Add to level 5 (should expand the levels slice)
	reader.AddSSTable(5, sst)

	levels := reader.GetLevels()
	if len(levels) < 6 {
		t.Errorf("levels should have expanded to at least 6, got %d", len(levels))
	}
	if len(levels[5]) != 1 {
		t.Errorf("level 5 should have 1 table, got %d", len(levels[5]))
	}
}

func TestReaderImmutableMemtable(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	mt := newMemtable()
	cache := newLRUCache(1024 * 1024)
	reader := newReader(mt, nil, cache, opts)

	// Create an immutable memtable with a key
	imm := newMemtable()
	imm.Put([]byte("immkey"), StringValue("immvalue"), 1)
	reader.AddImmutable(imm)

	// Should find the key in immutable
	val, err := reader.Get([]byte("immkey"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.String() != "immvalue" {
		t.Errorf("value = %s, want immvalue", val.String())
	}

	// Test tombstone in immutable
	imm2 := newMemtable()
	imm2.Put([]byte("tombkey"), TombstoneValue(), 2)
	reader.AddImmutable(imm2)

	_, err = reader.Get([]byte("tombkey"))
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound for tombstone in immutable, got %v", err)
	}
}

func TestMemtableIteratorExhaustion(t *testing.T) {
	mt := newMemtable()
	mt.Put([]byte("a"), Int64Value(1), 1)

	iter := mt.Iterator()
	defer iter.Close()

	// Advance to first entry
	if !iter.Next() {
		t.Fatal("Next() should return true")
	}
	if string(iter.Key()) != "a" {
		t.Errorf("key = %s, want a", iter.Key())
	}

	// Exhaust the iterator
	if iter.Next() {
		t.Error("Next() should return false after exhaustion")
	}

	// After exhaustion, current becomes nil
	// These should return safe defaults
	if iter.Key() != nil {
		t.Error("Key() after exhaustion should be nil")
	}
	if iter.Value().Type != 0 {
		t.Error("Value() after exhaustion should be zero")
	}
	if iter.Entry().Key != nil {
		t.Error("Entry() after exhaustion should be zero")
	}
	if iter.Valid() {
		t.Error("Valid() after exhaustion should be false")
	}

	// Calling Next() again should still return false
	if iter.Next() {
		t.Error("Next() should return false when already exhausted")
	}
}

func TestFindKey(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 64 * 1024 // 64KB memtable

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Key in memtable should return nil
	store.PutString([]byte("memtable_key"), "value")
	loc := store.FindKey([]byte("memtable_key"))
	if loc != nil {
		t.Errorf("FindKey for memtable key = %+v, want nil", loc)
	}

	// Add keys and flush to create SSTable
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("sstable_key_%03d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Key in SSTable should return location with valid level
	loc = store.FindKey([]byte("sstable_key_050"))
	if loc == nil {
		t.Fatal("FindKey for SSTable key returned nil")
	}
	// Level could be 0 or 1 depending on compaction timing
	if loc.Level < 0 || loc.Level > 1 {
		t.Errorf("FindKey level = %d, want 0 or 1", loc.Level)
	}
	if loc.TableID == 0 {
		t.Error("FindKey TableID should not be 0")
	}

	// Non-existent key should return nil
	loc = store.FindKey([]byte("nonexistent_key"))
	if loc != nil {
		t.Errorf("FindKey for nonexistent key = %+v, want nil", loc)
	}

	// Compact and verify key is still findable
	store.Compact()
	loc = store.FindKey([]byte("sstable_key_050"))
	if loc == nil {
		t.Fatal("FindKey after compaction returned nil")
	}
}

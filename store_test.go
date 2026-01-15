package tinykvs

import (
	"fmt"
	"sync"
	"testing"
)

func TestStoreBasicCRUD(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Put
	if err := store.PutString([]byte("key1"), "value1"); err != nil {
		t.Fatalf("PutString failed: %v", err)
	}
	if err := store.PutInt64([]byte("key2"), 42); err != nil {
		t.Fatalf("PutInt64 failed: %v", err)
	}

	// Get
	val1, err := store.GetString([]byte("key1"))
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if val1 != "value1" {
		t.Errorf("GetString = %q, want %q", val1, "value1")
	}

	val2, err := store.GetInt64([]byte("key2"))
	if err != nil {
		t.Fatalf("GetInt64 failed: %v", err)
	}
	if val2 != 42 {
		t.Errorf("GetInt64 = %d, want 42", val2)
	}

	// Not found
	_, err = store.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Get(nonexistent) = %v, want ErrKeyNotFound", err)
	}

	// Delete
	if err := store.Delete([]byte("key1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = store.Get([]byte("key1"))
	if err != ErrKeyNotFound {
		t.Errorf("Get after delete = %v, want ErrKeyNotFound", err)
	}
}

func TestStoreAllValueTypes(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Float64
	if err := store.PutFloat64([]byte("float"), 3.14159); err != nil {
		t.Fatalf("PutFloat64 failed: %v", err)
	}
	floatVal, err := store.GetFloat64([]byte("float"))
	if err != nil {
		t.Fatalf("GetFloat64 failed: %v", err)
	}
	if floatVal != 3.14159 {
		t.Errorf("GetFloat64 = %v, want 3.14159", floatVal)
	}

	// Bool
	if err := store.PutBool([]byte("bool_true"), true); err != nil {
		t.Fatalf("PutBool failed: %v", err)
	}
	if err := store.PutBool([]byte("bool_false"), false); err != nil {
		t.Fatalf("PutBool failed: %v", err)
	}
	boolVal, err := store.GetBool([]byte("bool_true"))
	if err != nil {
		t.Fatalf("GetBool failed: %v", err)
	}
	if boolVal != true {
		t.Errorf("GetBool = %v, want true", boolVal)
	}
	boolVal, err = store.GetBool([]byte("bool_false"))
	if err != nil {
		t.Fatalf("GetBool failed: %v", err)
	}
	if boolVal != false {
		t.Errorf("GetBool = %v, want false", boolVal)
	}

	// Bytes
	testBytes := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	if err := store.PutBytes([]byte("bytes"), testBytes); err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}
	bytesVal, err := store.GetBytes([]byte("bytes"))
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if string(bytesVal) != string(testBytes) {
		t.Errorf("GetBytes = %v, want %v", bytesVal, testBytes)
	}

	// Test Value constructors
	v := Int64Value(123)
	if v.Type != ValueTypeInt64 || v.Int64 != 123 {
		t.Errorf("Int64Value = %+v, want Int64=123", v)
	}

	v = Float64Value(2.718)
	if v.Type != ValueTypeFloat64 || v.Float64 != 2.718 {
		t.Errorf("Float64Value = %+v, want Float64=2.718", v)
	}

	v = BoolValue(true)
	if v.Type != ValueTypeBool || v.Bool != true {
		t.Errorf("BoolValue = %+v, want Bool=true", v)
	}

	v = BytesValue([]byte("test"))
	if v.Type != ValueTypeBytes || string(v.Bytes) != "test" {
		t.Errorf("BytesValue = %+v, want Bytes=test", v)
	}
}

func TestOptionsPresets(t *testing.T) {
	dir := t.TempDir()

	// Test LowMemoryOptions
	lowOpts := LowMemoryOptions(dir)
	if lowOpts.MemtableSize != 1*1024*1024 {
		t.Errorf("LowMemoryOptions.MemtableSize = %d, want 1MB", lowOpts.MemtableSize)
	}
	if lowOpts.BlockCacheSize != 0 {
		t.Errorf("LowMemoryOptions.BlockCacheSize = %d, want 0", lowOpts.BlockCacheSize)
	}

	// Test HighPerformanceOptions
	highOpts := HighPerformanceOptions(dir)
	if highOpts.MemtableSize != 64*1024*1024 {
		t.Errorf("HighPerformanceOptions.MemtableSize = %d, want 64MB", highOpts.MemtableSize)
	}
	if highOpts.BlockCacheSize != 512*1024*1024 {
		t.Errorf("HighPerformanceOptions.BlockCacheSize = %d, want 512MB", highOpts.BlockCacheSize)
	}

	// Verify stores can be opened with each preset
	store1, err := Open(dir+"/low", lowOpts)
	if err != nil {
		t.Fatalf("Open with LowMemoryOptions failed: %v", err)
	}
	store1.Close()

	store2, err := Open(dir+"/high", highOpts)
	if err != nil {
		t.Fatalf("Open with HighPerformanceOptions failed: %v", err)
	}
	store2.Close()
}

func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()

	// Write data
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	store.PutString([]byte("persistent"), "data")
	store.Flush()
	store.Close()

	// Reopen and verify
	store, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	val, err := store.GetString([]byte("persistent"))
	if err != nil {
		t.Fatalf("GetString after reopen failed: %v", err)
	}
	if val != "data" {
		t.Errorf("value = %q, want %q", val, "data")
	}
}

func TestStoreWALRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write data but don't flush (simulate crash before flush)
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	store.PutString([]byte("recovered"), "from-wal")
	// Don't call Flush() - data is only in WAL
	store.Close()

	// Reopen - should recover from WAL
	store, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	val, err := store.GetString([]byte("recovered"))
	if err != nil {
		t.Fatalf("GetString after recovery failed: %v", err)
	}
	if val != "from-wal" {
		t.Errorf("value = %q, want %q", val, "from-wal")
	}
}

func TestStoreLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 64 * 1024 // 64KB to trigger flushes

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Insert many keys
	n := 10000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Verify random access
	for i := 0; i < 100; i++ {
		idx := (i * 97) % n // Pseudo-random access
		key := fmt.Sprintf("key%06d", idx)
		expected := fmt.Sprintf("value%06d", idx)

		val, err := store.GetString([]byte(key))
		if err != nil {
			t.Fatalf("Get %s failed: %v", key, err)
		}
		if val != expected {
			t.Errorf("Get(%s) = %s, want %s", key, val, expected)
		}
	}
}

func TestStoreConcurrentReads(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-populate
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		store.PutInt64([]byte(key), int64(i))
	}
	store.Flush()

	// Concurrent reads
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("key%03d", i)
				val, err := store.GetInt64([]byte(key))
				if err != nil {
					t.Errorf("Get(%s) failed: %v", key, err)
					return
				}
				if val != int64(i) {
					t.Errorf("Get(%s) = %d, want %d", key, val, i)
				}
			}
		}()
	}
	wg.Wait()
}

func TestStoreConcurrentWrites(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Concurrent writes (via single goroutine due to write mutex)
	// But we can test that writes don't block reads
	var wg sync.WaitGroup

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("wkey%03d", i)
			store.PutInt64([]byte(key), int64(i))
		}
	}()

	// Reader (may see partial data, which is fine)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("wkey%03d", i)
			store.Get([]byte(key)) // Don't check result, just ensure no panic
		}
	}()

	wg.Wait()
}

func TestStoreStats(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Initial stats
	stats := store.Stats()
	if stats.MemtableCount != 0 {
		t.Errorf("initial memtable count = %d, want 0", stats.MemtableCount)
	}

	// After some writes
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte(fmt.Sprintf("key%d", i)), int64(i))
	}

	stats = store.Stats()
	if stats.MemtableCount != 10 {
		t.Errorf("memtable count = %d, want 10", stats.MemtableCount)
	}
}

func TestStoreLowMemoryOptions(t *testing.T) {
	dir := t.TempDir()

	opts := LowMemoryOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Should work with minimal memory
	store.PutString([]byte("key"), "value")
	val, err := store.GetString([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "value" {
		t.Errorf("value = %s, want value", val)
	}
}

func TestStoreClosedOperations(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	store.Close()

	// All operations should fail
	if _, err := store.Get([]byte("key")); err != ErrStoreClosed {
		t.Errorf("Get on closed store: %v, want ErrStoreClosed", err)
	}
	if err := store.Put([]byte("key"), StringValue("value")); err != ErrStoreClosed {
		t.Errorf("Put on closed store: %v, want ErrStoreClosed", err)
	}
	if err := store.Delete([]byte("key")); err != ErrStoreClosed {
		t.Errorf("Delete on closed store: %v, want ErrStoreClosed", err)
	}
}

func TestStoreFlushTrigger(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // 1KB to trigger flush quickly

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write enough to trigger flush
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("this is a longer value to fill up memtable %03d", i)
		store.PutString([]byte(key), value)
	}

	// Force flush
	store.Flush()

	// Check stats - should have some data in levels
	stats := store.Stats()

	// Either memtable has data or L0 has tables
	totalKeys := stats.MemtableCount
	for _, level := range stats.Levels {
		totalKeys += int64(level.NumKeys)
	}

	if totalKeys < 100 {
		t.Errorf("total keys = %d, want >= 100", totalKeys)
	}

	// Verify data is readable
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		_, err := store.GetString([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) after flush: %v", key, err)
		}
	}
}

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

func BenchmarkStorePut(b *testing.B) {
	dir := b.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	value := StringValue("benchmark value that is reasonably sized")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.Put([]byte(key), value)
	}
}

func BenchmarkStoreGet(b *testing.B) {
	dir := b.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-populate
	n := 10000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%n)
		store.Get([]byte(key))
	}
}

func BenchmarkStoreMixed(b *testing.B) {
	dir := b.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% writes
			key := fmt.Sprintf("key%08d", i)
			store.PutString([]byte(key), "new value")
		} else {
			// 80% reads
			key := fmt.Sprintf("key%08d", i%1000)
			store.Get([]byte(key))
		}
	}
}

func BenchmarkCompaction(b *testing.B) {
	for _, numKeys := range []int{10000, 50000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				opts := DefaultOptions(dir)
				opts.MemtableSize = 64 * 1024 // 64KB to create multiple L0 tables

				store, err := Open(dir, opts)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}

				// Write keys to create multiple L0 tables
				for j := 0; j < numKeys; j++ {
					key := fmt.Sprintf("key%08d", j)
					store.PutString([]byte(key), "value that is long enough to fill blocks quickly")
				}
				store.Flush()

				b.StartTimer()
				store.Compact()
				b.StopTimer()

				store.Close()
			}
		})
	}
}

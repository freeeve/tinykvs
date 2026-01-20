package tinykvs

import (
	"fmt"
	"os"
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
	if lowOpts.MemtableSize != 4*1024*1024 {
		t.Errorf("LowMemoryOptions.MemtableSize = %d, want 4MB", lowOpts.MemtableSize)
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

	// Should work with ultra low memory (no bloom, no cache)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Verify reads work without bloom filter
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		val, err := store.GetString([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		}
		if val != "value" {
			t.Errorf("Get(%s) = %s, want value", key, val)
		}
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

func TestStoreTypeMismatchErrors(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Store a string value
	store.PutString([]byte("str"), "hello")

	// Try to get it as wrong types
	_, err = store.GetInt64([]byte("str"))
	if err == nil {
		t.Error("GetInt64 on string should fail")
	}

	_, err = store.GetFloat64([]byte("str"))
	if err == nil {
		t.Error("GetFloat64 on string should fail")
	}

	_, err = store.GetBool([]byte("str"))
	if err == nil {
		t.Error("GetBool on string should fail")
	}
}

func TestStoreWALRecoveryWithDelete(t *testing.T) {
	dir := t.TempDir()

	// Write data including a delete
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	store.PutString([]byte("key1"), "value1")
	store.PutString([]byte("key2"), "value2")
	store.Delete([]byte("key1")) // Delete key1
	store.Close()                // Close without flush - data in WAL only

	// Reopen and verify delete was recovered
	store, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// key1 should be deleted
	_, err = store.Get([]byte("key1"))
	if err != ErrKeyNotFound {
		t.Errorf("key1 should be deleted, got err=%v", err)
	}

	// key2 should exist
	val, err := store.GetString([]byte("key2"))
	if err != nil || val != "value2" {
		t.Errorf("key2 = %q, err=%v, want value2", val, err)
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

// TestSync verifies that Sync() persists WAL data without creating SSTables.
func TestSync(t *testing.T) {
	dir := t.TempDir()

	// Write data and sync (but don't flush)
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write some data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("sync-key%03d", i)
		if err := store.PutString([]byte(key), fmt.Sprintf("value%03d", i)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Sync (should persist to WAL but not create SSTables)
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Check stats - data should be in memtable, not L0
	stats := store.Stats()
	if stats.MemtableCount != 100 {
		t.Errorf("memtable count = %d, want 100", stats.MemtableCount)
	}
	if len(stats.Levels) > 0 && stats.Levels[0].NumTables > 0 {
		t.Errorf("L0 has %d tables after Sync, want 0", stats.Levels[0].NumTables)
	}

	// Close without explicit flush
	store.Close()

	// Reopen - data should be recovered from WAL
	store, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Verify all data recovered
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("sync-key%03d", i)
		expected := fmt.Sprintf("value%03d", i)
		val, err := store.GetString([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) after recovery failed: %v", key, err)
			continue
		}
		if val != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val, expected)
		}
	}
}

// TestScanPrefixWithStats verifies that scan statistics are tracked correctly.
func TestScanPrefixWithStats(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4 * 1024 // Small memtable to force flushes

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Insert data with a prefix
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("stats:%05d", i)
		if err := store.PutString([]byte(key), fmt.Sprintf("value%05d", i)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create SSTables
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Add some more data to memtable
	for i := 500; i < 600; i++ {
		key := fmt.Sprintf("stats:%05d", i)
		if err := store.PutString([]byte(key), fmt.Sprintf("value%05d", i)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Scan with stats
	var count int
	var progressCalls int
	stats, err := store.ScanPrefixWithStats([]byte("stats:"), func(key []byte, value Value) bool {
		count++
		return true
	}, func(s ScanStats) bool {
		progressCalls++
		return true
	})

	if err != nil {
		t.Fatalf("ScanPrefixWithStats failed: %v", err)
	}

	if count != 600 {
		t.Errorf("scanned %d keys, want 600", count)
	}

	// Stats should show blocks loaded (from SSTables) and keys examined
	if stats.KeysExamined < int64(count) {
		t.Errorf("KeysExamined = %d, want >= %d", stats.KeysExamined, count)
	}

	t.Logf("Stats: BlocksLoaded=%d, KeysExamined=%d, ProgressCalls=%d",
		stats.BlocksLoaded, stats.KeysExamined, progressCalls)
}

// TestLoadSSTablesWithoutManifest tests the migration path when no manifest exists.
func TestLoadSSTablesWithoutManifest(t *testing.T) {
	dir := t.TempDir()

	// Create a store with some data and flush to create SSTables
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("migrate:%05d", i)
		if err := store.PutString([]byte(key), fmt.Sprintf("value%05d", i)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := store.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	store.Close()

	// Delete the manifest file to simulate migration from old format
	manifestPath := dir + "/MANIFEST"
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("Failed to remove manifest: %v", err)
	}

	// Reopen - should load SSTables by scanning directory
	store, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen without manifest failed: %v", err)
	}
	defer store.Close()

	// Verify data is still accessible
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("migrate:%05d", i)
		expected := fmt.Sprintf("value%05d", i)
		val, err := store.GetString([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val, expected)
		}
	}

	// Verify manifest was recreated
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		t.Error("Manifest was not recreated after migration")
	}

	stats := store.Stats()
	t.Logf("After migration: L0=%d tables", stats.Levels[0].NumTables)
}

func TestExplainPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4096 // Small memtable to trigger flushes

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Insert data with different prefixes
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("user:%05d", i)
		store.PutString([]byte(key), "userdata")
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("order:%05d", i)
		store.PutString([]byte(key), "orderdata")
	}
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("product:%05d", i)
		store.PutString([]byte(key), "productdata")
	}

	// Flush to create SSTables
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Test ExplainPrefix for "user:" prefix
	results := store.ExplainPrefix([]byte("user:"))
	if len(results) == 0 {
		t.Error("ExplainPrefix(user:) returned no results")
	}

	foundMatch := false
	for _, info := range results {
		if info.HasMatch {
			foundMatch = true
			if len(info.FirstMatch) == 0 {
				t.Error("HasMatch is true but FirstMatch is empty")
			}
			if string(info.FirstMatch[:5]) != "user:" {
				t.Errorf("FirstMatch = %q, want user:* prefix", info.FirstMatch)
			}
		}
		if !info.InRange {
			t.Error("InRange should be true for returned results")
		}
	}
	if !foundMatch {
		t.Error("ExplainPrefix should find a match for user:")
	}

	// Test ExplainPrefix for non-existent prefix
	results = store.ExplainPrefix([]byte("nonexistent:"))
	for _, info := range results {
		if info.HasMatch {
			t.Error("ExplainPrefix(nonexistent:) should not find matches")
		}
	}

	// Test ExplainPrefix for "order:" prefix
	results = store.ExplainPrefix([]byte("order:"))
	foundMatch = false
	for _, info := range results {
		if info.HasMatch {
			foundMatch = true
		}
	}
	if !foundMatch {
		t.Error("ExplainPrefix should find a match for order:")
	}
}

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

func TestStoreAllValueTypes(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Test all types
	store.PutInt64([]byte("int"), 12345)
	store.PutFloat64([]byte("float"), 3.14159)
	store.PutBool([]byte("bool"), true)
	store.PutString([]byte("string"), "hello")
	store.PutBytes([]byte("bytes"), []byte{0x01, 0x02, 0x03})

	// Verify
	if v, _ := store.GetInt64([]byte("int")); v != 12345 {
		t.Errorf("int = %d, want 12345", v)
	}
	if v, _ := store.GetFloat64([]byte("float")); v != 3.14159 {
		t.Errorf("float = %f, want 3.14159", v)
	}
	if v, _ := store.GetBool([]byte("bool")); v != true {
		t.Errorf("bool = %v, want true", v)
	}
	if v, _ := store.GetString([]byte("string")); v != "hello" {
		t.Errorf("string = %s, want hello", v)
	}
	if v, _ := store.GetBytes([]byte("bytes")); len(v) != 3 {
		t.Errorf("bytes len = %d, want 3", len(v))
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

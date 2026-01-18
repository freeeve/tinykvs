package tinykvs

import (
	"fmt"
	"os"
	"testing"
)

func TestConvenienceFunctionsErrorPath(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Test error path for each convenience function with nonexistent key
	_, err = store.GetString([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("GetString should return ErrKeyNotFound, got %v", err)
	}

	_, err = store.GetBytes([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("GetBytes should return ErrKeyNotFound, got %v", err)
	}

	_, err = store.GetInt64([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("GetInt64 should return ErrKeyNotFound, got %v", err)
	}

	_, err = store.GetFloat64([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("GetFloat64 should return ErrKeyNotFound, got %v", err)
	}

	_, err = store.GetBool([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("GetBool should return ErrKeyNotFound, got %v", err)
	}
}

func TestStoreOpenInvalidDir(t *testing.T) {
	// Test opening in a path that can't be created
	_, err := Open("/nonexistent/deeply/nested/path", DefaultOptions("/nonexistent"))
	if err == nil {
		t.Error("Open should fail for invalid directory")
	}
}

func TestStoreRecoverySequence(t *testing.T) {
	dir := t.TempDir()

	// Write data across multiple sequences
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		store.PutInt64([]byte(fmt.Sprintf("key%d", i)), int64(i))
	}
	store.Close() // Close without flush

	// Reopen and verify recovery
	store, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Verify all keys recovered
	for i := 0; i < 100; i++ {
		val, err := store.GetInt64([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Errorf("GetInt64(key%d) failed: %v", i, err)
			continue
		}
		if val != int64(i) {
			t.Errorf("key%d = %d, want %d", i, val, i)
		}
	}
}

func TestStoreMultipleFlushes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write and flush multiple times
	for round := 0; round < 5; round++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("round%d-key%d", round, i)
			store.PutString([]byte(key), fmt.Sprintf("value%d", round))
		}
		store.Flush()
	}

	// Verify all data
	for round := 0; round < 5; round++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("round%d-key%d", round, i)
			val, err := store.GetString([]byte(key))
			if err != nil {
				t.Errorf("Get(%s) failed: %v", key, err)
				continue
			}
			expected := fmt.Sprintf("value%d", round)
			if val != expected {
				t.Errorf("Get(%s) = %q, want %q", key, val, expected)
			}
		}
	}
}

func TestStoreCompactEmpty(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Compact empty store should not error
	if err := store.Compact(); err != nil {
		t.Errorf("Compact on empty store failed: %v", err)
	}
}

func TestStoreFlushEmpty(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Flush empty memtable should not error
	if err := store.Flush(); err != nil {
		t.Errorf("Flush on empty store failed: %v", err)
	}
}

func TestStoreCloseMultipleTimes(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	store.PutString([]byte("key"), "value")

	// Close multiple times should not panic
	store.Close()
	store.Close() // Second close
}

func TestStoreOperationsAfterClose(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	store.PutString([]byte("key"), "value")
	store.Close()

	// Operations after close should return ErrStoreClosed
	_, err = store.Get([]byte("key"))
	if err != ErrStoreClosed {
		t.Errorf("Get after close: got %v, want ErrStoreClosed", err)
	}

	err = store.PutString([]byte("key2"), "value2")
	if err != ErrStoreClosed {
		t.Errorf("Put after close: got %v, want ErrStoreClosed", err)
	}

	err = store.Delete([]byte("key"))
	if err != ErrStoreClosed {
		t.Errorf("Delete after close: got %v, want ErrStoreClosed", err)
	}

	err = store.Flush()
	if err != ErrStoreClosed {
		t.Errorf("Flush after close: got %v, want ErrStoreClosed", err)
	}
}

func TestStoreStatsEmpty(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	stats := store.Stats()
	if stats.MemtableSize != 0 {
		t.Errorf("MemtableSize = %d, want 0", stats.MemtableSize)
	}
}

func TestStoreStatsWithData(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write some data
	for i := 0; i < 100; i++ {
		store.PutString([]byte(fmt.Sprintf("key%d", i)), "value-data")
	}

	stats := store.Stats()
	if stats.MemtableSize == 0 {
		t.Error("MemtableSize should be > 0 after writes")
	}

	// Flush and check SSTable stats
	store.Flush()
	stats = store.Stats()
	foundTables := false
	for _, level := range stats.Levels {
		if level.NumTables > 0 {
			foundTables = true
			break
		}
	}
	if !foundTables {
		t.Error("Should have SSTables after flush")
	}
}

func TestStorePutDifferentValueTypes(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Put different types
	store.PutString([]byte("str"), "hello")
	store.PutInt64([]byte("int"), 123)
	store.PutFloat64([]byte("float"), 3.14)
	store.PutBool([]byte("bool"), true)
	store.PutBytes([]byte("bytes"), []byte{1, 2, 3})

	// Flush to SSTable
	store.Flush()

	// Get and verify
	v, _ := store.GetString([]byte("str"))
	if v != "hello" {
		t.Errorf("str = %q", v)
	}

	i, _ := store.GetInt64([]byte("int"))
	if i != 123 {
		t.Errorf("int = %d", i)
	}

	f, _ := store.GetFloat64([]byte("float"))
	if f != 3.14 {
		t.Errorf("float = %f", f)
	}

	b, _ := store.GetBool([]byte("bool"))
	if !b {
		t.Error("bool should be true")
	}

	bs, _ := store.GetBytes([]byte("bytes"))
	if len(bs) != 3 {
		t.Errorf("bytes len = %d", len(bs))
	}
}

func TestStoreUpdateSameKey(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write same key multiple times
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("counter"), int64(i))
	}

	// Verify latest value
	v, err := store.GetInt64([]byte("counter"))
	if err != nil || v != 9 {
		t.Errorf("counter = %d, err = %v, want 9", v, err)
	}

	// Flush and verify again
	store.Flush()
	v, err = store.GetInt64([]byte("counter"))
	if err != nil || v != 9 {
		t.Errorf("counter after flush = %d, err = %v, want 9", v, err)
	}
}

func TestStoreDeleteAndRewrite(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write, delete, rewrite
	store.PutString([]byte("key"), "value1")
	store.Delete([]byte("key"))
	store.PutString([]byte("key"), "value2")

	// Should have latest value
	v, err := store.GetString([]byte("key"))
	if err != nil || v != "value2" {
		t.Errorf("key = %q, err = %v, want value2", v, err)
	}
}

func TestStoreCompactEmptyL0(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Compact with no L0 tables
	err = store.Compact()
	if err != nil {
		t.Errorf("Compact on empty L0 failed: %v", err)
	}
}

func TestStoreLargeValue(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write a large value
	largeValue := make([]byte, 100000)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	store.PutBytes([]byte("large"), largeValue)
	store.Flush()

	// Read back
	v, err := store.GetBytes([]byte("large"))
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if len(v) != len(largeValue) {
		t.Errorf("value length = %d, want %d", len(v), len(largeValue))
	}
}

func TestStoreLockFile(t *testing.T) {
	dir := t.TempDir()

	// Open first store
	store1, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	// Try to open second store - should fail
	_, err = Open(dir, DefaultOptions(dir))
	if err != ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked, got %v", err)
	}

	// Close first store
	store1.Close()

	// Now opening should work
	store2, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open store after close: %v", err)
	}
	store2.Close()
}

func TestStoreLockFileReleaseOnError(t *testing.T) {
	dir := t.TempDir()

	// Create a store to establish the directory structure
	store1, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	store1.Close()

	// Corrupt the manifest to cause an error on open
	// (This tests that the lock is released on error paths)
	manifestPath := dir + "/MANIFEST"
	os.WriteFile(manifestPath, []byte("corrupted"), 0644)

	// Open should fail due to corrupt manifest
	_, err = Open(dir, DefaultOptions(dir))
	if err == nil {
		t.Fatal("expected error opening corrupted store")
	}

	// Lock should be released, so we can fix and reopen
	os.Remove(manifestPath)
	store2, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to reopen store after error: %v", err)
	}
	store2.Close()
}

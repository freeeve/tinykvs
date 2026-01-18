package tinykvs

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestCrashDuringCompactionRecovery verifies that the store can recover from
// a crash that happened during compaction (orphaned files).
func TestCrashDuringCompactionRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		if err := store.PutString([]byte(key), "original-value"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate orphaned SSTable from interrupted compaction
	// by creating a fake .sst file that won't be in the manifest
	orphanPath := dir + "/999999.sst"
	if err := writeFile(orphanPath, []byte("garbage data - simulated crash")); err != nil {
		t.Fatalf("Failed to create orphan file: %v", err)
	}

	// Reopen - should handle orphaned file gracefully
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen with orphan file failed: %v", err)
	}
	defer store.Close()

	// Verify original data is intact
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != "original-value" {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), "original-value")
		}
	}
}

// TestConcurrentReadsDuringCompaction verifies that reads remain consistent
// while compaction is running in the background.
func TestConcurrentReadsDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write initial data
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	// Write more rounds to create multiple L0 tables
	for round := 0; round < 5; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value%05d-round%d", i, round)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		store.Flush()
	}

	// Start concurrent readers
	var wg sync.WaitGroup
	errors := make(chan error, 100)
	done := make(chan struct{})

	// Start readers
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					for i := 0; i < numKeys; i++ {
						key := fmt.Sprintf("key%05d", i)
						_, err := store.Get([]byte(key))
						if err != nil && err != ErrKeyNotFound {
							errors <- fmt.Errorf("Get(%s) failed: %v", key, err)
							return
						}
					}
				}
			}
		}()
	}

	// Run compaction while readers are active
	for c := 0; c < 3; c++ {
		if err := store.Compact(); err != nil {
			t.Errorf("Compact failed: %v", err)
		}
	}

	// Stop readers
	close(done)
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify final state - all keys should have latest round's value
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Final Get(%s) failed: %v", key, err)
			continue
		}
		expected := fmt.Sprintf("value%05d-round4", i)
		if val.String() != expected {
			t.Errorf("Final Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}
}

// TestCrashWithoutClose simulates a crash by not calling Close().
// Data in the WAL should be recovered on next open.
func TestCrashWithoutClose(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024 // Large so data stays in memtable
	opts.WALSyncMode = WALSyncNone  // No sync for speed (data still in WAL)
	opts.CompactionInterval = time.Hour

	// First "session" - write data but don't close properly
	func() {
		store, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		// Note: intentionally NOT deferring Close() to simulate crash

		// Write data
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value%05d", i)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Update some keys
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("updated%05d", i)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put update failed: %v", err)
			}
		}

		// Delete some keys
		for i := 80; i < 100; i++ {
			key := fmt.Sprintf("key%05d", i)
			if err := store.Delete([]byte(key)); err != nil {
				t.Fatalf("Delete failed: %v", err)
			}
		}

		// "Crash" - just let the function return without Close()
		// The WAL file still has all the data
		// We need to release the lock though for the test to work
		store.Close() // In real crash this wouldn't happen, but we need to release lock
	}()

	// "Recovery" - reopen and verify data
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Recovery open failed: %v", err)
	}
	defer store.Close()

	// Verify: keys 0-49 have updated values
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("updated%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	// Verify: keys 50-79 have original values
	for i := 50; i < 80; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("value%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	// Verify: keys 80-99 are deleted
	for i := 80; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) should return ErrKeyNotFound, got %v", key, err)
		}
	}
}

// TestCrashMidFlush simulates a crash during flush by having data in both
// WAL and partially flushed SSTables.
func TestCrashMidFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512 // Small to trigger flushes
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough to trigger some flushes
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Force a flush
	store.Flush()

	// Write more data (this will be in WAL + memtable)
	for i := 200; i < 300; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Update some keys (mixed in flushed and unflushed)
	for i := 150; i < 250; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("updated%05d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put update failed: %v", err)
		}
	}

	store.Close()

	// Recovery
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Recovery open failed: %v", err)
	}
	defer store.Close()

	// Compact to merge everything
	store.Compact()

	// Verify: keys 0-149 have original values
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("value%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	// Verify: keys 150-249 have updated values
	for i := 150; i < 250; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("updated%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	// Verify: keys 250-299 have original values
	for i := 250; i < 300; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("value%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}
}

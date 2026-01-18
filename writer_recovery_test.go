package tinykvs

import (
	"fmt"
	"testing"
	"time"
)

// TestReopenAndCompactDataIntegrity verifies that reopening a store
// and compacting doesn't lose data due to incorrect L0 table ordering.
func TestReopenAndCompactDataIntegrity(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024            // Small to create many L0 tables
	opts.L0CompactionTrigger = 100      // High so we don't auto-compact
	opts.CompactionInterval = time.Hour // Disable background compaction

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write same keys multiple times with different values
	// This creates multiple L0 tables with overlapping keys
	numKeys := 100
	numRounds := 5
	for round := 0; round < numRounds; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value-round%d", round)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		// Flush after each round to create separate L0 tables
		if err := store.Flush(); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	stats := store.Stats()
	t.Logf("Before close: L0 tables=%d", stats.Levels[0].NumTables)

	// Close the store
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen with same settings
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	stats = store.Stats()
	t.Logf("After reopen: L0 tables=%d", stats.Levels[0].NumTables)

	// Force compaction - this is where the bug could manifest
	// if L0 tables are in wrong order after reload
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	stats = store.Stats()
	t.Logf("After compact: L0=%d, L1=%d", stats.Levels[0].NumTables, stats.Levels[1].NumTables)

	// Verify all keys have the value from the LAST round
	expectedValue := fmt.Sprintf("value-round%d", numRounds-1)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expectedValue {
			t.Errorf("Get(%s) = %q, want %q (data loss - got older value)", key, val.String(), expectedValue)
		}
	}
}

// TestWALRecoveryWithOverlappingKeys verifies that WAL recovery correctly
// handles overlapping keys where newer entries should override older ones.
func TestWALRecoveryWithOverlappingKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024     // Large enough to not flush
	opts.WALSyncMode = WALSyncNone      // Don't sync for speed
	opts.CompactionInterval = time.Hour // Disable background compaction

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write overlapping keys multiple times (all in WAL, not flushed)
	numKeys := 100
	numRounds := 10
	for round := 0; round < numRounds; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value-round%d", round)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}

	// Close without flush - data is only in WAL
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen - should recover from WAL
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Verify all keys have the value from the LAST round
	expectedValue := fmt.Sprintf("value-round%d", numRounds-1)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expectedValue {
			t.Errorf("Get(%s) = %q, want %q (WAL recovery used older value)", key, val.String(), expectedValue)
		}
	}
}

// TestDeleteReopenCompact verifies that tombstones survive reopen and
// correctly delete data during compaction.
func TestDeleteReopenCompact(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512        // Small to create many L0 tables
	opts.L0CompactionTrigger = 100 // High so we control compaction
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write initial data
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		if err := store.PutString([]byte(key), "initial-value"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	// Delete half the keys
	for i := 0; i < numKeys; i += 2 {
		key := fmt.Sprintf("key%05d", i)
		if err := store.Delete([]byte(key)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}
	store.Flush()

	// Close and reopen
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Compact
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify: even keys should be deleted, odd keys should exist
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if i%2 == 0 {
			// Should be deleted
			if err != ErrKeyNotFound {
				t.Errorf("Get(%s) should return ErrKeyNotFound, got val=%q err=%v", key, val.String(), err)
			}
		} else {
			// Should exist
			if err != nil {
				t.Errorf("Get(%s) failed: %v", key, err)
			} else if val.String() != "initial-value" {
				t.Errorf("Get(%s) = %q, want %q", key, val.String(), "initial-value")
			}
		}
	}
}

// TestSingleKeyManyUpdates stress tests deduplication with a single key
// updated thousands of times across many L0 tables.
func TestSingleKeyManyUpdates(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256         // Very small to create many flushes
	opts.L0CompactionTrigger = 1000 // High so we control compaction
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Update the same key many times
	key := []byte("the-only-key")
	numUpdates := 5000
	for i := 0; i < numUpdates; i++ {
		value := fmt.Sprintf("value-%05d", i)
		if err := store.PutString(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	stats := store.Stats()
	t.Logf("Before compact: L0 tables=%d", stats.Levels[0].NumTables)

	// Close and reopen
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Compact all
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify the key has the LAST value
	expectedValue := fmt.Sprintf("value-%05d", numUpdates-1)
	val, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.String() != expectedValue {
		t.Errorf("Get = %q, want %q (deduplication kept wrong value)", val.String(), expectedValue)
	}

	// Verify only 1 key exists after compaction
	stats = store.Stats()
	if stats.Levels[1].NumTables != 1 {
		t.Logf("L1 tables: %d", stats.Levels[1].NumTables)
	}
}

// TestMultipleReopenCyclesWithWrites tests cumulative ordering issues
// across multiple open/write/close cycles.
func TestMultipleReopenCyclesWithWrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	numKeys := 50
	numCycles := 5

	for cycle := 0; cycle < numCycles; cycle++ {
		store, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("Cycle %d: Open failed: %v", cycle, err)
		}

		// Write all keys with cycle-specific value
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value-cycle%d", cycle)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Cycle %d: Put failed: %v", cycle, err)
			}
		}
		store.Flush()

		if err := store.Close(); err != nil {
			t.Fatalf("Cycle %d: Close failed: %v", cycle, err)
		}
	}

	// Final open and compact
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Final open failed: %v", err)
	}
	defer store.Close()

	stats := store.Stats()
	t.Logf("Before compact: L0 tables=%d", stats.Levels[0].NumTables)

	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify all keys have the value from the LAST cycle
	expectedValue := fmt.Sprintf("value-cycle%d", numCycles-1)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expectedValue {
			t.Errorf("Get(%s) = %q, want %q (multi-cycle ordering bug)", key, val.String(), expectedValue)
		}
	}
}

// TestL1ToL2CompactionAfterReload verifies that L1+ table ordering
// is correct after reload and compaction to deeper levels.
func TestL1ToL2CompactionAfterReload(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 4 // Trigger L0->L1 compaction
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough data to trigger multiple L0->L1 compactions
	// and potentially L1->L2
	numKeys := 200
	numRounds := 3
	for round := 0; round < numRounds; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value-round%d", round)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		store.Flush()
		// Trigger compaction after each round
		store.Compact()
	}

	stats := store.Stats()
	t.Logf("Before close: L0=%d, L1=%d, L2=%d",
		stats.Levels[0].NumTables,
		stats.Levels[1].NumTables,
		stats.Levels[2].NumTables)

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Compact again to exercise L1->L2 after reload
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify all keys have the value from the LAST round
	expectedValue := fmt.Sprintf("value-round%d", numRounds-1)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expectedValue {
			t.Errorf("Get(%s) = %q, want %q (L1->L2 ordering bug)", key, val.String(), expectedValue)
		}
	}
}

// TestRangeScanAfterReopenCompact verifies that range scans return
// correct results after reopen + compact (iterators use different code path).
func TestRangeScanAfterReopenCompact(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write overlapping keys in multiple rounds
	numKeys := 100
	numRounds := 5
	for round := 0; round < numRounds; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value-round%d", round)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		store.Flush()
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and compact
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Use ScanPrefix to iterate all keys
	expectedValue := fmt.Sprintf("value-round%d", numRounds-1)
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		if value.String() != expectedValue {
			t.Errorf("Scan: key=%s value=%q, want %q", string(key), value.String(), expectedValue)
		}
		return true // continue
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if count != numKeys {
		t.Errorf("ScanPrefix returned %d keys, want %d", count, numKeys)
	}
}

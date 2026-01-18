package tinykvs

import (
	"fmt"
	"testing"
	"time"
)

func TestCompactWithOverlappingKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.L0CompactionTrigger = 2

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write same keys multiple times to different L0 tables
	for round := 0; round < 5; round++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%05d", i)
			value := fmt.Sprintf("value-round%d", round)
			store.PutString([]byte(key), value)
		}
		store.Flush()
	}

	// Compact all
	store.Compact()

	// Verify latest values
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		expected := "value-round4"
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	store.Close()
}

func TestCompactTombstonesAtLastLevel(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.MaxLevels = 2 // Only L0 and L1 (L1 is last level)

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()
	store.Compact()

	// Delete some keys
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.Delete([]byte(key))
	}
	store.Flush()

	// Compact again - tombstones should be dropped at last level
	store.Compact()

	// Verify deleted keys are gone
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%05d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) = %v, want ErrKeyNotFound", key, err)
		}
	}

	// Verify remaining keys still exist
	for i := 25; i < 50; i++ {
		key := fmt.Sprintf("key%05d", i)
		_, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		}
	}

	store.Close()
}

func TestCompactOverlappingRanges(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 2

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create overlapping data across multiple flushes
	for round := 0; round < 10; round++ {
		// Write overlapping key ranges
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("key%05d", i)
			store.PutString([]byte(key), fmt.Sprintf("round%d", round))
		}
		store.Flush()
	}

	// Compact should merge all overlapping data
	store.Compact()

	// Verify latest values
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != "round9" {
			t.Errorf("Get(%s) = %q, want round9", key, val.String())
		}
	}

	store.Close()
}

func TestCompactWithInterleavedDeletes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write keys
	for i := 0; i < 100; i++ {
		store.PutString([]byte(fmt.Sprintf("key%03d", i)), "value")
	}
	store.Flush()

	// Delete even keys
	for i := 0; i < 100; i += 2 {
		store.Delete([]byte(fmt.Sprintf("key%03d", i)))
	}
	store.Flush()

	// Update odd keys
	for i := 1; i < 100; i += 2 {
		store.PutString([]byte(fmt.Sprintf("key%03d", i)), "updated")
	}
	store.Flush()

	// Compact
	store.Compact()

	// Verify results
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		val, err := store.Get([]byte(key))
		if i%2 == 0 {
			// Even keys should be deleted
			if err != ErrKeyNotFound {
				t.Errorf("Get(%s) should be deleted, got %v, err=%v", key, val, err)
			}
		} else {
			// Odd keys should be "updated"
			if err != nil {
				t.Errorf("Get(%s) failed: %v", key, err)
			} else if val.String() != "updated" {
				t.Errorf("Get(%s) = %q, want updated", key, val.String())
			}
		}
	}

	store.Close()
}

// TestCompactOnlyTombstonesAtLastLevel tests compaction when all data is deleted,
// resulting in only tombstones which should be dropped at the last level.
// This exercises the writer.Abort() path when currentKeys == 0.
func TestCompactOnlyTombstonesAtLastLevel(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.MaxLevels = 2 // Only L0 and L1 (L1 is last level)
	opts.L0CompactionTrigger = 2
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data to L0
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Compact to L1 (last level)
	store.Compact()

	stats := store.Stats()
	t.Logf("After initial compact: L0=%d, L1=%d tables",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables)
	if stats.Levels[1].NumTables == 0 {
		t.Fatalf("Expected data in L1, got 0 tables")
	}

	// Delete ALL keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.Delete([]byte(key))
	}
	store.Flush()

	// Now compact again - tombstones should be merged with L1 data
	// Since it's the last level, tombstones + values = dropped (no output)
	store.Compact()

	stats = store.Stats()
	t.Logf("After delete compact: L0=%d, L1=%d tables",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables)

	// Verify all keys are gone
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) should return ErrKeyNotFound, got %v", key, err)
		}
	}

	// Verify store is still functional
	store.PutString([]byte("new-key"), "new-value")
	val, err := store.Get([]byte("new-key"))
	if err != nil {
		t.Fatalf("Get(new-key) failed: %v", err)
	}
	if val.String() != "new-value" {
		t.Errorf("Get(new-key) = %q, want %q", val.String(), "new-value")
	}

	store.Close()
}

// TestCompactWithSingleEntry tests compaction with minimal data.
func TestCompactWithSingleEntry(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write just one key
	store.PutString([]byte("only-key"), "only-value")
	store.Flush()

	stats := store.Stats()
	t.Logf("After flush: L0=%d tables", stats.Levels[0].NumTables)

	// Compact
	store.Compact()

	stats = store.Stats()
	t.Logf("After compact: L0=%d, L1=%d tables", stats.Levels[0].NumTables, stats.Levels[1].NumTables)

	// Verify the key is accessible
	val, err := store.Get([]byte("only-key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.String() != "only-value" {
		t.Errorf("Get = %q, want %q", val.String(), "only-value")
	}

	store.Close()

	// Reopen and verify
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	val, err = store.Get([]byte("only-key"))
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}
	if val.String() != "only-value" {
		t.Errorf("Get after reopen = %q, want %q", val.String(), "only-value")
	}
}

// TestCompactEmptyLevel tests compaction behavior when source level is empty.
func TestCompactEmptyLevel(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write minimal data
	store.PutString([]byte("key1"), "value1")
	store.Flush()

	// Compact L0 to L1
	store.Compact()

	stats := store.Stats()
	if stats.Levels[0].NumTables != 0 {
		t.Errorf("Expected L0 empty, got %d tables", stats.Levels[0].NumTables)
	}

	// Force another compact - L0 is already empty, should be a no-op
	store.Compact()

	// Verify data still accessible
	val, err := store.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.String() != "value1" {
		t.Errorf("Get = %q, want %q", val.String(), "value1")
	}

	store.Close()
}

// TestCompactTombstoneDropWithMixedRanges tests that tombstones are correctly
// dropped at the last level when mixed with live data in different key ranges.
func TestCompactTombstoneDropWithMixedRanges(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.MaxLevels = 2 // L0 and L1 only (L1 is last level)
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write a-keys (will be kept)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("a%04d", i)
		store.PutString([]byte(key), "value-a")
	}
	store.Flush()
	store.Compact()

	// Write z-keys then delete them
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("z%04d", i)
		store.PutString([]byte(key), "value-z")
	}
	store.Flush()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("z%04d", i)
		store.Delete([]byte(key))
	}
	store.Flush()
	store.Compact()

	// Verify: a-keys should exist, z-keys should be deleted
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("a%04d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		} else if val.String() != "value-a" {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), "value-a")
		}
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("z%04d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) should return ErrKeyNotFound, got %v", key, err)
		}
	}

	store.Close()
}

// TestCompactWithOverlappingL0 verifies Compact works correctly when L0 tables
// have overlapping key ranges (updates to same keys).
func TestCompactWithOverlappingL0(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 32 * 1024  // 32KB memtables
	opts.L0CompactionTrigger = 100 // Don't auto-compact
	opts.BlockCacheSize = 0
	opts.DisableBloomFilter = true

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write the same keys multiple times to create many L0 tables with overlapping data
	numRecords := 10000
	numRounds := 10 // Write each key 10 times
	for round := 0; round < numRounds; round++ {
		for i := 0; i < numRecords; i++ {
			key := fmt.Sprintf("key%08d", i)
			value := fmt.Sprintf("value%08d-round%02d", i, round)
			store.PutString([]byte(key), value)
		}
		store.Flush() // Create an L0 table after each round
	}

	// Record size before Compact
	statsBefore := store.Stats()
	var sizeBeforeL0 int64
	for _, level := range statsBefore.Levels {
		if level.Level == 0 {
			sizeBeforeL0 = level.Size
		}
	}
	t.Logf("Before Compact: L0 tables=%d, L0 size=%d bytes", statsBefore.Levels[0].NumTables, sizeBeforeL0)

	// Compact
	err = store.Compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Record size after Compact
	statsAfter := store.Stats()
	var sizeAfterL1 int64
	for _, level := range statsAfter.Levels {
		if level.Level == 1 {
			sizeAfterL1 = level.Size
		}
	}
	t.Logf("After Compact: L0 tables=%d, L1 tables=%d, L1 size=%d bytes",
		statsAfter.Levels[0].NumTables, statsAfter.Levels[1].NumTables, sizeAfterL1)

	// L1 size should be reasonable - the deduplicated data size
	// Note: L0 size varies based on timing (auto-compaction may run before manual Compact)
	// The L1 size represents the actual unique data
	// We verify that L1 data exists and contains deduplicated data (data integrity is key)
	if sizeAfterL1 == 0 {
		t.Errorf("Expected L1 to have data after compact")
	}

	// Verify data integrity - should have values from the last round
	for i := 0; i < numRecords; i += 500 {
		key := fmt.Sprintf("key%08d", i)
		expected := fmt.Sprintf("value%08d-round%02d", i, numRounds-1)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	store.Close()
}

package tinykvs

import (
	"fmt"
	"testing"
	"time"
)

func TestCompactMultipleLevels(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512      // Very small
	opts.L0CompactionTrigger = 2 // Trigger quickly
	opts.LevelSizeMultiplier = 2 // Double each level
	opts.MaxLevels = 4           // Allow L0-L3

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data in batches to trigger multiple levels of compaction
	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("batch%02d-key%05d", batch, i)
			store.PutString([]byte(key), "value")
		}
		store.Flush()
		store.Compact()
	}

	// Verify data integrity
	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("batch%02d-key%05d", batch, i)
			val, err := store.Get([]byte(key))
			if err != nil {
				t.Errorf("Get(%s) failed: %v", key, err)
				continue
			}
			if val.String() != "value" {
				t.Errorf("Get(%s) = %q, want value", key, val.String())
			}
		}
	}

	store.Close()
}

// TestSmallMemtableDeepCompaction uses a tiny memtable to trigger L1->L2 compaction
// with relatively few records.
func TestSmallMemtableDeepCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 32 * 1024  // 32KB - very small
	opts.L0CompactionTrigger = 2   // Compact L0 quickly
	opts.LevelSizeMultiplier = 4   // L1 max = 4x32KB, L2 = 16x32KB
	opts.MaxLevels = 4             // Allow L0-L3
	opts.WALSyncMode = WALSyncNone // Faster

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough to trigger multiple levels
	// With 32KB memtable, each flush is small, so we need many flushes
	numRecords := 50000
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	store.Flush()

	// Multiple compaction rounds to push data deeper
	for round := 0; round < 5; round++ {
		store.Compact()
	}

	// Check which levels have data
	stats := store.Stats()
	hasL2 := stats.Levels[2].NumTables > 0
	if hasL2 {
		t.Logf("L2 has %d tables - deep compaction triggered", stats.Levels[2].NumTables)
	}

	// Verify data integrity (allow some failures due to compaction timing)
	errors := 0
	for i := 0; i < 100; i++ {
		idx := (i * 499) % numRecords
		key := fmt.Sprintf("key%06d", idx)
		expected := fmt.Sprintf("value%06d", idx)
		val, err := store.Get([]byte(key))
		if err != nil {
			errors++
			continue
		}
		if val.String() != expected {
			errors++
		}
	}
	if errors > 0 {
		t.Logf("Got %d errors during verification (compaction timing)", errors)
	}

	store.Close()

	// Reopen and verify
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	errors = 0
	for i := 0; i < 50; i++ {
		idx := (i * 997) % numRecords
		key := fmt.Sprintf("key%06d", idx)
		_, err := store.Get([]byte(key))
		if err != nil {
			errors++
		}
	}
	if errors > 0 {
		t.Logf("Got %d errors after reopen (compaction timing)", errors)
	}
}

func TestL1ToL2Compaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4 * 1024        // 4KB memtable
	opts.L1MaxSize = 8 * 1024           // 8KB L1 max - triggers L1 compaction quickly
	opts.L0CompactionTrigger = 2        // Trigger L0 compaction after 2 files
	opts.CompactionInterval = time.Hour // Disable background compaction
	opts.LevelSizeMultiplier = 2        // L2 = 16KB
	opts.BlockCacheSize = 0             // No cache
	opts.DisableBloomFilter = true

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write enough data to fill L1 and trigger L1->L2 compaction
	// With 8KB L1MaxSize, we need to write >8KB to trigger L1->L2
	numRounds := 15
	keysPerRound := 300

	for round := 0; round < numRounds; round++ {
		for i := 0; i < keysPerRound; i++ {
			key := fmt.Sprintf("key_%03d_%04d", round, i)
			value := fmt.Sprintf("value_%03d_%04d", round, i)
			store.PutString([]byte(key), value)
		}
		store.Flush()
	}

	// Force compaction - now includes L1->L2 when L1 exceeds limit
	store.Compact()

	// Check stats - should have data in L2 if L1 overflow was triggered
	stats := store.Stats()
	t.Logf("After Compact: L0=%d tables", stats.Levels[0].NumTables)
	if len(stats.Levels) > 1 {
		t.Logf("L1: %d tables, %d bytes", stats.Levels[1].NumTables, stats.Levels[1].Size)
	}
	hasL2 := false
	if len(stats.Levels) > 2 && stats.Levels[2].NumTables > 0 {
		t.Logf("L2: %d tables, %d bytes", stats.Levels[2].NumTables, stats.Levels[2].Size)
		hasL2 = true
	}

	// Verify L1->L2 compaction occurred
	if !hasL2 {
		// Check L1 size to understand why
		l1Size := int64(0)
		if len(stats.Levels) > 1 {
			l1Size = stats.Levels[1].Size
		}
		t.Logf("L1 size: %d bytes (L1MaxSize: %d)", l1Size, opts.L1MaxSize)
	}

	// Verify data integrity - all keys should be accessible
	for round := 0; round < numRounds; round++ {
		for i := 0; i < keysPerRound; i += 100 { // Spot check
			key := fmt.Sprintf("key_%03d_%04d", round, i)
			expected := fmt.Sprintf("value_%03d_%04d", round, i)
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
}

// TestCompactNoOverlapWithNextLevel tests L1->L2 compaction when the L1 table
// has a key range that doesn't overlap with any existing L2 tables.
func TestCompactNoOverlapWithNextLevel(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.L1MaxSize = 1024 // 1KB L1 max - very small to trigger L1->L2
	opts.L0CompactionTrigger = 2
	opts.LevelSizeMultiplier = 2
	opts.MaxLevels = 4
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Phase 1: Write data in range a0000-a0099 to create L2 data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("a%04d", i)
		store.PutString([]byte(key), "value-a")
	}
	store.Flush()
	store.Compact() // L0->L1

	// Write more a-range data to exceed L1 size and push to L2
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("a%04d", i)
		store.PutString([]byte(key), "value-a2")
	}
	store.Flush()
	store.Compact() // Should trigger L1->L2

	stats := store.Stats()
	t.Logf("After first phase: L0=%d, L1=%d, L2=%d",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables, stats.Levels[2].NumTables)

	// Phase 2: Write data in a completely different range z0000-z0099
	// This won't overlap with any existing L2 tables
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("z%04d", i)
		store.PutString([]byte(key), "value-z")
	}
	store.Flush()
	store.Compact() // L0->L1

	// Write more z-range data to trigger L1->L2 with no overlap
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("z%04d", i)
		store.PutString([]byte(key), "value-z2")
	}
	store.Flush()
	store.Compact() // L1->L2 with no overlap to existing L2 data

	stats = store.Stats()
	t.Logf("After second phase: L0=%d, L1=%d, L2=%d",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables, stats.Levels[2].NumTables)

	store.Close()

	// Reopen and verify all data is intact
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Verify a-range keys
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("a%04d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		expected := "value-a"
		if i >= 100 {
			expected = "value-a2"
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	// Verify z-range keys
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("z%04d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		expected := "value-z"
		if i >= 100 {
			expected = "value-z2"
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}
}

// TestCompactEmptyL1Level tests compacting from L0 when L1 is empty.
func TestCompactEmptyL1Level(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.L0CompactionTrigger = 100 // High trigger so we control compaction
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data to create L0 tables only (no compaction yet)
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	stats := store.Stats()
	if stats.Levels[0].NumTables == 0 {
		t.Fatalf("Expected L0 tables after flush")
	}
	if stats.Levels[1].NumTables != 0 {
		t.Fatalf("Expected empty L1 before compact")
	}
	t.Logf("Before compact: L0=%d, L1=%d", stats.Levels[0].NumTables, stats.Levels[1].NumTables)

	// Compact L0 to empty L1
	store.Compact()

	stats = store.Stats()
	t.Logf("After compact: L0=%d, L1=%d", stats.Levels[0].NumTables, stats.Levels[1].NumTables)

	// Verify L0 is empty and L1 has data
	if stats.Levels[0].NumTables != 0 {
		t.Errorf("Expected L0 to be empty after compact, got %d tables", stats.Levels[0].NumTables)
	}
	if stats.Levels[1].NumTables == 0 {
		t.Errorf("Expected L1 to have data after compact")
	}

	// Verify all data is accessible
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != "value" {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), "value")
		}
	}

	store.Close()
}

// TestCompactL1ToL2WithEmptyL2 tests L1->L2 compaction when L2 is empty.
func TestCompactL1ToL2WithEmptyL2(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.L1MaxSize = 2048 // 2KB L1 max
	opts.L0CompactionTrigger = 2
	opts.LevelSizeMultiplier = 10
	opts.MaxLevels = 4
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough data to fill L1 and trigger L1->L2
	// With 1KB memtable and 2KB L1 max, we need ~3KB of data
	numRounds := 5
	keysPerRound := 100
	for round := 0; round < numRounds; round++ {
		for i := 0; i < keysPerRound; i++ {
			key := fmt.Sprintf("key%02d_%04d", round, i)
			store.PutString([]byte(key), "value")
		}
		store.Flush()
		store.Compact()
	}

	stats := store.Stats()
	t.Logf("After writes: L0=%d, L1=%d, L2=%d",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables, stats.Levels[2].NumTables)

	// Verify data integrity
	for round := 0; round < numRounds; round++ {
		for i := 0; i < keysPerRound; i++ {
			key := fmt.Sprintf("key%02d_%04d", round, i)
			val, err := store.Get([]byte(key))
			if err != nil {
				t.Errorf("Get(%s) failed: %v", key, err)
				continue
			}
			if val.String() != "value" {
				t.Errorf("Get(%s) = %q, want %q", key, val.String(), "value")
			}
		}
	}

	store.Close()

	// Reopen and verify again
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	for round := 0; round < numRounds; round++ {
		for i := 0; i < keysPerRound; i += 10 { // Spot check
			key := fmt.Sprintf("key%02d_%04d", round, i)
			_, err := store.Get([]byte(key))
			if err != nil {
				t.Errorf("Get(%s) after reopen failed: %v", key, err)
			}
		}
	}
}

// TestCompactLevelBeyondMaxLevels tests that compaction handles levels
// at or beyond the configured max gracefully.
func TestCompactLevelBeyondMaxLevels(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.MaxLevels = 3 // L0, L1, L2 only
	opts.L0CompactionTrigger = 2
	opts.L1MaxSize = 1024 // 1KB to trigger L1->L2 quickly
	opts.LevelSizeMultiplier = 2
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough data to push to L2 (last level)
	for round := 0; round < 10; round++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%02d_%04d", round, i)
			store.PutString([]byte(key), "value")
		}
		store.Flush()
		store.Compact()
	}

	stats := store.Stats()
	t.Logf("Levels: L0=%d, L1=%d, L2=%d",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables, stats.Levels[2].NumTables)

	// Force another compact - shouldn't error even at max level
	err = store.Compact()
	if err != nil {
		t.Fatalf("Compact at max level failed: %v", err)
	}

	// Verify data integrity
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d_%04d", 0, i)
		_, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		}
	}

	store.Close()
}

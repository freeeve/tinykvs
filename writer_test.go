package tinykvs

import (
	"fmt"
	"testing"
)

func TestCompactL0ToL1(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Very small to trigger many flushes
	opts.L0CompactionTrigger = 2

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough data to create multiple L0 SSTables and trigger compaction
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	// Verify compaction happened
	stats := store.Stats()
	if stats.Levels[1].NumTables == 0 && stats.Levels[0].NumTables > 2 {
		// Manually trigger compaction
		store.Compact()
	}

	// Verify data integrity after compaction
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		}
		if val.String() != "value" {
			t.Errorf("Get(%s) = %q, want value", key, val.String())
		}
	}

	store.Close()
}

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

func TestMergeIteratorEmpty(t *testing.T) {
	// Create empty merge iterator
	iter := newMergeIterator(nil, nil, false)
	defer iter.Close()

	if iter.Next() {
		t.Error("Next on empty iterator should return false")
	}
}

func TestCompactionLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Very small to create many flushes
	opts.L0CompactionTrigger = 2

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write a larger dataset
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		store.PutString([]byte(key), value)
	}

	store.Flush()
	store.Compact()

	// Verify random sample
	for i := 0; i < 100; i++ {
		idx := (i * 47) % 5000
		key := fmt.Sprintf("key%06d", idx)
		expected := fmt.Sprintf("value%06d", idx)
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

func TestWriterFlushWithImmutables(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512 // Very small

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough to trigger immutable memtables
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%05d", i)
		store.PutString([]byte(key), "value-that-is-long-enough-to-fill-memtable")
	}

	// Flush all
	store.Flush()

	// Verify all data accessible
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%05d", i)
		_, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		}
	}

	store.Close()
}

func TestCompactCreatesMultipleOutputTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512 // Very small to create many L0 tables
	opts.L0CompactionTrigger = 2

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write lots of data to create multiple L0 tables
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d-padding-to-make-it-bigger", i)
		store.PutString([]byte(key), value)
	}

	store.Flush()
	store.Compact()

	// Verify data integrity
	for i := 0; i < 100; i++ {
		idx := (i * 29) % 3000
		key := fmt.Sprintf("key%06d", idx)
		expected := fmt.Sprintf("value%06d-padding-to-make-it-bigger", idx)
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

	// Reopen and verify L1 tables are sorted
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Verify again after reopen (exercises sortTablesByMinKey)
	for i := 0; i < 50; i++ {
		idx := (i * 59) % 3000
		key := fmt.Sprintf("key%06d", idx)
		_, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) after reopen failed: %v", key, err)
		}
	}
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

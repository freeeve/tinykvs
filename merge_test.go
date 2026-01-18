package tinykvs

import (
	"fmt"
	"testing"
	"time"
)

func TestMergeIteratorEmpty(t *testing.T) {
	// Create empty merge iterator
	iter := newMergeIterator(nil, nil, false)
	defer iter.Close()

	if iter.Next() {
		t.Error("Next on empty iterator should return false")
	}
}

// TestMergeIteratorBlockLifetime tests that blocks are properly managed
// during k-way merge iteration across multiple SSTables.
func TestMergeIteratorBlockLifetime(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	opts.BlockSize = 128    // Very small to create many blocks
	opts.BlockCacheSize = 0 // Force all blocks to be read/released

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Create 3 SSTables with interleaved key ranges
	// SSTable 1: a000, a003, a006, ...
	// SSTable 2: a001, a004, a007, ...
	// SSTable 3: a002, a005, a008, ...
	for sstNum := 0; sstNum < 3; sstNum++ {
		for i := sstNum; i < 300; i += 3 {
			key := fmt.Sprintf("a%03d", i)
			value := fmt.Sprintf("val%03d-sst%d", i, sstNum)
			store.PutString([]byte(key), value)
		}
		store.Flush()
	}

	// Compact - merge iterator must alternate between blocks from all 3 tables
	err = store.Compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify sequential order is correct
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("a%03d", i)
		sstNum := i % 3
		expectedValue := fmt.Sprintf("val%03d-sst%d", i, sstNum)

		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expectedValue {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expectedValue)
		}
	}
}

// TestMergeTablesMultipleOutputs tests that mergeTables correctly creates
// multiple output SSTables when the data exceeds maxTableSize (64MB).
// This test is skipped in short mode due to the large data volume required.
func TestMergeTablesMultipleOutputs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large data test in short mode")
	}

	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 16 * 1024 * 1024   // 16MB memtable
	opts.L0CompactionTrigger = 100         // Don't auto-compact
	opts.WALSyncMode = WALSyncNone         // Fast writes
	opts.CompressionType = CompressionNone // Disable compression so we hit size limits
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write ~150MB of data to ensure we get multiple output tables after compaction
	// With no compression, 150K * 1KB = ~150MB which exceeds 2x 64MB limit
	t.Log("Writing 150K entries (~150MB uncompressed)...")
	value := make([]byte, 1000) // 1KB value
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < 150000; i++ {
		key := fmt.Sprintf("key%08d", i)
		if err := store.PutBytes([]byte(key), value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
		if i%25000 == 0 && i > 0 {
			t.Logf("Written %d entries...", i)
		}
	}
	store.Flush()

	stats := store.Stats()
	t.Logf("Before compact: L0 tables=%d, L0 size=%d bytes",
		stats.Levels[0].NumTables, stats.Levels[0].Size)

	// Compact - should create multiple L1 tables
	t.Log("Compacting...")
	err = store.Compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	stats = store.Stats()
	t.Logf("After compact: L0=%d, L1=%d, L2=%d tables",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables, stats.Levels[2].NumTables)
	for i, level := range stats.Levels {
		if level.NumTables > 0 {
			t.Logf("  L%d: %d tables, %d bytes", i, level.NumTables, level.Size)
		}
	}

	// Count total tables across all levels
	totalTables := 0
	for _, level := range stats.Levels {
		totalTables += level.NumTables
	}

	// With ~150MB of data and 64MB max table size, we should have multiple tables
	if totalTables < 2 {
		t.Errorf("Expected multiple tables after compaction, got %d total", totalTables)
	}

	// Verify data integrity (spot check)
	t.Log("Verifying data...")
	for i := 0; i < 100000; i += 1000 {
		key := fmt.Sprintf("key%08d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if len(val.Bytes) != 1000 {
			t.Errorf("Get(%s) returned %d bytes, want 1000", key, len(val.Bytes))
		}
	}

	store.Close()
}

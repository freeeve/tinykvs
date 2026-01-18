package tinykvs

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkFlush measures flush performance with various memtable sizes.
func BenchmarkFlush(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				opts := DefaultOptions(dir)
				opts.MemtableSize = 256 * 1024 * 1024 // Large to prevent auto-flush
				opts.WALSyncMode = WALSyncNone        // Remove WAL sync overhead for this test

				store, err := Open(dir, opts)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}

				// Fill memtable
				for j := 0; j < size; j++ {
					key := fmt.Sprintf("key%08d", j)
					value := fmt.Sprintf("value%08d", j)
					store.PutString([]byte(key), value)
				}

				b.StartTimer()
				err = store.Flush()
				b.StopTimer()

				if err != nil {
					b.Fatalf("Flush failed: %v", err)
				}
				store.Close()
			}
		})
	}
}

// BenchmarkFlushComponents breaks down flush into its components.
func BenchmarkFlushComponents(b *testing.B) {
	size := 10000

	b.Run("full_flush", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			opts := DefaultOptions(dir)
			opts.MemtableSize = 256 * 1024 * 1024
			opts.WALSyncMode = WALSyncNone

			store, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Open failed: %v", err)
			}

			for j := 0; j < size; j++ {
				key := fmt.Sprintf("key%08d", j)
				value := fmt.Sprintf("value%08d", j)
				store.PutString([]byte(key), value)
			}

			b.StartTimer()
			store.Flush()
			b.StopTimer()
			store.Close()
		}
	})

	b.Run("sync_only", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			opts := DefaultOptions(dir)
			opts.MemtableSize = 256 * 1024 * 1024
			opts.WALSyncMode = WALSyncNone

			store, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Open failed: %v", err)
			}

			for j := 0; j < size; j++ {
				key := fmt.Sprintf("key%08d", j)
				value := fmt.Sprintf("value%08d", j)
				store.PutString([]byte(key), value)
			}

			b.StartTimer()
			store.Sync()
			b.StopTimer()
			store.Close()
		}
	})
}

// BenchmarkSync measures WAL sync performance.
func BenchmarkSync(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256 * 1024 * 1024
	opts.WALSyncMode = WALSyncNone

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-fill some data
	for j := 0; j < 1000; j++ {
		key := fmt.Sprintf("key%08d", j)
		value := fmt.Sprintf("value%08d", j)
		store.PutString([]byte(key), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Sync()
	}
}

// BenchmarkFlushWithExistingData measures flush time when store already has data.
// This simulates a real-world scenario with incremental updates.
func BenchmarkFlushWithExistingData(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256 * 1024 * 1024 // Large to control when flush happens
	opts.WALSyncMode = WALSyncNone
	opts.L0CompactionTrigger = 100 // Don't auto-compact during test

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	// Pre-fill with 1M keys
	b.Log("Pre-filling 1M keys...")
	for j := 0; j < 1_000_000; j++ {
		key := fmt.Sprintf("key%08d", j)
		value := fmt.Sprintf("value%08d-initial", j)
		store.PutString([]byte(key), value)

		// Flush every 100k to create SSTables
		if (j+1)%100_000 == 0 {
			store.Flush()
			b.Logf("  Flushed %d keys", j+1)
		}
	}

	// Compact to organize data
	store.Compact()

	stats := store.Stats()
	b.Logf("Store ready: L0=%d, L1=%d tables",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Update 10k random keys (simulating incremental updates)
		for j := 0; j < 10_000; j++ {
			keyIdx := (i*10_000 + j) % 1_000_000
			key := fmt.Sprintf("key%08d", keyIdx)
			value := fmt.Sprintf("value%08d-update%d", keyIdx, i)
			store.PutString([]byte(key), value)
		}

		b.StartTimer()
		store.Flush()
	}

	store.Close()
}

// BenchmarkReadsDuringFlush measures read latency while flush is happening.
func BenchmarkReadsDuringFlush(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256 * 1024 * 1024
	opts.WALSyncMode = WALSyncNone
	opts.L0CompactionTrigger = 100

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-fill with 100k keys and flush to SSTable
	for j := 0; j < 100_000; j++ {
		key := fmt.Sprintf("key%08d", j)
		value := fmt.Sprintf("value%08d", j)
		store.PutString([]byte(key), value)
	}
	store.Flush()

	// Add 10k more keys to memtable (will be flushed during test)
	for j := 0; j < 10_000; j++ {
		key := fmt.Sprintf("update%08d", j)
		value := fmt.Sprintf("value%08d", j)
		store.PutString([]byte(key), value)
	}

	// Measure reads while concurrent flush is happening
	var wg sync.WaitGroup
	readLatencies := make([]time.Duration, 0, b.N)
	var mu sync.Mutex

	// Start background flush
	wg.Add(1)
	go func() {
		defer wg.Done()
		store.Flush()
	}()

	// Measure read latencies
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % 100_000
		key := fmt.Sprintf("key%08d", keyIdx)

		start := time.Now()
		_, err := store.Get([]byte(key))
		elapsed := time.Since(start)

		if err != nil && err != ErrKeyNotFound {
			b.Errorf("Get failed: %v", err)
		}

		mu.Lock()
		readLatencies = append(readLatencies, elapsed)
		mu.Unlock()
	}
	b.StopTimer()

	wg.Wait()

	// Report latency stats
	if len(readLatencies) > 0 {
		var total time.Duration
		var max time.Duration
		for _, lat := range readLatencies {
			total += lat
			if lat > max {
				max = lat
			}
		}
		avg := total / time.Duration(len(readLatencies))
		b.Logf("Read latencies: avg=%v, max=%v, count=%d", avg, max, len(readLatencies))
	}
}

// BenchmarkReadsNoFlush measures read latency baseline (no concurrent flush).
func BenchmarkReadsNoFlush(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256 * 1024 * 1024
	opts.WALSyncMode = WALSyncNone

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-fill with 100k keys and flush to SSTable
	for j := 0; j < 100_000; j++ {
		key := fmt.Sprintf("key%08d", j)
		value := fmt.Sprintf("value%08d", j)
		store.PutString([]byte(key), value)
	}
	store.Flush()

	readLatencies := make([]time.Duration, 0, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % 100_000
		key := fmt.Sprintf("key%08d", keyIdx)

		start := time.Now()
		_, err := store.Get([]byte(key))
		elapsed := time.Since(start)

		if err != nil && err != ErrKeyNotFound {
			b.Errorf("Get failed: %v", err)
		}

		readLatencies = append(readLatencies, elapsed)
	}
	b.StopTimer()

	// Report latency stats
	if len(readLatencies) > 0 {
		var total time.Duration
		var max time.Duration
		for _, lat := range readLatencies {
			total += lat
			if lat > max {
				max = lat
			}
		}
		avg := total / time.Duration(len(readLatencies))
		b.Logf("Read latencies: avg=%v, max=%v, count=%d", avg, max, len(readLatencies))
	}
}

// BenchmarkSyncWithExistingData measures Sync() time with existing data.
func BenchmarkSyncWithExistingData(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256 * 1024 * 1024
	opts.WALSyncMode = WALSyncNone
	opts.L0CompactionTrigger = 100

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	// Pre-fill with 1M keys
	b.Log("Pre-filling 1M keys...")
	for j := 0; j < 1_000_000; j++ {
		key := fmt.Sprintf("key%08d", j)
		value := fmt.Sprintf("value%08d-initial", j)
		store.PutString([]byte(key), value)

		if (j+1)%100_000 == 0 {
			store.Flush()
			b.Logf("  Flushed %d keys", j+1)
		}
	}

	store.Compact()

	stats := store.Stats()
	b.Logf("Store ready: L0=%d, L1=%d tables",
		stats.Levels[0].NumTables, stats.Levels[1].NumTables)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Update 10k keys
		for j := 0; j < 10_000; j++ {
			keyIdx := (i*10_000 + j) % 1_000_000
			key := fmt.Sprintf("key%08d", keyIdx)
			value := fmt.Sprintf("value%08d-update%d", keyIdx, i)
			store.PutString([]byte(key), value)
		}

		b.StartTimer()
		store.Sync()
	}

	store.Close()
}

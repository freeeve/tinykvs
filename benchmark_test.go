package tinykvs

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// BenchmarkStoreReadCacheSize tests read performance with different cache sizes
func BenchmarkStoreReadCacheSize(b *testing.B) {
	cacheSizes := []struct {
		name string
		size int64
	}{
		{"0MB", 0},
		{"64MB", 64 * 1024 * 1024},
		{"128MB", 128 * 1024 * 1024},
		{"256MB", 256 * 1024 * 1024},
	}

	// Create a store with data for benchmarking
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024 // 1MB to trigger flushes

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	// Insert 100K keys to create multiple SSTables
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		store.PutString([]byte(key), value)
	}
	store.Flush()
	store.Close()

	for _, cs := range cacheSizes {
		b.Run(cs.name, func(b *testing.B) {
			opts := DefaultOptions(dir)
			opts.BlockCacheSize = cs.size

			store, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Open failed: %v", err)
			}
			defer store.Close()

			// Warm up cache with sequential reads
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key%08d", i)
				store.Get([]byte(key))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Random read
				idx := rand.Intn(numKeys)
				key := fmt.Sprintf("key%08d", idx)
				store.Get([]byte(key))
			}

			stats := store.Stats()
			b.ReportMetric(stats.CacheStats.HitRate(), "hit%")
		})
	}
}

// BenchmarkStoreSequentialRead tests sequential read performance
func BenchmarkStoreSequentialRead(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	numKeys := 50000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()
	store.Close()

	store, _ = Open(dir, DefaultOptions(dir))
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%numKeys)
		store.Get([]byte(key))
	}
}

// BenchmarkStoreRandomRead tests random read performance
func BenchmarkStoreRandomRead(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024

	store, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	numKeys := 50000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()
	store.Close()

	store, _ = Open(dir, DefaultOptions(dir))
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", rand.Intn(numKeys))
		store.Get([]byte(key))
	}
}

// BenchmarkStoreWriteSequential tests sequential write performance
func BenchmarkStoreWriteSequential(b *testing.B) {
	dir := b.TempDir()
	store, _ := Open(dir, DefaultOptions(dir))
	defer store.Close()

	value := StringValue("benchmark-value-that-is-reasonably-sized-for-testing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.Put([]byte(key), value)
	}
}

// BenchmarkStoreWriteRandom tests random write performance
func BenchmarkStoreWriteRandom(b *testing.B) {
	dir := b.TempDir()
	store, _ := Open(dir, DefaultOptions(dir))
	defer store.Close()

	value := StringValue("benchmark-value-that-is-reasonably-sized-for-testing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", rand.Intn(1000000))
		store.Put([]byte(key), value)
	}
}

// BenchmarkStoreMixedWorkload tests 80% read / 20% write workload
func BenchmarkStoreMixedWorkload(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)

	store, _ := Open(dir, opts)
	defer store.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}

	value := StringValue("new-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% write
			key := fmt.Sprintf("key%08d", rand.Intn(100000))
			store.Put([]byte(key), value)
		} else {
			// 80% read
			key := fmt.Sprintf("key%08d", rand.Intn(10000))
			store.Get([]byte(key))
		}
	}
}

// BenchmarkStoreLoad tests loading/opening a store with existing data
func BenchmarkStoreLoad(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512 * 1024 // Small to create many SSTables

	// Create store with data
	store, _ := Open(dir, opts)
	for i := 0; i < 50000; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()
	store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, _ := Open(dir, DefaultOptions(dir))
		store.Close()
	}
}

// BenchmarkStoreValueSizes tests performance with different value sizes
func BenchmarkStoreValueSizes(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"256B", 256},
		{"1KB", 1024},
		{"4KB", 4096},
		{"16KB", 16384},
	}

	for _, s := range sizes {
		b.Run(s.name+"/write", func(b *testing.B) {
			dir := b.TempDir()
			store, _ := Open(dir, DefaultOptions(dir))
			defer store.Close()

			value := BytesValue(make([]byte, s.size))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key%08d", i)
				store.Put([]byte(key), value)
			}
		})
	}
}

// BenchmarkStoreFlush tests flush performance
func BenchmarkStoreFlush(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4 * 1024 * 1024 // 4MB

	store, _ := Open(dir, opts)
	defer store.Close()

	// Fill memtable
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value-that-is-reasonably-sized")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Flush()
		// Refill
		for j := 0; j < 1000; j++ {
			key := fmt.Sprintf("key%08d", i*1000+j)
			store.PutString([]byte(key), "value")
		}
	}
}

// BenchmarkStoreConcurrentReads tests concurrent read performance
func BenchmarkStoreConcurrentReads(b *testing.B) {
	dir := b.TempDir()
	store, _ := Open(dir, DefaultOptions(dir))

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%08d", i%10000)
			store.Get([]byte(key))
			i++
		}
	})

	store.Close()
}

// Test100MRecords tests performance with 100 million records.
// Run with: go test -v -run Test100MRecords -timeout 60m
// For low memory: GOMEMLIMIT=256MiB go test -v -run Test100MRecords -timeout 60m
func Test100MRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100M record test in short mode")
	}

	numRecords := 100_000_000
	numReads := 100_000
	batchSize := 1_000_000

	dir := t.TempDir()
	opts := UltraLowMemoryOptions(dir) // Use ultra low memory settings
	opts.MemtableSize = 1 * 1024 * 1024   // 1MB memtable
	opts.BlockCacheSize = 0               // No cache during writes
	opts.WALSyncMode = WALSyncNone        // Faster writes for benchmark

	t.Logf("Starting 100M record test")
	t.Logf("Directory: %s", dir)

	// Phase 1: Write 100M records
	t.Logf("\n=== WRITE PHASE ===")
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	writeStart := time.Now()
	lastReport := writeStart

	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%09d", i)
		value := fmt.Sprintf("value%09d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}

		// Progress report every batch
		if (i+1)%batchSize == 0 {
			elapsed := time.Since(lastReport)
			totalElapsed := time.Since(writeStart)
			rate := float64(batchSize) / elapsed.Seconds()
			avgRate := float64(i+1) / totalElapsed.Seconds()
			pct := float64(i+1) / float64(numRecords) * 100

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			t.Logf("Written: %dM / %dM (%.1f%%) | Batch: %.0f ops/sec | Avg: %.0f ops/sec | Heap: %dMB | Sys: %dMB",
				(i+1)/1_000_000, numRecords/1_000_000, pct,
				rate, avgRate, m.HeapAlloc/1024/1024, m.Sys/1024/1024)

			lastReport = time.Now()

			// Periodic GC to help keep memory in check
			if (i+1)%(10*batchSize) == 0 {
				runtime.GC()
			}
		}
	}

	// Final flush
	t.Logf("Flushing...")
	flushStart := time.Now()
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Logf("Flush completed in %v", time.Since(flushStart))

	writeDuration := time.Since(writeStart)
	writeRate := float64(numRecords) / writeDuration.Seconds()
	t.Logf("\nWrite complete: %d records in %v (%.0f ops/sec)", numRecords, writeDuration, writeRate)

	// Get stats before closing
	stats := store.Stats()
	t.Logf("SSTables: L0=%d", stats.Levels[0].NumTables)
	for i, level := range stats.Levels {
		if level.NumTables > 0 {
			t.Logf("  L%d: %d tables, %d keys, %d bytes", i, level.NumTables, level.NumKeys, level.Size)
		}
	}

	store.Close()

	// Phase 2: Read tests BEFORE compaction
	t.Logf("\n=== READ PHASE (BEFORE COMPACTION) ===")
	t.Logf("L0 tables overlap - must check all of them for each read")

	runReadTests(t, dir, opts, numRecords, numReads, "Before compaction")

	// Phase 3: Compact all L0 to L1
	t.Logf("\n=== COMPACTION PHASE ===")
	compactOpts := opts
	compactOpts.BlockCacheSize = 0 // No cache during compaction
	store, err = Open(dir, compactOpts)
	if err != nil {
		t.Fatalf("Open for compaction failed: %v", err)
	}

	compactStart := time.Now()
	t.Logf("Compacting L0 to L1...")
	if err := store.Compact(); err != nil {
		t.Logf("Compaction error: %v", err)
	}
	t.Logf("Compaction completed in %v", time.Since(compactStart))

	stats = store.Stats()
	for i, level := range stats.Levels {
		if level.NumTables > 0 {
			t.Logf("  L%d: %d tables, %d keys, %d bytes", i, level.NumTables, level.NumKeys, level.Size)
		}
	}
	store.Close()

	// Force GC after compaction
	runtime.GC()

	// Phase 4: Read tests AFTER compaction
	t.Logf("\n=== READ PHASE (AFTER COMPACTION) ===")
	t.Logf("L1 tables are disjoint - can binary search")

	runReadTests(t, dir, opts, numRecords, numReads, "After compaction")

	t.Logf("\n=== TEST COMPLETE ===")
}

// runReadTests runs read benchmarks with different cache sizes
func runReadTests(t *testing.T, dir string, baseOpts Options, numRecords, numReads int, label string) {
	cacheSizes := []struct {
		name string
		size int64
	}{
		{"0MB", 0},
		{"64MB", 64 * 1024 * 1024},
	}

	for _, cs := range cacheSizes {
		opts := baseOpts
		opts.BlockCacheSize = cs.size

		t.Logf("\n--- %s | Cache: %s ---", label, cs.name)
		loadStart := time.Now()
		store, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		t.Logf("Store loaded in %v", time.Since(loadStart))

		// Random reads
		readStart := time.Now()
		found := 0
		for i := 0; i < numReads; i++ {
			idx := rand.Intn(numRecords)
			key := fmt.Sprintf("key%09d", idx)
			_, err := store.Get([]byte(key))
			if err == nil {
				found++
			}
		}
		readDuration := time.Since(readStart)
		readRate := float64(numReads) / readDuration.Seconds()

		stats := store.Stats()
		t.Logf("Random reads: %d in %v (%.0f ops/sec) | Found: %d | Hit rate: %.2f%%",
			numReads, readDuration, readRate, found, stats.CacheStats.HitRate())

		// Sequential reads
		seqStart := time.Now()
		for i := 0; i < numReads; i++ {
			key := fmt.Sprintf("key%09d", i)
			store.Get([]byte(key))
		}
		seqDuration := time.Since(seqStart)
		seqRate := float64(numReads) / seqDuration.Seconds()
		t.Logf("Sequential reads: %d in %v (%.0f ops/sec)", numReads, seqDuration, seqRate)

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("Memory: Heap=%dMB | Sys=%dMB | GC cycles=%d",
			m.HeapAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)

		store.Close()
	}
}

package tinykvs

import (
	"fmt"
	"math/rand"
	"testing"
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

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/freeeve/tinykvs"
)

func main() {
	numRecords := flag.Int("records", 100_000_000, "Number of records to write")
	numReads := flag.Int("reads", 100_000, "Number of reads to perform per test")
	dataDir := flag.String("dir", "/tmp/tinykvs-bench", "Data directory")
	skipWrite := flag.Bool("skip-write", false, "Skip write phase (use existing data)")
	skipCompact := flag.Bool("skip-compact", false, "Skip compaction phase")
	memtableSize := flag.Int64("memtable", 1*1024*1024, "Memtable size in bytes (default 1MB)")
	disableBloom := flag.Bool("no-bloom", true, "Disable bloom filters (default true for low memory)")
	flag.Parse()

	fmt.Println("=== TinyKVS Benchmark ===")
	fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("GOMEMLIMIT: %d bytes\n", debug.SetMemoryLimit(-1))
	fmt.Printf("Records: %d\n", *numRecords)
	fmt.Printf("Memtable: %d MB\n", *memtableSize/1024/1024)
	fmt.Printf("Bloom filters: %v\n", !*disableBloom)
	fmt.Printf("Data dir: %s\n", *dataDir)
	fmt.Println()

	// Configure options for low memory
	opts := tinykvs.UltraLowMemoryOptions(*dataDir)
	opts.MemtableSize = *memtableSize
	opts.DisableBloomFilter = *disableBloom
	opts.WALSyncMode = tinykvs.WALSyncNone

	if !*skipWrite {
		runWrite(*dataDir, opts, *numRecords)
	}

	// Read before compaction
	fmt.Println("\n=== READ BEFORE COMPACTION ===")
	runReads(*dataDir, opts, *numRecords, *numReads)

	if !*skipCompact {
		runCompact(*dataDir, opts)
	}

	// Read after compaction
	fmt.Println("\n=== READ AFTER COMPACTION ===")
	runReads(*dataDir, opts, *numRecords, *numReads)

	fmt.Println("\n=== BENCHMARK COMPLETE ===")
}

func runWrite(dir string, opts tinykvs.Options, numRecords int) {
	fmt.Println("=== WRITE PHASE ===")

	// Clean up old data
	os.RemoveAll(dir)

	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open store: %v\n", err)
		os.Exit(1)
	}

	batchSize := 1_000_000
	writeStart := time.Now()
	lastReport := writeStart

	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%012d", i)
		value := fmt.Sprintf("val%012d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			fmt.Fprintf(os.Stderr, "Put failed at %d: %v\n", i, err)
			os.Exit(1)
		}

		if (i+1)%batchSize == 0 {
			elapsed := time.Since(lastReport)
			totalElapsed := time.Since(writeStart)
			rate := float64(batchSize) / elapsed.Seconds()
			avgRate := float64(i+1) / totalElapsed.Seconds()
			pct := float64(i+1) / float64(numRecords) * 100

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			fmt.Printf("[%s] Written: %dM / %dM (%.1f%%) | Batch: %.0f/s | Avg: %.0f/s | Heap: %dMB | Sys: %dMB\n",
				totalElapsed.Truncate(time.Second), (i+1)/1_000_000, numRecords/1_000_000, pct,
				rate, avgRate, m.HeapAlloc/1024/1024, m.Sys/1024/1024)

			lastReport = time.Now()

			// Periodic GC to keep memory in check
			if (i+1)%(10*batchSize) == 0 {
				runtime.GC()
				debug.FreeOSMemory()
			}
		}
	}

	// Final flush
	fmt.Println("Flushing...")
	flushStart := time.Now()
	if err := store.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "Flush failed: %v\n", err)
	}
	fmt.Printf("Flush completed in %v\n", time.Since(flushStart))

	writeDuration := time.Since(writeStart)
	writeRate := float64(numRecords) / writeDuration.Seconds()
	fmt.Printf("\nWrite complete: %d records in %v (%.0f ops/sec)\n",
		numRecords, writeDuration, writeRate)

	// Print stats
	stats := store.Stats()
	for i, level := range stats.Levels {
		if level.NumTables > 0 {
			fmt.Printf("  L%d: %d tables, %d keys, %d bytes\n",
				i, level.NumTables, level.NumKeys, level.Size)
		}
	}

	store.Close()
}

func runCompact(dir string, opts tinykvs.Options) {
	fmt.Println("\n=== COMPACTION PHASE ===")

	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open store: %v\n", err)
		return
	}

	compactStart := time.Now()
	fmt.Println("Compacting L0 -> L1...")

	if err := store.Compact(); err != nil {
		fmt.Printf("Compaction error: %v\n", err)
	}

	fmt.Printf("Compaction completed in %v\n", time.Since(compactStart))

	// Print stats
	stats := store.Stats()
	for i, level := range stats.Levels {
		if level.NumTables > 0 {
			fmt.Printf("  L%d: %d tables, %d keys, %d bytes\n",
				i, level.NumTables, level.NumKeys, level.Size)
		}
	}

	store.Close()

	// GC after compaction
	runtime.GC()
	debug.FreeOSMemory()
}

func runReads(dir string, opts tinykvs.Options, numRecords, numReads int) {
	cacheSizes := []struct {
		name string
		size int64
	}{
		{"0MB", 0},
		{"64MB", 64 * 1024 * 1024},
	}

	for _, cs := range cacheSizes {
		opts.BlockCacheSize = cs.size

		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open store: %v\n", err)
			continue
		}

		// Random reads
		readStart := time.Now()
		found := 0
		for i := 0; i < numReads; i++ {
			idx := rand.Intn(numRecords)
			key := fmt.Sprintf("key%012d", idx)
			if _, err := store.Get([]byte(key)); err == nil {
				found++
			}
		}
		readDuration := time.Since(readStart)
		readRate := float64(numReads) / readDuration.Seconds()

		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		fmt.Printf("Cache %s: %d reads in %v (%.0f/s) | Found: %d | Heap: %dMB | Sys: %dMB\n",
			cs.name, numReads, readDuration, readRate, found,
			m.HeapAlloc/1024/1024, m.Sys/1024/1024)

		store.Close()
	}
}

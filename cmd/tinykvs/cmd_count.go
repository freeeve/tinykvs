package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/freeeve/tinykvs"
)

// prefixStats holds count and size statistics for a prefix
type prefixStats struct {
	count            int64
	compressedSize   int64 // apportioned from block sizes
	uncompressedSize int64 // actual key + value bytes
}

func cmdCount(args []string) {
	fs := flag.NewFlagSet("count", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	prefixLen := fs.Int("prefix-len", 1, "Number of prefix bytes to group by")
	workers := fs.Int("workers", 8, "Number of parallel workers")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	if *prefixLen < 1 || *prefixLen > 8 {
		fmt.Fprintln(os.Stderr, "Error: -prefix-len must be between 1 and 8")
		os.Exit(1)
	}

	// Open store to acquire lock (we read SSTables directly for speed)
	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// Read manifest
	manifestPath := filepath.Join(storeDir, "MANIFEST")
	manifest, err := tinykvs.OpenManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening manifest: %v\n", err)
		os.Exit(1)
	}
	tables := manifest.Tables()
	manifest.Close()

	fmt.Fprintf(os.Stderr, "Scanning %d SSTables with %d workers (prefix length: %d)...\n",
		len(tables), *workers, *prefixLen)

	// Process tables in parallel
	var total int64
	var totalCompressed int64
	var totalUncompressed int64
	var processed int64
	stats := make([]map[string]*prefixStats, *workers)
	for i := range stats {
		stats[i] = make(map[string]*prefixStats)
	}

	tableChan := make(chan *tinykvs.TableMeta, len(tables))
	for _, meta := range tables {
		tableChan <- meta
	}
	close(tableChan)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localStats := stats[workerID]

			for meta := range tableChan {
				path := filepath.Join(storeDir, fmt.Sprintf("%06d.sst", meta.ID))

				sst, err := tinykvs.OpenSSTable(meta.ID, path)
				if err != nil {
					continue
				}

				file, err := os.Open(path)
				if err != nil {
					sst.Close()
					continue
				}

				for _, entry := range sst.Index.Entries {
					blockData := make([]byte, entry.BlockSize)
					if _, err := file.ReadAt(blockData, int64(entry.BlockOffset)); err != nil {
						continue
					}

					block, err := tinykvs.DecodeBlock(blockData, false)
					if err != nil {
						continue
					}

					// Count keys and uncompressed size per prefix in this block
					type blockStats struct {
						count            int
						uncompressedSize int64
					}
					blockPrefixStats := make(map[string]*blockStats)
					totalKeysInBlock := 0
					for _, e := range block.Entries {
						if len(e.Key) >= *prefixLen {
							prefix := string(e.Key[:*prefixLen])
							bs := blockPrefixStats[prefix]
							if bs == nil {
								bs = &blockStats{}
								blockPrefixStats[prefix] = bs
							}
							bs.count++
							bs.uncompressedSize += int64(len(e.Key) + len(e.Value))
							totalKeysInBlock++
						}
					}

					// Apportion block size to prefixes based on key count
					blockSize := int64(entry.BlockSize)
					for prefix, bs := range blockPrefixStats {
						ps := localStats[prefix]
						if ps == nil {
							ps = &prefixStats{}
							localStats[prefix] = ps
						}
						ps.count += int64(bs.count)
						ps.uncompressedSize += bs.uncompressedSize
						// Apportion compressed size proportionally
						apportionedSize := blockSize * int64(bs.count) / int64(totalKeysInBlock)
						ps.compressedSize += apportionedSize
						atomic.AddInt64(&total, int64(bs.count))
						atomic.AddInt64(&totalCompressed, apportionedSize)
						atomic.AddInt64(&totalUncompressed, bs.uncompressedSize)
					}
					block.Release()
				}

				file.Close()
				sst.Close()
				p := atomic.AddInt64(&processed, 1)
				if p%20 == 0 {
					fmt.Fprintf(os.Stderr, "\rProcessed %d/%d tables, %d million keys...",
						p, len(tables), atomic.LoadInt64(&total)/1000000)
				}
			}
		}(w)
	}

	wg.Wait()

	// Merge stats
	merged := make(map[string]*prefixStats)
	for _, s := range stats {
		for k, v := range s {
			if merged[k] == nil {
				merged[k] = &prefixStats{}
			}
			merged[k].count += v.count
			merged[k].compressedSize += v.compressedSize
			merged[k].uncompressedSize += v.uncompressedSize
		}
	}

	overallRatio := float64(totalUncompressed) / float64(totalCompressed)
	fmt.Fprintf(os.Stderr, "\rProcessed %d tables, %d total keys\n", len(tables), total)
	fmt.Fprintf(os.Stderr, "Compressed: %s, Uncompressed: %s, Ratio: %.2fx\n\n",
		formatBytes(totalCompressed), formatBytes(totalUncompressed), overallRatio)

	// Sort by count descending
	type kv struct {
		prefix string
		stats  *prefixStats
	}
	var sorted []kv
	for k, v := range merged {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].stats.count > sorted[j].stats.count
	})

	// Print results
	fmt.Printf("%-20s %12s %7s %10s %12s %6s\n", "Prefix", "Count", "Pct", "Compressed", "Uncompressed", "Ratio")
	fmt.Println(strings.Repeat("-", 73))
	for _, kv := range sorted {
		countPct := float64(kv.stats.count) / float64(total) * 100
		ratio := float64(kv.stats.uncompressedSize) / float64(kv.stats.compressedSize)
		fmt.Printf("%-20s %12d %6.2f%% %10s %12s %5.2fx\n",
			formatKey([]byte(kv.prefix)), kv.stats.count, countPct,
			formatBytes(kv.stats.compressedSize),
			formatBytes(kv.stats.uncompressedSize), ratio)
	}
}

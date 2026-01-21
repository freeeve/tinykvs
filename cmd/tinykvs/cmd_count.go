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

// prefixStats holds count and size statistics for a prefix.
type prefixStats struct {
	count            int64
	compressedSize   int64 // apportioned from block sizes
	uncompressedSize int64 // actual key + value bytes
}

// countContext holds shared state for the count operation.
type countContext struct {
	storeDir          string
	prefixLen         int
	numTables         int
	total             int64
	totalCompressed   int64
	totalUncompressed int64
	processed         int64
}

// blockPrefixStats tracks per-prefix statistics within a single block.
type blockPrefixStats struct {
	count            int
	uncompressedSize int64
}

func (c *CLI) cmdCount(args []string) int {
	fs := flag.NewFlagSet("count", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Path to store directory (required)")
	prefixLen := fs.Int("prefix-len", 1, "Number of prefix bytes to group by")
	workers := fs.Int("workers", 8, "Number of parallel workers")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	if *prefixLen < 1 || *prefixLen > 8 {
		fmt.Fprintln(c.Stderr, "Error: -prefix-len must be between 1 and 8")
		return 1
	}

	tables, code := c.openStoreAndGetTables(storeDir)
	if code != 0 {
		return code
	}

	fmt.Fprintf(c.Stderr, "Scanning %d SSTables with %d workers (prefix length: %d)...\n",
		len(tables), *workers, *prefixLen)

	ctx := &countContext{
		storeDir:  storeDir,
		prefixLen: *prefixLen,
		numTables: len(tables),
	}

	workerStats := runCountWorkers(ctx, tables, *workers)
	merged := mergeWorkerStats(workerStats)

	c.printCountSummary(ctx, len(tables))
	c.printCountResults(merged, ctx.total)
	return 0
}

// openStoreAndGetTables opens the store for locking and returns the table list.
func (c *CLI) openStoreAndGetTables(storeDir string) (map[uint32]*tinykvs.TableMeta, int) {
	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, "Error opening store: %v\n", err)
		return nil, 1
	}
	defer store.Close()

	manifestPath := filepath.Join(storeDir, "MANIFEST")
	manifest, err := tinykvs.OpenManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(c.Stderr, "Error opening manifest: %v\n", err)
		return nil, 1
	}
	tables := manifest.Tables()
	manifest.Close()
	return tables, 0
}

// runCountWorkers processes tables in parallel and returns per-worker stats.
func runCountWorkers(ctx *countContext, tables map[uint32]*tinykvs.TableMeta, numWorkers int) []map[string]*prefixStats {
	stats := make([]map[string]*prefixStats, numWorkers)
	for i := range stats {
		stats[i] = make(map[string]*prefixStats)
	}

	tableChan := make(chan *tinykvs.TableMeta, len(tables))
	for _, meta := range tables {
		tableChan <- meta
	}
	close(tableChan)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			processTablesForWorker(ctx, tableChan, stats[workerID])
		}(w)
	}
	wg.Wait()

	return stats
}

// processTablesForWorker processes tables from the channel and updates local stats.
func processTablesForWorker(ctx *countContext, tableChan <-chan *tinykvs.TableMeta, localStats map[string]*prefixStats) {
	for meta := range tableChan {
		processTable(ctx, meta, localStats)
	}
}

// processTable processes a single SSTable and updates statistics.
func processTable(ctx *countContext, meta *tinykvs.TableMeta, localStats map[string]*prefixStats) {
	path := filepath.Join(ctx.storeDir, fmt.Sprintf("%06d.sst", meta.ID))

	sst, err := tinykvs.OpenSSTable(meta.ID, path)
	if err != nil {
		return
	}
	defer sst.Close()

	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	for _, entry := range sst.Index.Entries {
		processIndexEntry(ctx, file, entry, localStats)
	}

	p := atomic.AddInt64(&ctx.processed, 1)
	if p%20 == 0 {
		fmt.Fprintf(os.Stderr, "\rProcessed %d/%d tables, %d million keys...",
			p, ctx.numTables, atomic.LoadInt64(&ctx.total)/1000000)
	}
}

// processIndexEntry processes a single block from an SSTable index entry.
func processIndexEntry(ctx *countContext, file *os.File, entry tinykvs.IndexEntry, localStats map[string]*prefixStats) {
	blockData := make([]byte, entry.BlockSize)
	if _, err := file.ReadAt(blockData, int64(entry.BlockOffset)); err != nil {
		return
	}

	block, err := tinykvs.DecodeBlock(blockData, false)
	if err != nil {
		return
	}
	defer block.Release()

	blockStats, totalKeysInBlock := collectBlockStats(block, ctx.prefixLen)
	if totalKeysInBlock == 0 {
		return
	}

	apportionBlockStats(ctx, localStats, blockStats, int64(entry.BlockSize), totalKeysInBlock)
}

// collectBlockStats gathers per-prefix statistics from a block's entries.
func collectBlockStats(block *tinykvs.Block, prefixLen int) (map[string]*blockPrefixStats, int) {
	stats := make(map[string]*blockPrefixStats)
	totalKeys := 0

	for _, e := range block.Entries {
		if len(e.Key) >= prefixLen {
			prefix := string(e.Key[:prefixLen])
			bs := stats[prefix]
			if bs == nil {
				bs = &blockPrefixStats{}
				stats[prefix] = bs
			}
			bs.count++
			bs.uncompressedSize += int64(len(e.Key) + len(e.Value))
			totalKeys++
		}
	}

	return stats, totalKeys
}

// apportionBlockStats distributes block statistics to prefix stats.
func apportionBlockStats(ctx *countContext, localStats map[string]*prefixStats, blockStats map[string]*blockPrefixStats, blockSize int64, totalKeys int) {
	for prefix, bs := range blockStats {
		ps := localStats[prefix]
		if ps == nil {
			ps = &prefixStats{}
			localStats[prefix] = ps
		}
		ps.count += int64(bs.count)
		ps.uncompressedSize += bs.uncompressedSize

		apportionedSize := blockSize * int64(bs.count) / int64(totalKeys)
		ps.compressedSize += apportionedSize

		atomic.AddInt64(&ctx.total, int64(bs.count))
		atomic.AddInt64(&ctx.totalCompressed, apportionedSize)
		atomic.AddInt64(&ctx.totalUncompressed, bs.uncompressedSize)
	}
}

// mergeWorkerStats combines statistics from all workers into a single map.
func mergeWorkerStats(workerStats []map[string]*prefixStats) map[string]*prefixStats {
	merged := make(map[string]*prefixStats)
	for _, s := range workerStats {
		for k, v := range s {
			if merged[k] == nil {
				merged[k] = &prefixStats{}
			}
			merged[k].count += v.count
			merged[k].compressedSize += v.compressedSize
			merged[k].uncompressedSize += v.uncompressedSize
		}
	}
	return merged
}

// printCountSummary prints the processing summary to stderr.
func (c *CLI) printCountSummary(ctx *countContext, numTables int) {
	overallRatio := float64(ctx.totalUncompressed) / float64(ctx.totalCompressed)
	fmt.Fprintf(c.Stderr, "\rProcessed %d tables, %d total keys\n", numTables, ctx.total)
	fmt.Fprintf(c.Stderr, "Compressed: %s, Uncompressed: %s, Ratio: %.2fx\n\n",
		formatBytes(ctx.totalCompressed), formatBytes(ctx.totalUncompressed), overallRatio)
}

// printCountResults prints the sorted prefix statistics table.
func (c *CLI) printCountResults(merged map[string]*prefixStats, total int64) {
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

	fmt.Fprintf(c.Stdout, "%-20s %12s %7s %10s %12s %6s\n", "Prefix", "Count", "Pct", "Compressed", "Uncompressed", "Ratio")
	fmt.Fprintln(c.Stdout, strings.Repeat("-", 73))
	for _, kv := range sorted {
		countPct := float64(kv.stats.count) / float64(total) * 100
		ratio := float64(kv.stats.uncompressedSize) / float64(kv.stats.compressedSize)
		fmt.Fprintf(c.Stdout, "%-20s %12d %6.2f%% %10s %12s %5.2fx\n",
			formatKey([]byte(kv.prefix)), kv.stats.count, countPct,
			formatBytes(kv.stats.compressedSize),
			formatBytes(kv.stats.uncompressedSize), ratio)
	}
}

package main

import (
	"bufio"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/freeeve/tinykvs"
)

// Version info - set via ldflags at build time
// go build -ldflags "-X main.Version=1.0.0 -X main.GitCommit=$(git rev-parse --short HEAD)"
var (
	Version   = "dev"
	GitCommit = "unknown"
)

// versionString returns the version, only appending commit if not already in version
func versionString() string {
	if strings.Contains(Version, GitCommit) {
		return Version
	}
	return Version + " (" + GitCommit + ")"
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "version", "-v", "--version":
		fmt.Println("tinykvs " + versionString())
		return
	case "get":
		cmdGet(args)
	case "put":
		cmdPut(args)
	case "delete":
		cmdDelete(args)
	case "scan":
		cmdScan(args)
	case "stats":
		cmdStats(args)
	case "count":
		cmdCount(args)
	case "compact":
		cmdCompact(args)
	case "repair":
		cmdRepair(args)
	case "info":
		cmdInfo(args)
	case "export":
		cmdExport(args)
	case "import":
		cmdImport(args)
	case "shell":
		cmdShell(args)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`tinykvs - CLI for TinyKVS stores

Usage:
  tinykvs <command> [options]

Commands:
  get      Get a value by key
  put      Put a key-value pair
  delete   Delete a key
  scan     Scan keys with a prefix
  stats    Show store statistics
  count    Count keys by prefix
  compact  Compact the store
  repair   Remove orphan SST files
  info     Show which level/SSTable contains a key
  export   Export store to CSV file
  import   Import data from CSV file
  shell    Interactive SQL-like query shell

Environment:
  TINYKVS_STORE  Default store directory (used if -dir not specified)

Examples:
  export TINYKVS_STORE=/path/to/store
  tinykvs get -key mykey
  tinykvs put -key mykey -value "hello world"
  tinykvs scan -prefix-hex 05 -limit 10

  # Or specify -dir explicitly:
  tinykvs get -dir /path/to/store -key-hex 0506abcd
  tinykvs stats -dir /path/to/store

Use "tinykvs <command> -h" for more information about a command.`)
}

// getDir returns the store directory from flag or TINYKVS_STORE env var.
func getDir(flagDir string) string {
	if flagDir != "" {
		return flagDir
	}
	if envDir := os.Getenv("TINYKVS_STORE"); envDir != "" {
		return envDir
	}
	return ""
}

// requireDir returns the directory or exits with an error.
func requireDir(flagDir string) string {
	dir := getDir(flagDir)
	if dir == "" {
		fmt.Fprintln(os.Stderr, "Error: -dir is required (or set TINYKVS_STORE)")
		os.Exit(1)
	}
	return dir
}

func cmdGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	key := fs.String("key", "", "Key to get (string)")
	keyHex := fs.String("key-hex", "", "Key to get (hex encoded)")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	var keyBytes []byte
	if *keyHex != "" {
		var err error
		keyBytes, err = hex.DecodeString(*keyHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding hex key: %v\n", err)
			os.Exit(1)
		}
	} else if *key != "" {
		keyBytes = []byte(*key)
	} else {
		fmt.Fprintln(os.Stderr, "Error: -key or -key-hex is required")
		fs.Usage()
		os.Exit(1)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	val, err := store.Get(keyBytes)
	if err != nil {
		if err == tinykvs.ErrKeyNotFound {
			fmt.Println("Key not found")
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	printValue(keyBytes, val)
}

func cmdPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	key := fs.String("key", "", "Key (string)")
	keyHex := fs.String("key-hex", "", "Key (hex encoded)")
	value := fs.String("value", "", "Value (string)")
	valueHex := fs.String("value-hex", "", "Value (hex encoded)")
	valueInt := fs.Int64("value-int", 0, "Value (int64)")
	valueFloat := fs.Float64("value-float", 0, "Value (float64)")
	valueBool := fs.String("value-bool", "", "Value (bool: true/false)")
	flush := fs.Bool("flush", false, "Flush after put")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	var keyBytes []byte
	if *keyHex != "" {
		var err error
		keyBytes, err = hex.DecodeString(*keyHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding hex key: %v\n", err)
			os.Exit(1)
		}
	} else if *key != "" {
		keyBytes = []byte(*key)
	} else {
		fmt.Fprintln(os.Stderr, "Error: -key or -key-hex is required")
		fs.Usage()
		os.Exit(1)
	}

	// Determine value type
	var val tinykvs.Value
	valueSet := false

	if *value != "" {
		val = tinykvs.StringValue(*value)
		valueSet = true
	}
	if *valueHex != "" {
		if valueSet {
			fmt.Fprintln(os.Stderr, "Error: only one value flag allowed")
			os.Exit(1)
		}
		bytes, err := hex.DecodeString(*valueHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding hex value: %v\n", err)
			os.Exit(1)
		}
		val = tinykvs.BytesValue(bytes)
		valueSet = true
	}
	if fs.Lookup("value-int").Value.String() != "0" || containsFlag(args, "-value-int") {
		if valueSet {
			fmt.Fprintln(os.Stderr, "Error: only one value flag allowed")
			os.Exit(1)
		}
		val = tinykvs.Int64Value(*valueInt)
		valueSet = true
	}
	if fs.Lookup("value-float").Value.String() != "0" || containsFlag(args, "-value-float") {
		if valueSet {
			fmt.Fprintln(os.Stderr, "Error: only one value flag allowed")
			os.Exit(1)
		}
		val = tinykvs.Float64Value(*valueFloat)
		valueSet = true
	}
	if *valueBool != "" {
		if valueSet {
			fmt.Fprintln(os.Stderr, "Error: only one value flag allowed")
			os.Exit(1)
		}
		switch strings.ToLower(*valueBool) {
		case "true", "1", "yes":
			val = tinykvs.BoolValue(true)
		case "false", "0", "no":
			val = tinykvs.BoolValue(false)
		default:
			fmt.Fprintf(os.Stderr, "Error: invalid bool value: %s\n", *valueBool)
			os.Exit(1)
		}
		valueSet = true
	}

	if !valueSet {
		fmt.Fprintln(os.Stderr, "Error: a value flag is required (-value, -value-hex, -value-int, -value-float, -value-bool)")
		fs.Usage()
		os.Exit(1)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.Put(keyBytes, val); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *flush {
		if err := store.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "Error flushing: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("OK\n")
}

func containsFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag || strings.HasPrefix(a, flag+"=") {
			return true
		}
	}
	return false
}

func cmdDelete(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	key := fs.String("key", "", "Key to delete (string)")
	keyHex := fs.String("key-hex", "", "Key to delete (hex encoded)")
	flush := fs.Bool("flush", false, "Flush after delete")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	var keyBytes []byte
	if *keyHex != "" {
		var err error
		keyBytes, err = hex.DecodeString(*keyHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding hex key: %v\n", err)
			os.Exit(1)
		}
	} else if *key != "" {
		keyBytes = []byte(*key)
	} else {
		fmt.Fprintln(os.Stderr, "Error: -key or -key-hex is required")
		fs.Usage()
		os.Exit(1)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.Delete(keyBytes); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *flush {
		if err := store.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "Error flushing: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("OK\n")
}

func cmdScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	prefix := fs.String("prefix", "", "Key prefix (string)")
	prefixHex := fs.String("prefix-hex", "", "Key prefix (hex encoded)")
	limit := fs.Int("limit", 100, "Maximum number of results")
	keysOnly := fs.Bool("keys-only", false, "Only print keys, not values")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	var prefixBytes []byte
	if *prefixHex != "" {
		var err error
		prefixBytes, err = hex.DecodeString(*prefixHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding hex prefix: %v\n", err)
			os.Exit(1)
		}
	} else if *prefix != "" {
		prefixBytes = []byte(*prefix)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	count := 0
	err = store.ScanPrefix(prefixBytes, func(key []byte, val tinykvs.Value) bool {
		if count >= *limit {
			return false
		}
		if *keysOnly {
			fmt.Printf("%s\n", formatKey(key))
		} else {
			printValue(key, val)
		}
		count++
		return true
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error scanning: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "\n(%d results)\n", count)
}

func cmdStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	stats := store.Stats()

	fmt.Printf("Store: %s\n\n", storeDir)
	fmt.Printf("Memtable:\n")
	fmt.Printf("  Size:  %s\n", formatBytes(stats.MemtableSize))
	fmt.Printf("  Keys:  %d\n", stats.MemtableCount)

	fmt.Printf("\nIndex Memory: %s\n", formatBytes(stats.IndexMemory))

	fmt.Printf("\nCache:\n")
	fmt.Printf("  Size:    %s\n", formatBytes(stats.CacheStats.Size))
	fmt.Printf("  Entries: %d\n", stats.CacheStats.Entries)
	fmt.Printf("  Hits:    %d\n", stats.CacheStats.Hits)
	fmt.Printf("  Misses:  %d\n", stats.CacheStats.Misses)
	if stats.CacheStats.Hits+stats.CacheStats.Misses > 0 {
		hitRate := float64(stats.CacheStats.Hits) / float64(stats.CacheStats.Hits+stats.CacheStats.Misses) * 100
		fmt.Printf("  Hit Rate: %.1f%%\n", hitRate)
	}

	fmt.Printf("\nLevels:\n")
	var totalSize int64
	var totalKeys uint64
	var totalTables int
	for _, level := range stats.Levels {
		if level.NumTables > 0 {
			fmt.Printf("  L%d: %3d tables, %10s, %12d keys\n",
				level.Level, level.NumTables, formatBytes(level.Size), level.NumKeys)
			totalSize += level.Size
			totalKeys += level.NumKeys
			totalTables += level.NumTables
		}
	}
	fmt.Printf("\nTotal: %d tables, %s, %d keys\n", totalTables, formatBytes(totalSize), totalKeys)
}

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

func cmdCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// Show before stats
	statsBefore := store.Stats()
	var l0Before, l1Before int
	for _, level := range statsBefore.Levels {
		if level.Level == 0 {
			l0Before = level.NumTables
		} else if level.Level == 1 {
			l1Before = level.NumTables
		}
	}

	fmt.Printf("Before: L0=%d tables, L1=%d tables\n", l0Before, l1Before)
	fmt.Printf("Compacting...\n")

	if err := store.Compact(); err != nil {
		fmt.Fprintf(os.Stderr, "Error compacting: %v\n", err)
		os.Exit(1)
	}

	// Show after stats
	statsAfter := store.Stats()
	var l0After, l1After int
	for _, level := range statsAfter.Levels {
		if level.Level == 0 {
			l0After = level.NumTables
		} else if level.Level == 1 {
			l1After = level.NumTables
		}
	}

	fmt.Printf("After:  L0=%d tables, L1=%d tables\n", l0After, l1After)
	fmt.Printf("Done\n")
}

func cmdRepair(args []string) {
	fs := flag.NewFlagSet("repair", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory")
	dryRun := fs.Bool("dry-run", false, "Show what would be deleted without deleting")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	// Read manifest to get valid table IDs
	manifestPath := filepath.Join(storeDir, "MANIFEST")
	manifest, err := tinykvs.OpenManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening manifest: %v\n", err)
		os.Exit(1)
	}
	tables := manifest.Tables()
	manifest.Close()

	// Build set of valid IDs
	validIDs := make(map[uint32]bool)
	for _, meta := range tables {
		validIDs[meta.ID] = true
	}

	// Find orphan files
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading directory: %v\n", err)
		os.Exit(1)
	}

	var orphans []string
	var orphanSize int64
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".sst")
		id, err := parseUint32(name)
		if err != nil {
			continue
		}
		if !validIDs[id] {
			info, _ := e.Info()
			orphans = append(orphans, e.Name())
			orphanSize += info.Size()
		}
	}

	if len(orphans) == 0 {
		fmt.Println("No orphan files found")
		return
	}

	fmt.Printf("Found %d orphan files (%.2f GB)\n", len(orphans), float64(orphanSize)/1e9)

	if *dryRun {
		fmt.Println("\nOrphan files (dry run - not deleting):")
		for _, name := range orphans {
			fmt.Printf("  %s\n", name)
		}
		return
	}

	// Delete orphans
	var deleted int
	var deletedSize int64
	for _, name := range orphans {
		path := filepath.Join(storeDir, name)
		info, _ := os.Stat(path)
		if err := os.Remove(path); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete %s: %v\n", name, err)
			continue
		}
		deleted++
		deletedSize += info.Size()
		fmt.Printf("Deleted: %s\n", name)
	}

	fmt.Printf("\nDeleted %d files, recovered %.2f GB\n", deleted, float64(deletedSize)/1e9)
}

func parseUint32(s string) (uint32, error) {
	v, err := fmt.Sscanf(s, "%d", new(uint32))
	if err != nil || v != 1 {
		return 0, fmt.Errorf("invalid uint32: %s", s)
	}
	var result uint32
	fmt.Sscanf(s, "%d", &result)
	return result, nil
}

func cmdInfo(args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory")
	key := fs.String("key", "", "Key to look up (string)")
	keyHex := fs.String("key-hex", "", "Key to look up (hex encoded)")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	var keyBytes []byte
	if *keyHex != "" {
		var err error
		keyBytes, err = hex.DecodeString(*keyHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding hex key: %v\n", err)
			os.Exit(1)
		}
	} else if *key != "" {
		keyBytes = []byte(*key)
	} else {
		fmt.Fprintln(os.Stderr, "Error: -key or -key-hex is required")
		fs.Usage()
		os.Exit(1)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// Check memtable first
	val, err := store.Get(keyBytes)
	if err == tinykvs.ErrKeyNotFound {
		fmt.Println("Key not found")
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Key: %s\n", formatKey(keyBytes))
	fmt.Printf("Value type: %s\n", valueTypeName(val.Type))

	// Find which SSTable contains it
	location := store.FindKey(keyBytes)
	if location != nil {
		fmt.Printf("Location: L%d SSTable %06d\n", location.Level, location.TableID)
	} else {
		fmt.Printf("Location: memtable\n")
	}
}

func valueTypeName(t tinykvs.ValueType) string {
	switch t {
	case tinykvs.ValueTypeInt64:
		return "int64"
	case tinykvs.ValueTypeFloat64:
		return "float64"
	case tinykvs.ValueTypeBool:
		return "bool"
	case tinykvs.ValueTypeString:
		return "string"
	case tinykvs.ValueTypeBytes:
		return "bytes"
	case tinykvs.ValueTypeTombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

func cmdExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory")
	output := fs.String("output", "", "Output file path (required)")
	prefix := fs.String("prefix", "", "Only export keys with this prefix")
	prefixHex := fs.String("prefix-hex", "", "Only export keys with this prefix (hex)")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	if *output == "" {
		fmt.Fprintln(os.Stderr, "Error: -output is required")
		fs.Usage()
		os.Exit(1)
	}

	var prefixBytes []byte
	if *prefixHex != "" {
		var err error
		prefixBytes, err = hex.DecodeString(*prefixHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding hex prefix: %v\n", err)
			os.Exit(1)
		}
	} else if *prefix != "" {
		prefixBytes = []byte(*prefix)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	file, err := os.Create(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	writer.Write([]string{"key", "type", "value"})

	var count int64
	err = store.ScanPrefix(prefixBytes, func(key []byte, val tinykvs.Value) bool {
		var valueStr string
		switch val.Type {
		case tinykvs.ValueTypeInt64:
			valueStr = strconv.FormatInt(val.Int64, 10)
		case tinykvs.ValueTypeFloat64:
			valueStr = strconv.FormatFloat(val.Float64, 'g', -1, 64)
		case tinykvs.ValueTypeBool:
			valueStr = strconv.FormatBool(val.Bool)
		case tinykvs.ValueTypeString, tinykvs.ValueTypeBytes:
			valueStr = hex.EncodeToString(val.Bytes)
		}

		writer.Write([]string{
			hex.EncodeToString(key),
			valueTypeName(val.Type),
			valueStr,
		})

		count++
		if count%100000 == 0 {
			fmt.Fprintf(os.Stderr, "\rExported %d keys...", count)
		}
		return true
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "\nError scanning: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "\rExported %d keys to %s\n", count, *output)
}

func cmdImport(args []string) {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory")
	input := fs.String("input", "", "Input file path (required)")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	if *input == "" {
		fmt.Fprintln(os.Stderr, "Error: -input is required")
		fs.Usage()
		os.Exit(1)
	}

	file, err := os.Open(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	// Skip header
	if _, err := reader.Read(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading header: %v\n", err)
		os.Exit(1)
	}

	var count int64
	var errors int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errors++
			continue
		}
		if len(record) != 3 {
			errors++
			continue
		}

		keyBytes, err := hex.DecodeString(record[0])
		if err != nil {
			errors++
			continue
		}

		typeName := record[1]
		valueStr := record[2]

		var val tinykvs.Value
		switch typeName {
		case "int64":
			v, err := strconv.ParseInt(valueStr, 10, 64)
			if err != nil {
				errors++
				continue
			}
			val = tinykvs.Int64Value(v)
		case "float64":
			v, err := strconv.ParseFloat(valueStr, 64)
			if err != nil {
				errors++
				continue
			}
			val = tinykvs.Float64Value(v)
		case "bool":
			v, err := strconv.ParseBool(valueStr)
			if err != nil {
				errors++
				continue
			}
			val = tinykvs.BoolValue(v)
		case "string":
			bytes, err := hex.DecodeString(valueStr)
			if err != nil {
				errors++
				continue
			}
			val = tinykvs.StringValue(string(bytes))
		case "bytes":
			bytes, err := hex.DecodeString(valueStr)
			if err != nil {
				errors++
				continue
			}
			val = tinykvs.BytesValue(bytes)
		default:
			errors++
			continue
		}

		if err := store.Put(keyBytes, val); err != nil {
			errors++
			continue
		}
		count++

		if count%100000 == 0 {
			fmt.Fprintf(os.Stderr, "\rImported %d keys...", count)
		}
	}

	if err := store.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "\nError flushing: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "\rImported %d keys (%d errors)\n", count, errors)
}

func cmdShell(args []string) {
	fs := flag.NewFlagSet("shell", flag.ExitOnError)
	dir := fs.String("dir", "", "Path to store directory (required)")
	fs.Parse(args)

	storeDir := requireDir(*dir)

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	shell := NewShell(store)
	shell.Run()
}

func printValue(key []byte, val tinykvs.Value) {
	fmt.Printf("%s = ", formatKey(key))
	switch val.Type {
	case tinykvs.ValueTypeInt64:
		fmt.Printf("(int64) %d\n", val.Int64)
	case tinykvs.ValueTypeFloat64:
		fmt.Printf("(float64) %f\n", val.Float64)
	case tinykvs.ValueTypeBool:
		fmt.Printf("(bool) %t\n", val.Bool)
	case tinykvs.ValueTypeString:
		fmt.Printf("(string) %q\n", string(val.Bytes))
	case tinykvs.ValueTypeBytes:
		if len(val.Bytes) <= 64 {
			fmt.Printf("(bytes) %s\n", hex.EncodeToString(val.Bytes))
		} else {
			fmt.Printf("(bytes) %s... (%d bytes)\n", hex.EncodeToString(val.Bytes[:64]), len(val.Bytes))
		}
	case tinykvs.ValueTypeTombstone:
		fmt.Printf("(deleted)\n")
	default:
		fmt.Printf("(unknown type %d)\n", val.Type)
	}
}

func formatKey(key []byte) string {
	// If key is printable ASCII, show as string, otherwise hex
	printable := true
	for _, b := range key {
		if b < 32 || b > 126 {
			printable = false
			break
		}
	}
	if printable && len(key) > 0 {
		return string(key)
	}
	return "0x" + hex.EncodeToString(key)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

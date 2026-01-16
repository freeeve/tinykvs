package main

import (
	"encoding/hex"
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

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
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
	var processed int64
	counts := make([]map[string]int64, *workers)
	for i := range counts {
		counts[i] = make(map[string]int64)
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
			localCounts := counts[workerID]

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

					for _, e := range block.Entries {
						if len(e.Key) >= *prefixLen {
							prefix := string(e.Key[:*prefixLen])
							localCounts[prefix]++
							atomic.AddInt64(&total, 1)
						}
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

	// Merge counts
	merged := make(map[string]int64)
	for _, c := range counts {
		for k, v := range c {
			merged[k] += v
		}
	}

	fmt.Fprintf(os.Stderr, "\rProcessed %d tables, %d total keys\n\n", len(tables), total)

	// Sort by count descending
	type kv struct {
		prefix string
		count  int64
	}
	var sorted []kv
	for k, v := range merged {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	// Print results
	fmt.Printf("%-20s %15s %8s\n", "Prefix", "Count", "Percent")
	fmt.Println(strings.Repeat("-", 45))
	for _, kv := range sorted {
		pct := float64(kv.count) / float64(total) * 100
		fmt.Printf("%-20s %15d %7.2f%%\n", formatKey([]byte(kv.prefix)), kv.count, pct)
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

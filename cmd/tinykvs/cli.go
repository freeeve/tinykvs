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
	"strconv"

	"github.com/freeeve/tinykvs"
)

// CLI holds injectable dependencies for testability.
type CLI struct {
	Stdout io.Writer
	Stderr io.Writer
	Getenv func(string) string
}

// NewCLI creates a CLI with default OS dependencies.
func NewCLI() *CLI {
	return &CLI{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Getenv: os.Getenv,
	}
}

// Run executes the CLI and returns an exit code (0 = success, 1 = error).
func (c *CLI) Run(args []string) int {
	if len(args) < 2 {
		c.printUsage()
		return 1
	}

	cmd := args[1]
	cmdArgs := args[2:]

	switch cmd {
	case "version", "-v", "--version":
		fmt.Fprintln(c.Stdout, "tinykvs "+versionString())
		return 0
	case "get":
		return c.cmdGet(cmdArgs)
	case "put":
		return c.cmdPut(cmdArgs)
	case "delete":
		return c.cmdDelete(cmdArgs)
	case "scan":
		return c.cmdScan(cmdArgs)
	case "stats":
		return c.cmdStats(cmdArgs)
	case "count":
		return c.cmdCount(cmdArgs)
	case "compact":
		return c.cmdCompact(cmdArgs)
	case "repair":
		return c.cmdRepair(cmdArgs)
	case "info":
		return c.cmdInfo(cmdArgs)
	case "export":
		return c.cmdExport(cmdArgs)
	case "import":
		return c.cmdImport(cmdArgs)
	case "shell":
		return c.cmdShell(cmdArgs)
	case "help", "-h", "--help":
		c.printUsage()
		return 0
	default:
		fmt.Fprintf(c.Stderr, "Unknown command: %s\n\n", cmd)
		c.printUsage()
		return 1
	}
}

func (c *CLI) printUsage() {
	fmt.Fprintln(c.Stdout, `tinykvs - CLI for TinyKVS stores

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
func (c *CLI) getDir(flagDir string) string {
	if flagDir != "" {
		return flagDir
	}
	if envDir := c.Getenv("TINYKVS_STORE"); envDir != "" {
		return envDir
	}
	return ""
}

// requireDir returns the directory or returns empty string and false if not set.
func (c *CLI) requireDir(flagDir string) (string, bool) {
	dir := c.getDir(flagDir)
	if dir == "" {
		fmt.Fprintln(c.Stderr, "Error: -dir is required (or set TINYKVS_STORE)")
		return "", false
	}
	return dir, true
}

func (c *CLI) cmdGet(args []string) int {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	key := fs.String("key", "", "Key to get (string)")
	keyHex := fs.String(flagKeyHex, "", "Key to get (hex encoded)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	var keyBytes []byte
	if *keyHex != "" {
		var err error
		keyBytes, err = hex.DecodeString(*keyHex)
		if err != nil {
			fmt.Fprintf(c.Stderr, msgErrDecodeHexKey, err)
			return 1
		}
	} else if *key != "" {
		keyBytes = []byte(*key)
	} else {
		fmt.Fprintln(c.Stderr, msgErrKeyRequired)
		fs.Usage()
		return 1
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	val, err := store.Get(keyBytes)
	if err != nil {
		if err == tinykvs.ErrKeyNotFound {
			fmt.Fprintln(c.Stdout, "Key not found")
			return 0
		}
		fmt.Fprintf(c.Stderr, msgErr, err)
		return 1
	}

	c.printValue(keyBytes, val)
	return 0
}

func (c *CLI) cmdPut(args []string) int {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	pf := &putFlags{args: args}
	fs.StringVar(&pf.key, "key", "", "Key (string)")
	fs.StringVar(&pf.keyHex, flagKeyHex, "", "Key (hex encoded)")
	fs.StringVar(&pf.value, "value", "", "Value (string)")
	fs.StringVar(&pf.valueHex, "value-hex", "", "Value (hex encoded)")
	fs.Int64Var(&pf.valueInt, "value-int", 0, "Value (int64)")
	fs.Float64Var(&pf.valueFloat, "value-float", 0, "Value (float64)")
	fs.StringVar(&pf.valueBool, "value-bool", "", "Value (bool: true/false)")
	fs.BoolVar(&pf.flush, "flush", false, "Flush after put")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	keyBytes, err := parseCLIKey(pf.key, pf.keyHex)
	if err != nil {
		fmt.Fprintln(c.Stderr, err)
		fs.Usage()
		return 1
	}

	val, err := pf.parseValue()
	if err != nil {
		fmt.Fprintln(c.Stderr, err)
		fs.Usage()
		return 1
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	if err := store.Put(keyBytes, val); err != nil {
		fmt.Fprintf(c.Stderr, msgErr, err)
		return 1
	}

	if pf.flush {
		if err := store.Flush(); err != nil {
			fmt.Fprintf(c.Stderr, "Error flushing: %v\n", err)
			return 1
		}
	}

	fmt.Fprintln(c.Stdout, "OK")
	return 0
}

func (c *CLI) cmdDelete(args []string) int {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	key := fs.String("key", "", "Key to delete (string)")
	keyHex := fs.String(flagKeyHex, "", "Key to delete (hex encoded)")
	flush := fs.Bool("flush", false, "Flush after delete")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	var keyBytes []byte
	if *keyHex != "" {
		var err error
		keyBytes, err = hex.DecodeString(*keyHex)
		if err != nil {
			fmt.Fprintf(c.Stderr, msgErrDecodeHexKey, err)
			return 1
		}
	} else if *key != "" {
		keyBytes = []byte(*key)
	} else {
		fmt.Fprintln(c.Stderr, msgErrKeyRequired)
		fs.Usage()
		return 1
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	if err := store.Delete(keyBytes); err != nil {
		fmt.Fprintf(c.Stderr, msgErr, err)
		return 1
	}

	if *flush {
		if err := store.Flush(); err != nil {
			fmt.Fprintf(c.Stderr, "Error flushing: %v\n", err)
			return 1
		}
	}

	fmt.Fprintln(c.Stdout, "OK")
	return 0
}

func (c *CLI) cmdScan(args []string) int {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	prefix := fs.String("prefix", "", "Key prefix (string)")
	prefixHex := fs.String("prefix-hex", "", "Key prefix (hex encoded)")
	limit := fs.Int("limit", 100, "Maximum number of results")
	keysOnly := fs.Bool("keys-only", false, "Only print keys, not values")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	var prefixBytes []byte
	if *prefixHex != "" {
		var err error
		prefixBytes, err = hex.DecodeString(*prefixHex)
		if err != nil {
			fmt.Fprintf(c.Stderr, "Error decoding hex prefix: %v\n", err)
			return 1
		}
	} else if *prefix != "" {
		prefixBytes = []byte(*prefix)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	count := 0
	err = store.ScanPrefix(prefixBytes, func(key []byte, val tinykvs.Value) bool {
		if count >= *limit {
			return false
		}
		if *keysOnly {
			fmt.Fprintf(c.Stdout, "%s\n", formatKey(key))
		} else {
			c.printValue(key, val)
		}
		count++
		return true
	})

	if err != nil {
		fmt.Fprintf(c.Stderr, "Error scanning: %v\n", err)
		return 1
	}

	fmt.Fprintf(c.Stderr, "\n(%d results)\n", count)
	return 0
}

func (c *CLI) cmdStats(args []string) int {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	stats := store.Stats()

	fmt.Fprintf(c.Stdout, "Store: %s\n\n", storeDir)
	fmt.Fprintf(c.Stdout, "Memtable:\n")
	fmt.Fprintf(c.Stdout, "  Size:  %s\n", formatBytes(stats.MemtableSize))
	fmt.Fprintf(c.Stdout, "  Keys:  %d\n", stats.MemtableCount)

	fmt.Fprintf(c.Stdout, "\nIndex Memory: %s\n", formatBytes(stats.IndexMemory))

	fmt.Fprintf(c.Stdout, "\nCache:\n")
	fmt.Fprintf(c.Stdout, "  Size:    %s\n", formatBytes(stats.CacheStats.Size))
	fmt.Fprintf(c.Stdout, "  Entries: %d\n", stats.CacheStats.Entries)
	fmt.Fprintf(c.Stdout, "  Hits:    %d\n", stats.CacheStats.Hits)
	fmt.Fprintf(c.Stdout, "  Misses:  %d\n", stats.CacheStats.Misses)
	if stats.CacheStats.Hits+stats.CacheStats.Misses > 0 {
		hitRate := float64(stats.CacheStats.Hits) / float64(stats.CacheStats.Hits+stats.CacheStats.Misses) * 100
		fmt.Fprintf(c.Stdout, "  Hit Rate: %.1f%%\n", hitRate)
	}

	fmt.Fprintf(c.Stdout, "\nLevels:\n")
	var totalSize int64
	var totalKeys uint64
	var totalTables int
	for _, level := range stats.Levels {
		if level.NumTables > 0 {
			fmt.Fprintf(c.Stdout, "  L%d: %3d tables, %10s, %12d keys\n",
				level.Level, level.NumTables, formatBytes(level.Size), level.NumKeys)
			totalSize += level.Size
			totalKeys += level.NumKeys
			totalTables += level.NumTables
		}
	}
	fmt.Fprintf(c.Stdout, "\nTotal: %d tables, %s, %d keys\n", totalTables, formatBytes(totalSize), totalKeys)
	return 0
}

func (c *CLI) cmdCompact(args []string) int {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
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

	fmt.Fprintf(c.Stdout, "Before: L0=%d tables, L1=%d tables\n", l0Before, l1Before)
	fmt.Fprintln(c.Stdout, "Compacting...")

	if err := store.Compact(); err != nil {
		fmt.Fprintf(c.Stderr, "Error compacting: %v\n", err)
		return 1
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

	fmt.Fprintf(c.Stdout, "After:  L0=%d tables, L1=%d tables\n", l0After, l1After)
	fmt.Fprintln(c.Stdout, "Done")
	return 0
}

func (c *CLI) cmdRepair(args []string) int {
	fs := flag.NewFlagSet("repair", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	dryRun := fs.Bool("dry-run", false, "Show what would be deleted without deleting")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	validIDs, code := c.loadValidTableIDs(storeDir)
	if code != 0 {
		return code
	}

	orphans, orphanSize, code := c.findOrphanFiles(storeDir, validIDs)
	if code != 0 {
		return code
	}

	if len(orphans) == 0 {
		fmt.Fprintln(c.Stdout, "No orphan files found")
		return 0
	}

	fmt.Fprintf(c.Stdout, "Found %d orphan files (%.2f GB)\n", len(orphans), float64(orphanSize)/1e9)

	if *dryRun {
		c.printDryRunOrphans(orphans)
		return 0
	}

	c.deleteOrphanFiles(storeDir, orphans)
	return 0
}

// loadValidTableIDs reads the manifest and returns a set of valid table IDs.
func (c *CLI) loadValidTableIDs(storeDir string) (map[uint32]bool, int) {
	manifestPath := filepath.Join(storeDir, "MANIFEST")
	manifest, err := tinykvs.OpenManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(c.Stderr, "Error opening manifest: %v\n", err)
		return nil, 1
	}
	tables := manifest.Tables()
	manifest.Close()

	validIDs := make(map[uint32]bool)
	for _, meta := range tables {
		validIDs[meta.ID] = true
	}
	return validIDs, 0
}

// findOrphanFiles returns SST files not in the valid ID set.
func (c *CLI) findOrphanFiles(storeDir string, validIDs map[uint32]bool) ([]string, int64, int) {
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		fmt.Fprintf(c.Stderr, "Error reading directory: %v\n", err)
		return nil, 0, 1
	}

	var orphans []string
	var orphanSize int64
	for _, e := range entries {
		if name, size, isOrphan := checkOrphanFile(e, validIDs); isOrphan {
			orphans = append(orphans, name)
			orphanSize += size
		}
	}
	return orphans, orphanSize, 0
}

// printDryRunOrphans prints orphan files without deleting.
func (c *CLI) printDryRunOrphans(orphans []string) {
	fmt.Fprintln(c.Stdout, "\nOrphan files (dry run - not deleting):")
	for _, name := range orphans {
		fmt.Fprintf(c.Stdout, "  %s\n", name)
	}
}

// deleteOrphanFiles deletes the given orphan files.
func (c *CLI) deleteOrphanFiles(storeDir string, orphans []string) {
	var deleted int
	var deletedSize int64
	for _, name := range orphans {
		path := filepath.Join(storeDir, name)
		info, _ := os.Stat(path)
		if err := os.Remove(path); err != nil {
			fmt.Fprintf(c.Stderr, "Failed to delete %s: %v\n", name, err)
			continue
		}
		deleted++
		deletedSize += info.Size()
		fmt.Fprintf(c.Stdout, "Deleted: %s\n", name)
	}
	fmt.Fprintf(c.Stdout, "\nDeleted %d files, recovered %.2f GB\n", deleted, float64(deletedSize)/1e9)
}

func (c *CLI) cmdInfo(args []string) int {
	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	key := fs.String("key", "", "Key to look up (string)")
	keyHex := fs.String(flagKeyHex, "", "Key to look up (hex encoded)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	var keyBytes []byte
	if *keyHex != "" {
		var err error
		keyBytes, err = hex.DecodeString(*keyHex)
		if err != nil {
			fmt.Fprintf(c.Stderr, msgErrDecodeHexKey, err)
			return 1
		}
	} else if *key != "" {
		keyBytes = []byte(*key)
	} else {
		fmt.Fprintln(c.Stderr, msgErrKeyRequired)
		fs.Usage()
		return 1
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	// Check memtable first
	val, err := store.Get(keyBytes)
	if err == tinykvs.ErrKeyNotFound {
		fmt.Fprintln(c.Stdout, "Key not found")
		return 0
	}
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErr, err)
		return 1
	}

	fmt.Fprintf(c.Stdout, "Key: %s\n", formatKey(keyBytes))
	fmt.Fprintf(c.Stdout, "Value type: %s\n", valueTypeName(val.Type))

	// Find which SSTable contains it
	location := store.FindKey(keyBytes)
	if location != nil {
		fmt.Fprintf(c.Stdout, "Location: L%d SSTable %06d\n", location.Level, location.TableID)
	} else {
		fmt.Fprintln(c.Stdout, "Location: memtable")
	}
	return 0
}

func (c *CLI) cmdExport(args []string) int {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	output := fs.String("output", "", "Output file path (required)")
	prefix := fs.String("prefix", "", "Only export keys with this prefix")
	prefixHex := fs.String("prefix-hex", "", "Only export keys with this prefix (hex)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	if *output == "" {
		fmt.Fprintln(c.Stderr, "Error: -output is required")
		fs.Usage()
		return 1
	}

	var prefixBytes []byte
	if *prefixHex != "" {
		var err error
		prefixBytes, err = hex.DecodeString(*prefixHex)
		if err != nil {
			fmt.Fprintf(c.Stderr, "Error decoding hex prefix: %v\n", err)
			return 1
		}
	} else if *prefix != "" {
		prefixBytes = []byte(*prefix)
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	file, err := os.Create(*output)
	if err != nil {
		fmt.Fprintf(c.Stderr, "Error creating output file: %v\n", err)
		return 1
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
			fmt.Fprintf(c.Stderr, "\rExported %d keys...", count)
		}
		return true
	})

	if err != nil {
		fmt.Fprintf(c.Stderr, "\nError scanning: %v\n", err)
		return 1
	}

	fmt.Fprintf(c.Stderr, "\rExported %d keys to %s\n", count, *output)
	return 0
}

func (c *CLI) cmdImport(args []string) int {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	input := fs.String("input", "", "Input file path (required)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	if *input == "" {
		fmt.Fprintln(c.Stderr, "Error: -input is required")
		fs.Usage()
		return 1
	}

	file, err := os.Open(*input)
	if err != nil {
		fmt.Fprintf(c.Stderr, "Error opening input file: %v\n", err)
		return 1
	}
	defer file.Close()

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	count, errors := c.importCSVRecordsToStore(file, store)

	if err := store.Flush(); err != nil {
		fmt.Fprintf(c.Stderr, "\nError flushing: %v\n", err)
		return 1
	}

	fmt.Fprintf(c.Stderr, "\rImported %d keys (%d errors)\n", count, errors)
	return 0
}

// importCSVRecordsToStore reads CSV records and imports them to the store.
func (c *CLI) importCSVRecordsToStore(file *os.File, store *tinykvs.Store) (count, errors int64) {
	reader := csv.NewReader(bufio.NewReader(file))

	if _, err := reader.Read(); err != nil {
		fmt.Fprintf(c.Stderr, "Error reading header: %v\n", err)
		return 0, 1
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errors++
			continue
		}

		if importSingleRecord(store, record) {
			count++
			if count%100000 == 0 {
				fmt.Fprintf(c.Stderr, "\rImported %d keys...", count)
			}
		} else {
			errors++
		}
	}
	return count, errors
}

func (c *CLI) cmdShell(args []string) int {
	fs := flag.NewFlagSet("shell", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	dir := fs.String("dir", "", "Store directory")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	storeDir, ok := c.requireDir(*dir)
	if !ok {
		return 1
	}

	opts := tinykvs.DefaultOptions(storeDir)
	store, err := tinykvs.Open(storeDir, opts)
	if err != nil {
		fmt.Fprintf(c.Stderr, msgErrOpenStore, err)
		return 1
	}
	defer store.Close()

	shell := NewShell(store)
	shell.Run()
	return 0
}

func (c *CLI) printValue(key []byte, val tinykvs.Value) {
	fmt.Fprintf(c.Stdout, "%s = ", formatKey(key))
	switch val.Type {
	case tinykvs.ValueTypeInt64:
		fmt.Fprintf(c.Stdout, "(int64) %d\n", val.Int64)
	case tinykvs.ValueTypeFloat64:
		fmt.Fprintf(c.Stdout, "(float64) %f\n", val.Float64)
	case tinykvs.ValueTypeBool:
		fmt.Fprintf(c.Stdout, "(bool) %t\n", val.Bool)
	case tinykvs.ValueTypeString:
		fmt.Fprintf(c.Stdout, "(string) %q\n", string(val.Bytes))
	case tinykvs.ValueTypeBytes:
		if len(val.Bytes) <= 64 {
			fmt.Fprintf(c.Stdout, "(bytes) %s\n", hex.EncodeToString(val.Bytes))
		} else {
			fmt.Fprintf(c.Stdout, "(bytes) %s... (%d bytes)\n", hex.EncodeToString(val.Bytes[:64]), len(val.Bytes))
		}
	case tinykvs.ValueTypeTombstone:
		fmt.Fprintln(c.Stdout, "(deleted)")
	default:
		fmt.Fprintf(c.Stdout, "(unknown type %d)\n", val.Type)
	}
}

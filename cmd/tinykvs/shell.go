package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/freeeve/tinykvs"
	"github.com/peterh/liner"
)

// Shell provides an interactive SQL-like query interface.
type Shell struct {
	store       *tinykvs.Store
	prompt      string
	historyFile string
	line        *liner.State
}

// NewShell creates a new shell instance.
func NewShell(store *tinykvs.Store) *Shell {
	// History file in user's home directory
	historyFile := ""
	if home, err := os.UserHomeDir(); err == nil {
		historyFile = filepath.Join(home, ".tinykvs_history")
	}

	return &Shell{
		store:       store,
		prompt:      "tinykvs> ",
		historyFile: historyFile,
	}
}

// Run starts the interactive shell.
func (s *Shell) Run() {
	s.line = liner.NewLiner()
	defer s.line.Close()

	s.line.SetCtrlCAborts(true)

	// Load history
	if s.historyFile != "" {
		if f, err := os.Open(s.historyFile); err == nil {
			s.line.ReadHistory(f)
			f.Close()
		}
	}

	fmt.Println("TinyKVS Shell " + versionString())
	fmt.Println("Type \\help for help, \\q to quit")
	fmt.Println()

	for {
		input, err := s.line.Prompt(s.prompt)
		if err != nil {
			if err == liner.ErrPromptAborted {
				fmt.Println("^C")
				continue
			}
			// Ctrl+D (EOF) or other error - exit gracefully
			fmt.Println()
			break
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Add to history
		s.line.AppendHistory(input)

		if !s.execute(input) {
			break
		}
	}

	// Save history
	if s.historyFile != "" {
		if f, err := os.Create(s.historyFile); err == nil {
			s.line.WriteHistory(f)
			f.Close()
		}
	}
}

// execute runs a command. Returns false to exit.
func (s *Shell) execute(line string) bool {
	// Handle shell commands
	if strings.HasPrefix(line, "\\") {
		return s.handleCommand(line)
	}

	// Remove trailing semicolon if present
	line = strings.TrimSuffix(line, ";")

	// Pre-process SQL functions (uint64_be, byte, fnv64, etc.) and concatenation
	line = preprocessFunctions(line)

	// Pre-process: convert "STARTS WITH x'...'" to "LIKE '$$HEX$$...%'"
	// and "STARTS WITH '...'" to "LIKE '...%'"
	line = preprocessStartsWith(line)

	// Extract ORDER BY before SQL parsing (parser doesn't support ORDER BY for kv)
	line, orderBy := ParseOrderBy(line)

	// Parse SQL
	stmt, err := sqlparser.Parse(line)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return true
	}

	switch st := stmt.(type) {
	case *sqlparser.Select:
		s.handleSelect(st, orderBy)
	case *sqlparser.Insert:
		s.handleInsert(st)
	case *sqlparser.Update:
		s.handleUpdate(st)
	case *sqlparser.Delete:
		s.handleDelete(st)
	default:
		fmt.Printf("Unsupported statement type: %T\n", stmt)
	}

	return true
}

func (s *Shell) handleCommand(cmd string) bool {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return true
	}

	switch parts[0] {
	case "\\q", "\\quit", "\\exit":
		fmt.Println("Bye")
		return false
	case "\\help", "\\h", "\\?":
		s.printHelp()
	case "\\stats":
		s.printStats()
	case "\\compact":
		fmt.Println("Compacting...")
		if err := s.store.Compact(); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println("Done")
		}
	case "\\flush":
		if err := s.store.Flush(); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println("Flushed")
		}
	case "\\tables":
		fmt.Println("Table: kv (k TEXT, v TEXT)")
		fmt.Println("  - k: the key (string or hex with x'...')")
		fmt.Println("  - v: the value")
	case "\\explain":
		if len(parts) < 2 {
			fmt.Println("Usage: \\explain <prefix>")
			fmt.Println("  Shows which SSTables contain keys with the given prefix")
			return true
		}
		s.explainPrefix(parts[1])
	case "\\export":
		if len(parts) < 2 {
			fmt.Println("Usage: \\export <filename.csv>")
			return true
		}
		s.exportCSV(parts[1])
	case "\\import":
		if len(parts) < 2 {
			fmt.Println("Usage: \\import <filename.csv>")
			return true
		}
		s.importCSV(parts[1])
	default:
		fmt.Printf("Unknown command: %s\n", parts[0])
		fmt.Println("Type \\help for help")
	}
	return true
}

func (s *Shell) printHelp() {
	fmt.Println(`SQL Commands:
  SELECT * FROM kv WHERE k = 'mykey'
  SELECT * FROM kv WHERE k LIKE 'prefix%'
  SELECT * FROM kv WHERE k BETWEEN 'a' AND 'z' LIMIT 10
  SELECT * FROM kv LIMIT 100
  SELECT v.name, v.age FROM kv WHERE k = 'user:1'    -- record fields
  SELECT v.address.city FROM kv WHERE k = 'user:1'   -- nested fields

  ORDER BY (buffers results for sorting):
  SELECT * FROM kv ORDER BY k DESC LIMIT 10
  SELECT v.name, v.age FROM kv ORDER BY v.age DESC, v.name
  SELECT * FROM kv WHERE k LIKE 'user:%' ORDER BY v.score LIMIT 100

  Aggregations (streaming):
  SELECT count() FROM kv
  SELECT count(), sum(v.age), avg(v.age) FROM kv
  SELECT min(v.score), max(v.score) FROM kv WHERE k LIKE 'user:%'

  INSERT INTO kv (k, v) VALUES ('mykey', 'myvalue')
  INSERT INTO kv VALUES ('mykey', 'myvalue')
  INSERT INTO kv VALUES ('user:1', '{"name":"Alice","age":30}')  -- JSON record
  INSERT INTO kv VALUES ('user:2', x'82a46e616d65a3426f62a361676514')  -- msgpack

  UPDATE kv SET v = 'newvalue' WHERE k = 'mykey'

  DELETE FROM kv WHERE k = 'mykey'
  DELETE FROM kv WHERE k LIKE 'prefix%'

Shell Commands:
  \help, \h, \?      Show this help
  \stats             Show store statistics
  \explain <prefix>  Show which SSTables contain a prefix
  \compact           Run compaction
  \flush             Flush memtable to disk
  \tables            Show table schema
  \export <file>     Export to CSV (key,value format)
  \import <file>     Import from CSV (auto-detects format)
  \q, \quit          Exit shell

CSV Import Formats:
  key,value              2 columns: key + value (auto-detects type)
  key,col1,col2,...      3+ columns: key + fields become a record
  key,name:string,age:int  Type hints: string, int, float, bool, json

Notes:
  - Use single quotes for strings: 'mykey'
  - Use x'...' for hex values: x'deadbeef'
  - JSON strings are auto-detected and stored as records
  - Hex values starting with msgpack map markers are stored as records
  - Access record fields with v.fieldname in SELECT
  - Nested fields: v.a.b (2 levels) or v.` + "`a.b.c`" + ` (deeper)
  - LIKE only supports prefix matching (trailing %)
  - All operations are on the virtual 'kv' table
  - Use up/down arrows to navigate command history`)
}

func (s *Shell) printStats() {
	stats := s.store.Stats()

	fmt.Printf("Memtable: %d keys, %s\n", stats.MemtableCount, formatBytes(stats.MemtableSize))
	fmt.Printf("Cache: %d entries, %s (%.1f%% hit rate)\n",
		stats.CacheStats.Entries, formatBytes(stats.CacheStats.Size),
		cacheHitRate(stats.CacheStats))

	var totalKeys uint64
	var totalSize int64
	for _, level := range stats.Levels {
		if level.NumTables > 0 {
			fmt.Printf("L%d: %d tables, %d keys, %s\n",
				level.Level, level.NumTables, level.NumKeys, formatBytes(level.Size))
			totalKeys += level.NumKeys
			totalSize += level.Size
		}
	}
	fmt.Printf("Total: %d keys, %s\n", totalKeys, formatBytes(totalSize))
}

func (s *Shell) explainPrefix(prefixStr string) {
	// Parse prefix (handle hex format)
	var prefix []byte
	if strings.HasPrefix(prefixStr, "0x") || strings.HasPrefix(prefixStr, "0X") {
		// Hex format
		hexStr := prefixStr[2:]
		prefix = make([]byte, len(hexStr)/2)
		for i := 0; i < len(prefix); i++ {
			fmt.Sscanf(hexStr[i*2:i*2+2], "%02x", &prefix[i])
		}
	} else if strings.HasPrefix(prefixStr, "x'") && strings.HasSuffix(prefixStr, "'") {
		// SQL hex format x'...'
		hexStr := prefixStr[2 : len(prefixStr)-1]
		prefix = make([]byte, len(hexStr)/2)
		for i := 0; i < len(prefix); i++ {
			fmt.Sscanf(hexStr[i*2:i*2+2], "%02x", &prefix[i])
		}
	} else {
		prefix = []byte(prefixStr)
	}

	tables := s.store.ExplainPrefix(prefix)

	if len(tables) == 0 {
		fmt.Printf("No tables have prefix %x in their key range\n", prefix)
		return
	}

	// Group by level
	levelTables := make(map[int][]tinykvs.PrefixTableInfo)
	for _, t := range tables {
		levelTables[t.Level] = append(levelTables[t.Level], t)
	}

	fmt.Printf("Tables with prefix %x in range:\n", prefix)
	fmt.Println()

	var totalInRange, totalWithMatch int
	for level := 0; level < 7; level++ {
		lt := levelTables[level]
		if len(lt) == 0 {
			continue
		}

		matchCount := 0
		for _, t := range lt {
			if t.HasMatch {
				matchCount++
			}
		}
		totalInRange += len(lt)
		totalWithMatch += matchCount

		fmt.Printf("L%d: %d tables in range, %d with matching keys\n", level, len(lt), matchCount)

		// Show details for tables with matches (limit to 10)
		shown := 0
		for _, t := range lt {
			if t.HasMatch && shown < 10 {
				fmt.Printf("  [%d] minKey=%x maxKey=%x firstMatch=%x (%d keys)\n",
					t.TableID, t.MinKey, t.MaxKey, t.FirstMatch, t.NumKeys)
				shown++
			}
		}
		if matchCount > 10 {
			fmt.Printf("  ... and %d more tables with matches\n", matchCount-10)
		}
	}

	fmt.Println()
	fmt.Printf("Summary: %d tables in range, %d with actual matches\n", totalInRange, totalWithMatch)
}

func cacheHitRate(cs tinykvs.CacheStats) float64 {
	total := cs.Hits + cs.Misses
	if total == 0 {
		return 0
	}
	return float64(cs.Hits) / float64(total) * 100
}

// formatIntCommas formats an integer with comma separators for readability.
func formatIntCommas(n int64) string {
	if n < 0 {
		return "-" + formatIntCommas(-n)
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	// Insert commas from right to left
	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// formatDuration formats a duration in human-readable form (e.g., "3m 7s", "2.3s").
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	if minutes < 60 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := minutes / 60
	minutes = minutes % 60
	return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
}

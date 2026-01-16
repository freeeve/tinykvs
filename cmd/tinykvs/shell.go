package main

import (
	"bufio"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/freeeve/tinykvs"
	"github.com/peterh/liner"
	"github.com/vmihailenco/msgpack/v5"
)

// Shell provides an interactive SQL-like query interface.
type Shell struct {
	store       *tinykvs.Store
	prompt      string
	historyFile string
	line        *liner.State
}

// aggType represents an aggregation function type.
type aggType int

const (
	aggCount aggType = iota
	aggSum
	aggAvg
	aggMin
	aggMax
)

// aggregator holds state for a streaming aggregation.
type aggregator struct {
	typ   aggType
	field string // field path for sum/avg/min/max (empty for count)
	alias string // display name

	// streaming state
	count    int64
	sum      float64
	min      float64
	max      float64
	hasValue bool
}

func (a *aggregator) update(val tinykvs.Value) {
	a.count++

	if a.field == "" {
		return // count() doesn't need field extraction
	}

	// Extract numeric value from field
	num, ok := a.extractNumeric(val)
	if !ok {
		return
	}

	a.sum += num
	if !a.hasValue {
		a.min = num
		a.max = num
		a.hasValue = true
	} else {
		if num < a.min {
			a.min = num
		}
		if num > a.max {
			a.max = num
		}
	}
}

func (a *aggregator) extractNumeric(val tinykvs.Value) (float64, bool) {
	// For non-record types, use the value directly
	if a.field == "" {
		switch val.Type {
		case tinykvs.ValueTypeInt64:
			return float64(val.Int64), true
		case tinykvs.ValueTypeFloat64:
			return val.Float64, true
		default:
			return 0, false
		}
	}

	// For record/msgpack types, extract the field
	var record map[string]any
	switch val.Type {
	case tinykvs.ValueTypeRecord:
		record = val.Record
	case tinykvs.ValueTypeMsgpack:
		if err := msgpack.Unmarshal(val.Bytes, &record); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if record == nil {
		return 0, false
	}

	fieldVal, ok := extractNestedField(record, a.field)
	if !ok {
		return 0, false
	}

	switch v := fieldVal.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func (a *aggregator) result() string {
	switch a.typ {
	case aggCount:
		return fmt.Sprintf("%d", a.count)
	case aggSum:
		if !a.hasValue {
			return "NULL"
		}
		return formatNumber(a.sum)
	case aggAvg:
		if a.count == 0 || !a.hasValue {
			return "NULL"
		}
		return formatNumber(a.sum / float64(a.count))
	case aggMin:
		if !a.hasValue {
			return "NULL"
		}
		return formatNumber(a.min)
	case aggMax:
		if !a.hasValue {
			return "NULL"
		}
		return formatNumber(a.max)
	default:
		return "NULL"
	}
}

func formatNumber(f float64) string {
	if f == math.Trunc(f) {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%g", f)
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

	fmt.Println("TinyKVS Shell")
	fmt.Println("Type \\help for help, \\q to quit")
	fmt.Println()

	for {
		input, err := s.line.Prompt(s.prompt)
		if err != nil {
			if err == liner.ErrPromptAborted {
				fmt.Println("^C")
				continue
			}
			break // EOF or other error
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

	// Parse SQL
	stmt, err := sqlparser.Parse(line)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return true
	}

	switch st := stmt.(type) {
	case *sqlparser.Select:
		s.handleSelect(st)
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

func cacheHitRate(cs tinykvs.CacheStats) float64 {
	total := cs.Hits + cs.Misses
	if total == 0 {
		return 0
	}
	return float64(cs.Hits) / float64(total) * 100
}

func (s *Shell) handleSelect(stmt *sqlparser.Select) {
	// Parse WHERE clause
	var keyEquals string
	var keyPrefix string
	var keyStart, keyEnd string
	limit := 100 // default limit

	// Parse SELECT fields and aggregates
	var fields []string
	var aggs []*aggregator
	for _, expr := range stmt.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.AliasedExpr:
			// Check for aggregate functions
			if funcExpr, ok := e.Expr.(*sqlparser.FuncExpr); ok {
				agg := parseAggregateFunc(funcExpr)
				if agg != nil {
					aggs = append(aggs, agg)
					continue
				}
			}

			// Check for field access (v.name, v.address.city)
			if col, ok := e.Expr.(*sqlparser.ColName); ok {
				qualifier := strings.ToLower(col.Qualifier.Name.String())
				fieldName := col.Name.String()

				if qualifier == "v" {
					// Direct v.field or v.`nested.path` syntax
					fields = append(fields, fieldName)
				} else if qualifier != "" && qualifier != "kv" {
					// Parser split v.address.city into qualifier="address", name="city"
					// Reconstruct as dotted path: "address.city"
					fields = append(fields, qualifier+"."+fieldName)
				}
			}
		case *sqlparser.StarExpr:
			// SELECT * - no specific fields
			fields = nil
		}
	}

	// Parse LIMIT
	if stmt.Limit != nil {
		if stmt.Limit.Rowcount != nil {
			if val, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok {
				if n, err := strconv.Atoi(string(val.Val)); err == nil {
					limit = n
				}
			}
		}
	}

	// Parse WHERE
	if stmt.Where != nil {
		s.parseWhere(stmt.Where.Expr, &keyEquals, &keyPrefix, &keyStart, &keyEnd)
	}

	count := 0
	isAggregate := len(aggs) > 0

	// Callback for processing each row
	processRow := func(key []byte, val tinykvs.Value) bool {
		if !isAggregate && count >= limit {
			return false
		}
		if isAggregate {
			for _, agg := range aggs {
				agg.update(val)
			}
		} else {
			printRowFields(key, val, fields)
		}
		count++
		return true
	}

	var err error
	if keyEquals != "" {
		// Point lookup
		val, e := s.store.Get([]byte(keyEquals))
		if e == tinykvs.ErrKeyNotFound {
			if isAggregate {
				printAggregateResults(aggs)
			} else {
				fmt.Println("(0 rows)")
			}
			return
		}
		if e != nil {
			fmt.Printf("Error: %v\n", e)
			return
		}
		processRow([]byte(keyEquals), val)
	} else if keyPrefix != "" {
		err = s.store.ScanPrefix([]byte(keyPrefix), processRow)
	} else if keyStart != "" && keyEnd != "" {
		err = s.store.ScanRange([]byte(keyStart), []byte(keyEnd), processRow)
	} else {
		err = s.store.ScanPrefix(nil, processRow)
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if isAggregate {
		printAggregateResults(aggs)
	} else {
		fmt.Printf("(%d rows)\n", count)
	}
}

func parseAggregateFunc(funcExpr *sqlparser.FuncExpr) *aggregator {
	funcName := strings.ToLower(funcExpr.Name.String())

	var typ aggType
	switch funcName {
	case "count":
		typ = aggCount
	case "sum":
		typ = aggSum
	case "avg":
		typ = aggAvg
	case "min":
		typ = aggMin
	case "max":
		typ = aggMax
	default:
		return nil
	}

	agg := &aggregator{typ: typ}

	// Extract field argument for sum/avg/min/max
	if len(funcExpr.Exprs) > 0 {
		if aliased, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr); ok {
			if col, ok := aliased.Expr.(*sqlparser.ColName); ok {
				qualifier := strings.ToLower(col.Qualifier.Name.String())
				fieldName := col.Name.String()

				if qualifier == "v" {
					agg.field = fieldName
				} else if qualifier != "" && qualifier != "kv" {
					agg.field = qualifier + "." + fieldName
				}
			}
		}
	}

	// Build alias for display
	if agg.field != "" {
		agg.alias = fmt.Sprintf("%s(v.%s)", funcName, agg.field)
	} else {
		agg.alias = fmt.Sprintf("%s()", funcName)
	}

	return agg
}

func printAggregateResults(aggs []*aggregator) {
	// Print header
	headers := make([]string, len(aggs))
	for i, agg := range aggs {
		headers[i] = agg.alias
	}
	fmt.Println(strings.Join(headers, " | "))

	// Print separator
	seps := make([]string, len(aggs))
	for i, agg := range aggs {
		seps[i] = strings.Repeat("-", len(agg.alias))
	}
	fmt.Println(strings.Join(seps, "-+-"))

	// Print values
	values := make([]string, len(aggs))
	for i, agg := range aggs {
		result := agg.result()
		// Pad to match header width
		if len(result) < len(agg.alias) {
			result = strings.Repeat(" ", len(agg.alias)-len(result)) + result
		}
		values[i] = result
	}
	fmt.Println(strings.Join(values, " | "))
}

func (s *Shell) parseWhere(expr sqlparser.Expr, keyEquals, keyPrefix, keyStart, keyEnd *string) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// Check if left side is 'k' column
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			colName := strings.ToLower(col.Name.String())
			if colName == "k" {
				val := extractValue(e.Right)
				switch e.Operator {
				case "=":
					*keyEquals = val
				case "like":
					// Only support prefix matching (trailing %)
					if strings.HasSuffix(val, "%") && !strings.Contains(val[:len(val)-1], "%") {
						*keyPrefix = val[:len(val)-1]
					} else {
						fmt.Println("Warning: LIKE only supports prefix matching (e.g., 'prefix%')")
					}
				case ">=":
					*keyStart = val
				case "<=":
					*keyEnd = val
				}
			}
		}
	case *sqlparser.RangeCond:
		// BETWEEN
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			if strings.ToLower(col.Name.String()) == "k" {
				*keyStart = extractValue(e.From)
				*keyEnd = extractValue(e.To)
			}
		}
	case *sqlparser.AndExpr:
		s.parseWhere(e.Left, keyEquals, keyPrefix, keyStart, keyEnd)
		s.parseWhere(e.Right, keyEquals, keyPrefix, keyStart, keyEnd)
	}
}

func extractValue(expr sqlparser.Expr) string {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return string(v.Val)
		case sqlparser.HexVal:
			// x'deadbeef'
			decoded, _ := hexDecode(string(v.Val))
			return string(decoded)
		case sqlparser.IntVal:
			return string(v.Val)
		}
	}
	return ""
}

func hexDecode(s string) ([]byte, error) {
	// Remove any spaces
	s = strings.ReplaceAll(s, " ", "")
	result := make([]byte, len(s)/2)
	for i := 0; i < len(s)/2; i++ {
		b, err := strconv.ParseUint(s[i*2:i*2+2], 16, 8)
		if err != nil {
			return nil, err
		}
		result[i] = byte(b)
	}
	return result, nil
}

// isMsgpackMap checks if data starts with a msgpack map marker
func isMsgpackMap(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	b := data[0]
	// fixmap (0x80-0x8f), map16 (0xde), map32 (0xdf)
	return (b >= 0x80 && b <= 0x8f) || b == 0xde || b == 0xdf
}

// extractNestedField extracts a nested field value from a record using a dotted path.
// For example, path "address.city" extracts record["address"]["city"].
func extractNestedField(record map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var current any = record
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// extractValueAndType returns the value string and whether it was a hex value
func extractValueAndType(expr sqlparser.Expr) (value string, hexBytes []byte, isHex bool) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return string(v.Val), nil, false
		case sqlparser.HexVal:
			decoded, _ := hexDecode(string(v.Val))
			return string(decoded), decoded, true
		case sqlparser.IntVal:
			return string(v.Val), nil, false
		}
	}
	return "", nil, false
}

func (s *Shell) handleInsert(stmt *sqlparser.Insert) {
	// Extract values from INSERT
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok || len(rows) == 0 {
		fmt.Println("Error: Invalid INSERT syntax")
		return
	}

	inserted := 0
	for _, row := range rows {
		if len(row) < 2 {
			fmt.Println("Error: INSERT requires (key, value)")
			continue
		}

		key := extractValue(row[0])
		value, hexBytes, isHex := extractValueAndType(row[1])

		if key == "" {
			fmt.Println("Error: key cannot be empty")
			continue
		}

		// Try msgpack first if it's hex data that looks like a msgpack map
		if isHex && isMsgpackMap(hexBytes) {
			if record, err := tinykvs.DecodeMsgpack(hexBytes); err == nil {
				if err := s.store.PutMap([]byte(key), record); err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}
				inserted++
				continue
			}
			// Fall through to store as bytes if msgpack decode fails
		}

		// Detect JSON and store as record
		if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
			var record map[string]any
			if err := json.Unmarshal([]byte(value), &record); err == nil {
				if err := s.store.PutMap([]byte(key), record); err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}
				inserted++
				continue
			}
		}

		// Store hex data as bytes, strings as strings
		if isHex {
			if err := s.store.PutBytes([]byte(key), hexBytes); err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
		} else {
			if err := s.store.PutString([]byte(key), value); err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
		}
		inserted++
	}

	fmt.Printf("INSERT %d\n", inserted)
}

func (s *Shell) handleUpdate(stmt *sqlparser.Update) {
	// Extract key from WHERE
	var keyEquals string
	if stmt.Where != nil {
		var dummy1, dummy2, dummy3 string
		s.parseWhere(stmt.Where.Expr, &keyEquals, &dummy1, &dummy2, &dummy3)
	}

	if keyEquals == "" {
		fmt.Println("Error: UPDATE requires WHERE k = 'value'")
		return
	}

	// Extract new value from SET
	var newValue string
	for _, expr := range stmt.Exprs {
		if strings.ToLower(expr.Name.Name.String()) == "v" {
			newValue = extractValue(expr.Expr)
			break
		}
	}

	if err := s.store.PutString([]byte(keyEquals), newValue); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("UPDATE 1")
}

func (s *Shell) handleDelete(stmt *sqlparser.Delete) {
	var keyEquals string
	var keyPrefix string
	var keyStart, keyEnd string

	if stmt.Where != nil {
		s.parseWhere(stmt.Where.Expr, &keyEquals, &keyPrefix, &keyStart, &keyEnd)
	}

	if keyEquals != "" {
		// Delete single key
		if err := s.store.Delete([]byte(keyEquals)); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("DELETE 1")
	} else if keyPrefix != "" {
		// Delete by prefix
		deleted, err := s.store.DeletePrefix([]byte(keyPrefix))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("DELETE %d\n", deleted)
	} else if keyStart != "" && keyEnd != "" {
		// Delete range
		deleted, err := s.store.DeleteRange([]byte(keyStart), []byte(keyEnd))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("DELETE %d\n", deleted)
	} else {
		fmt.Println("Error: DELETE requires WHERE clause")
	}
}

func printRowFields(key []byte, val tinykvs.Value, fields []string) {
	keyStr := formatKey(key)
	switch val.Type {
	case tinykvs.ValueTypeInt64:
		fmt.Printf("%s | %d\n", keyStr, val.Int64)
	case tinykvs.ValueTypeFloat64:
		fmt.Printf("%s | %f\n", keyStr, val.Float64)
	case tinykvs.ValueTypeBool:
		fmt.Printf("%s | %t\n", keyStr, val.Bool)
	case tinykvs.ValueTypeString, tinykvs.ValueTypeBytes:
		if len(val.Bytes) > 100 {
			fmt.Printf("%s | %s... (%d bytes)\n", keyStr, string(val.Bytes[:100]), len(val.Bytes))
		} else {
			fmt.Printf("%s | %s\n", keyStr, string(val.Bytes))
		}
	case tinykvs.ValueTypeRecord, tinykvs.ValueTypeMsgpack:
		var record map[string]any
		if val.Type == tinykvs.ValueTypeRecord {
			record = val.Record
		} else {
			// Decode msgpack to map for display
			if err := msgpack.Unmarshal(val.Bytes, &record); err != nil {
				fmt.Printf("%s | (msgpack decode error)\n", keyStr)
				return
			}
		}
		if record == nil {
			fmt.Printf("%s | {}\n", keyStr)
			return
		}
		if len(fields) > 0 {
			// Print only selected fields (supports nested paths like "address.city")
			values := make([]string, len(fields))
			for i, field := range fields {
				if v, ok := extractNestedField(record, field); ok {
					values[i] = fmt.Sprintf("%v", v)
				} else {
					values[i] = "NULL"
				}
			}
			fmt.Printf("%s | %s\n", keyStr, strings.Join(values, " | "))
		} else {
			// Print as JSON
			jsonBytes, _ := json.Marshal(record)
			fmt.Printf("%s | %s\n", keyStr, string(jsonBytes))
		}
	default:
		fmt.Printf("%s | (unknown type)\n", keyStr)
	}
}

func (s *Shell) exportCSV(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header - simple format: key, value
	writer.Write([]string{"key", "value"})

	var count int64
	err = s.store.ScanPrefix(nil, func(key []byte, val tinykvs.Value) bool {
		var valueStr string
		switch val.Type {
		case tinykvs.ValueTypeInt64:
			valueStr = strconv.FormatInt(val.Int64, 10)
		case tinykvs.ValueTypeFloat64:
			valueStr = strconv.FormatFloat(val.Float64, 'g', -1, 64)
		case tinykvs.ValueTypeBool:
			valueStr = strconv.FormatBool(val.Bool)
		case tinykvs.ValueTypeString:
			valueStr = string(val.Bytes)
		case tinykvs.ValueTypeBytes:
			// Bytes exported as hex with prefix
			valueStr = "0x" + hex.EncodeToString(val.Bytes)
		case tinykvs.ValueTypeRecord:
			jsonBytes, _ := json.Marshal(val.Record)
			valueStr = string(jsonBytes)
		case tinykvs.ValueTypeMsgpack:
			var record map[string]any
			if err := msgpack.Unmarshal(val.Bytes, &record); err != nil {
				return true // skip on decode error
			}
			jsonBytes, _ := json.Marshal(record)
			valueStr = string(jsonBytes)
		default:
			return true // skip unknown types
		}

		writer.Write([]string{string(key), valueStr})

		count++
		if count%10000 == 0 {
			fmt.Printf("\rExported %d keys...", count)
		}
		return true
	})

	if err != nil {
		fmt.Printf("\nError scanning: %v\n", err)
		return
	}

	fmt.Printf("\rExported %d keys to %s\n", count, filename)
}

func (s *Shell) importCSV(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	// Read header to detect format
	header, err := reader.Read()
	if err != nil {
		fmt.Printf("Error reading header: %v\n", err)
		return
	}

	var count int64
	var errors int64

	// Detect format based on header
	// Old format: "key,type,value" (hex-encoded)
	// Simple format: "key,value" (key is string, value is string/JSON)
	// Record format: "key,field1,field2,..." (first col is key, rest are fields)
	//   - supports type hints: "field:type" where type is string/int/float/bool/json
	isOldFormat := len(header) == 3 && header[0] == "key" && header[1] == "type" && header[2] == "value"
	isSimpleFormat := len(header) == 2

	// Parse field names and type hints for record format
	type fieldSpec struct {
		name     string
		typeHint string // "", "string", "int", "float", "bool", "json"
	}
	var fieldSpecs []fieldSpec
	if !isOldFormat && !isSimpleFormat {
		for i := 1; i < len(header); i++ {
			spec := fieldSpec{}
			if idx := strings.LastIndex(header[i], ":"); idx != -1 {
				spec.name = header[i][:idx]
				spec.typeHint = strings.ToLower(header[i][idx+1:])
			} else {
				spec.name = header[i]
				spec.typeHint = "" // auto-detect
			}
			fieldSpecs = append(fieldSpecs, spec)
		}
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

		var key []byte
		var val tinykvs.Value

		if isOldFormat {
			// Old format: hex key, type, hex/json value
			if len(record) != 3 {
				errors++
				continue
			}
			key, err = hex.DecodeString(record[0])
			if err != nil {
				errors++
				continue
			}
			val, err = parseTypedValue(record[1], record[2])
			if err != nil {
				errors++
				continue
			}
		} else if isSimpleFormat {
			// Simple format: string key, string/JSON value
			if len(record) != 2 {
				errors++
				continue
			}
			key = []byte(record[0])
			val = parseAutoValue(record[1])
		} else {
			// Record format: first column is key, rest are fields with optional type hints
			if len(record) != len(header) {
				errors++
				continue
			}
			key = []byte(record[0])
			fields := make(map[string]any)
			for i, spec := range fieldSpecs {
				fields[spec.name] = parseFieldValueWithHint(record[i+1], spec.typeHint)
			}
			val = tinykvs.RecordValue(fields)
		}

		if err := s.store.Put(key, val); err != nil {
			errors++
			continue
		}
		count++

		if count%10000 == 0 {
			fmt.Printf("\rImported %d keys...", count)
		}
	}

	fmt.Printf("\rImported %d keys (%d errors)\n", count, errors)
}

// parseTypedValue parses a value in the old export format (type + hex/json encoded value)
func parseTypedValue(typeName, valueStr string) (tinykvs.Value, error) {
	switch typeName {
	case "int64":
		v, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return tinykvs.Value{}, err
		}
		return tinykvs.Int64Value(v), nil
	case "float64":
		v, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return tinykvs.Value{}, err
		}
		return tinykvs.Float64Value(v), nil
	case "bool":
		v, err := strconv.ParseBool(valueStr)
		if err != nil {
			return tinykvs.Value{}, err
		}
		return tinykvs.BoolValue(v), nil
	case "string":
		bytes, err := hex.DecodeString(valueStr)
		if err != nil {
			return tinykvs.Value{}, err
		}
		return tinykvs.StringValue(string(bytes)), nil
	case "bytes":
		bytes, err := hex.DecodeString(valueStr)
		if err != nil {
			return tinykvs.Value{}, err
		}
		return tinykvs.BytesValue(bytes), nil
	case "record":
		var record map[string]any
		if err := json.Unmarshal([]byte(valueStr), &record); err != nil {
			return tinykvs.Value{}, err
		}
		return tinykvs.RecordValue(record), nil
	default:
		return tinykvs.Value{}, fmt.Errorf("unknown type: %s", typeName)
	}
}

// parseAutoValue tries to parse a value, auto-detecting JSON and hex bytes
func parseAutoValue(s string) tinykvs.Value {
	// Try hex bytes (0x prefix)
	if strings.HasPrefix(s, "0x") {
		if bytes, err := hex.DecodeString(s[2:]); err == nil {
			return tinykvs.BytesValue(bytes)
		}
	}
	// Try JSON object
	if strings.HasPrefix(s, "{") {
		var record map[string]any
		if err := json.Unmarshal([]byte(s), &record); err == nil {
			return tinykvs.RecordValue(record)
		}
	}
	// Try int
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return tinykvs.Int64Value(i)
	}
	// Try float (only if contains decimal point to avoid int ambiguity)
	if strings.Contains(s, ".") {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return tinykvs.Float64Value(f)
		}
	}
	// Try bool
	if s == "true" {
		return tinykvs.BoolValue(true)
	}
	if s == "false" {
		return tinykvs.BoolValue(false)
	}
	// Default to string
	return tinykvs.StringValue(s)
}

// parseFieldValue tries to parse a field value, auto-detecting type
func parseFieldValue(s string) any {
	// Try int
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	// Try bool
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	// Try JSON
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		var v any
		if err := json.Unmarshal([]byte(s), &v); err == nil {
			return v
		}
	}
	// Default to string
	return s
}

// parseFieldValueWithHint parses a field value using an optional type hint
// Type hints: "string", "int", "float", "bool", "json", or "" for auto-detect
func parseFieldValueWithHint(s string, typeHint string) any {
	switch typeHint {
	case "string", "str", "s":
		return s
	case "int", "integer", "i":
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
		return s // fallback to string on parse error
	case "float", "f", "double", "number":
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		return s
	case "bool", "boolean", "b":
		if b, err := strconv.ParseBool(s); err == nil {
			return b
		}
		return s
	case "json", "j", "object":
		var v any
		if err := json.Unmarshal([]byte(s), &v); err == nil {
			return v
		}
		return s
	default:
		// Auto-detect
		return parseFieldValue(s)
	}
}

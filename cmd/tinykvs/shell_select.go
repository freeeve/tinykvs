package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/freeeve/tinykvs"
	"github.com/vmihailenco/msgpack/v5"
)

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

// valueFilter represents a filter condition on a value field
type valueFilter struct {
	field    string // e.g., "ttl", "name.first"
	operator string // "=", "like", ">", "<", ">=", "<="
	value    string // the comparison value
}

func (vf *valueFilter) matches(val tinykvs.Value) bool {
	// Extract the record
	var record map[string]any
	switch val.Type {
	case tinykvs.ValueTypeRecord:
		record = val.Record
	case tinykvs.ValueTypeMsgpack:
		if err := msgpack.Unmarshal(val.Bytes, &record); err != nil {
			return false
		}
	default:
		return false
	}

	// Get field value
	fieldVal, ok := extractNestedField(record, vf.field)
	if !ok {
		return false
	}

	fieldStr := fmt.Sprintf("%v", fieldVal)

	switch vf.operator {
	case "=":
		return fieldStr == vf.value
	case "!=", "<>":
		return fieldStr != vf.value
	case "like":
		// Only prefix matching supported
		if strings.HasSuffix(vf.value, "%") {
			prefix := vf.value[:len(vf.value)-1]
			return strings.HasPrefix(fieldStr, prefix)
		}
		return fieldStr == vf.value
	case ">":
		return fieldStr > vf.value
	case "<":
		return fieldStr < vf.value
	case ">=":
		return fieldStr >= vf.value
	case "<=":
		return fieldStr <= vf.value
	default:
		return true
	}
}

func (s *Shell) handleSelect(stmt *sqlparser.Select, orderBy []SortOrder) {
	// Parse WHERE clause
	var keyEquals string
	var keyPrefix string
	var keyStart, keyEnd string
	var valueFilters []*valueFilter
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
		s.parseWhere(stmt.Where.Expr, &keyEquals, &keyPrefix, &keyStart, &keyEnd, &valueFilters)
	}

	isAggregate := len(aggs) > 0
	hasOrderBy := len(orderBy) > 0

	// Set up headers based on fields
	var headers []string
	if len(fields) > 0 {
		headers = append([]string{"k"}, fields...)
	} else {
		headers = []string{"k", "v"}
	}

	// Track progress for slow queries
	var scanned int64
	var scanStats tinykvs.ScanStats
	var matchCount int
	var lastProgress time.Time
	hasValueFilters := len(valueFilters) > 0
	startTime := time.Now()

	// Buffer rows for table output
	var bufferedRows [][]string

	// Set up Ctrl+C handler to cancel scan
	var interrupted int32
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)
	go func() {
		<-sigChan
		atomic.StoreInt32(&interrupted, 1)
	}()
	defer signal.Stop(sigChan)

	// Progress callback for scan stats
	progressCallback := func(stats tinykvs.ScanStats) bool {
		if atomic.LoadInt32(&interrupted) != 0 {
			return false
		}
		scanStats = stats
		if time.Since(lastProgress) > time.Second {
			elapsed := time.Since(startTime)
			rate := int64(float64(stats.KeysExamined) / elapsed.Seconds())
			fmt.Fprintf(os.Stderr, "\rScanned %s keys (%s blocks) in %s (%s keys/sec)...    ",
				formatIntCommas(stats.KeysExamined), formatIntCommas(stats.BlocksLoaded),
				formatDuration(elapsed), formatIntCommas(rate))
			lastProgress = time.Now()
		}
		return true
	}

	// Callback for processing each row
	processRow := func(key []byte, val tinykvs.Value) bool {
		if atomic.LoadInt32(&interrupted) != 0 {
			return false
		}
		scanned++

		// Show progress every second for queries with value filters
		if hasValueFilters && time.Since(lastProgress) > time.Second {
			elapsed := time.Since(startTime)
			rate := int64(float64(scanned) / elapsed.Seconds())
			fmt.Fprintf(os.Stderr, "\rScanned %s keys (%s blocks) in %s (%s keys/sec), found %d matches...    ",
				formatIntCommas(scanned), formatIntCommas(scanStats.BlocksLoaded),
				formatDuration(elapsed), formatIntCommas(rate), matchCount)
			lastProgress = time.Now()
		}

		// For ORDER BY, we need all matching rows before applying limit
		if !isAggregate && !hasOrderBy && matchCount >= limit {
			return false
		}

		// Apply value filters
		for _, vf := range valueFilters {
			if !vf.matches(val) {
				return true // skip this row, continue scanning
			}
		}

		if isAggregate {
			for _, agg := range aggs {
				agg.update(val)
			}
		} else {
			// Buffer rows for table output
			row := extractRowFields(key, val, fields)
			bufferedRows = append(bufferedRows, row)
			matchCount++
		}
		return true
	}

	var err error
	var scanErr error

	// Wrap processRow to catch any panics
	safeProcessRow := func(key []byte, val tinykvs.Value) bool {
		defer func() {
			if r := recover(); r != nil {
				scanErr = fmt.Errorf("panic processing key %x: %v", key[:min(8, len(key))], r)
			}
		}()
		return processRow(key, val)
	}

	if keyEquals != "" {
		// Point lookup
		val, e := s.store.Get([]byte(keyEquals))
		if e == tinykvs.ErrKeyNotFound {
			if isAggregate {
				printAggregateResults(aggs)
			} else {
				printTable(headers, nil)
				fmt.Printf("(0 rows)\n")
			}
			return
		}
		if e != nil {
			fmt.Printf("Error: %v\n", e)
			return
		}
		safeProcessRow([]byte(keyEquals), val)
	} else if keyPrefix != "" {
		scanStats, err = s.store.ScanPrefixWithStats([]byte(keyPrefix), safeProcessRow, progressCallback)
	} else if keyStart != "" && keyEnd != "" {
		err = s.store.ScanRange([]byte(keyStart), []byte(keyEnd), safeProcessRow)
	} else {
		scanStats, err = s.store.ScanPrefixWithStats(nil, safeProcessRow, progressCallback)
	}

	// Clear progress line
	if (hasValueFilters || scanned > 10000) && scanned > 0 {
		fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
	}

	wasInterrupted := atomic.LoadInt32(&interrupted) != 0
	if wasInterrupted {
		fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
		fmt.Println("^C")
	}

	if scanErr != nil {
		fmt.Printf("Scan error: %v\n", scanErr)
		return
	}
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	elapsed := time.Since(startTime)

	if isAggregate {
		printAggregateResults(aggs)
	} else {
		// Sort if ORDER BY was specified
		if hasOrderBy {
			SortRows(headers, bufferedRows, orderBy)
		}

		// Apply limit after sorting (for ORDER BY) or just cap results
		if len(bufferedRows) > limit {
			bufferedRows = bufferedRows[:limit]
		}
		matchCount = len(bufferedRows)

		// Print table with box formatting
		printTable(headers, bufferedRows)
	}

	// Show stats
	if scanned > 0 || scanStats.BlocksLoaded > 0 {
		rate := float64(scanned) / elapsed.Seconds()
		if wasInterrupted {
			fmt.Printf("(%d rows) - interrupted, scanned %s keys (%s blocks) in %s (%s keys/sec)\n",
				matchCount, formatIntCommas(scanned),
				formatIntCommas(scanStats.BlocksLoaded), formatDuration(elapsed), formatIntCommas(int64(rate)))
		} else {
			// Show detailed stats: tables checked/added, blocks (cache/disk)
			blockDetails := fmt.Sprintf("%d blocks", scanStats.BlocksLoaded)
			if scanStats.BlocksCacheHit > 0 || scanStats.BlocksDiskRead > 0 {
				blockDetails = fmt.Sprintf("%d blocks (%d cache, %d disk)",
					scanStats.BlocksLoaded, scanStats.BlocksCacheHit, scanStats.BlocksDiskRead)
			}
			tableDetails := ""
			if scanStats.TablesChecked > 0 {
				tableDetails = fmt.Sprintf(", %d/%d tables",
					scanStats.TablesAdded, scanStats.TablesChecked)
			}
			fmt.Printf("(%d rows) scanned %s keys, %s%s, %s\n",
				matchCount, formatIntCommas(scanned),
				blockDetails, tableDetails, formatDuration(elapsed))
		}
	} else {
		fmt.Printf("(%d rows)\n", matchCount)
	}
}

// printStreamingHeader prints column headers for streaming output
func printStreamingHeader(headers []string) {
	for i, h := range headers {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(h)
	}
	fmt.Println()
	// Print separator
	for i, h := range headers {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(strings.Repeat("-", len(h)))
	}
	fmt.Println()
}

// printStreamingRow prints a single row for streaming output
func printStreamingRow(row []string) {
	for i, cell := range row {
		if i > 0 {
			fmt.Print("\t")
		}
		// Truncate long values
		if len(cell) > 60 {
			fmt.Print(cell[:57] + "...")
		} else {
			fmt.Print(cell)
		}
	}
	fmt.Println()
}

// printTable prints rows in DuckDB-style box format
func printTable(headers []string, rows [][]string) {
	if len(headers) == 0 {
		return
	}

	// Calculate column widths
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Cap column widths at 50 chars for readability
	for i := range widths {
		if widths[i] > 50 {
			widths[i] = 50
		}
	}

	// Print top border
	printBoxLine(widths, "┌", "┬", "┐")

	// Print header row
	fmt.Print("│")
	for i, h := range headers {
		fmt.Printf(" %-*s │", widths[i], truncate(h, widths[i]))
	}
	fmt.Println()

	// Print header separator
	printBoxLine(widths, "├", "┼", "┤")

	// Print data rows
	for _, row := range rows {
		fmt.Print("│")
		for i := 0; i < len(headers); i++ {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			fmt.Printf(" %-*s │", widths[i], truncate(cell, widths[i]))
		}
		fmt.Println()
	}

	// Print bottom border
	printBoxLine(widths, "└", "┴", "┘")
}

func printBoxLine(widths []int, left, mid, right string) {
	fmt.Print(left)
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print(mid)
		}
	}
	fmt.Println(right)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
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

// extractRowFields extracts field values as strings for tabular display
func extractRowFields(key []byte, val tinykvs.Value, fields []string) []string {
	keyStr := formatKey(key)

	if len(fields) == 0 {
		// SELECT * - return key and full value
		return []string{keyStr, formatValue(val)}
	}

	// Extract specific fields
	row := []string{keyStr}

	var record map[string]any
	switch val.Type {
	case tinykvs.ValueTypeRecord:
		record = val.Record
	case tinykvs.ValueTypeMsgpack:
		if err := msgpack.Unmarshal(val.Bytes, &record); err != nil {
			// Can't decode, return NULLs for all fields
			for range fields {
				row = append(row, "NULL")
			}
			return row
		}
	default:
		// Non-record type, return NULLs for all fields
		for range fields {
			row = append(row, "NULL")
		}
		return row
	}

	for _, field := range fields {
		if v, ok := extractNestedField(record, field); ok {
			row = append(row, fmt.Sprintf("%v", v))
		} else {
			row = append(row, "NULL")
		}
	}
	return row
}

func formatValue(val tinykvs.Value) string {
	switch val.Type {
	case tinykvs.ValueTypeInt64:
		return fmt.Sprintf("%d", val.Int64)
	case tinykvs.ValueTypeFloat64:
		return fmt.Sprintf("%f", val.Float64)
	case tinykvs.ValueTypeBool:
		return fmt.Sprintf("%t", val.Bool)
	case tinykvs.ValueTypeString, tinykvs.ValueTypeBytes:
		if len(val.Bytes) > 100 {
			return string(val.Bytes[:100]) + "..."
		}
		return string(val.Bytes)
	case tinykvs.ValueTypeRecord:
		if val.Record == nil {
			return "{}"
		}
		jsonBytes, _ := json.Marshal(val.Record)
		return string(jsonBytes)
	case tinykvs.ValueTypeMsgpack:
		var record map[string]any
		if err := msgpack.Unmarshal(val.Bytes, &record); err != nil {
			return "(msgpack)"
		}
		jsonBytes, _ := json.Marshal(record)
		return string(jsonBytes)
	default:
		return "(unknown)"
	}
}

func (s *Shell) parseWhere(expr sqlparser.Expr, keyEquals, keyPrefix, keyStart, keyEnd *string, valueFilters *[]*valueFilter) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			qualifier := strings.ToLower(col.Qualifier.Name.String())
			colName := strings.ToLower(col.Name.String())

			// Check if this is a key filter (k = ..., k LIKE ..., etc.)
			isKeyFilter := (colName == "k" && qualifier == "") ||
				(colName == "k" && qualifier == "kv")

			if isKeyFilter {
				// Key filter
				val, isHexPrefix := extractValueForLike(e.Right, e.Operator)
				switch e.Operator {
				case "=":
					*keyEquals = val
				case "like":
					if isHexPrefix {
						*keyPrefix = val
					} else if strings.HasSuffix(val, "%") && !strings.Contains(val[:len(val)-1], "%") {
						*keyPrefix = val[:len(val)-1]
					} else {
						fmt.Println("Warning: LIKE only supports prefix matching (e.g., 'prefix%' or x'14%')")
					}
				case ">=":
					*keyStart = val
				case "<=":
					*keyEnd = val
				}
			} else {
				// Value field filter: v.field, v.a.b, or just field (assume v.)
				var fieldName string
				if qualifier == "v" {
					// v.ttl → field is "ttl"
					fieldName = colName
				} else if qualifier != "" && qualifier != "kv" {
					// Nested: parsed as ttl.sub → field is "ttl.sub"
					// Or v.address.city parsed as address.city
					fieldName = qualifier + "." + colName
				} else {
					// No qualifier, assume it's a value field
					// e.g., just "ttl" means v.ttl
					fieldName = colName
				}

				val := extractValue(e.Right)
				*valueFilters = append(*valueFilters, &valueFilter{
					field:    fieldName,
					operator: e.Operator,
					value:    val,
				})
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
		s.parseWhere(e.Left, keyEquals, keyPrefix, keyStart, keyEnd, valueFilters)
		s.parseWhere(e.Right, keyEquals, keyPrefix, keyStart, keyEnd, valueFilters)
	}
}

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
	"github.com/freeeve/msgpck"
	"github.com/freeeve/tinykvs"
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
	var err error
	switch val.Type {
	case tinykvs.ValueTypeRecord:
		record = val.Record
	case tinykvs.ValueTypeMsgpack:
		record, err = msgpck.UnmarshalMapStringAny(val.Bytes, false)
		if err != nil {
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
	var err error
	switch val.Type {
	case tinykvs.ValueTypeRecord:
		record = val.Record
	case tinykvs.ValueTypeMsgpack:
		record, err = msgpck.UnmarshalMapStringAny(val.Bytes, false)
		if err != nil {
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

// selectContext holds state for a SELECT query execution.
type selectContext struct {
	keyEquals    string
	keyPrefix    string
	keyStart     string
	keyEnd       string
	valueFilters []*valueFilter
	limit        int
	fields       []string
	aggs         []*aggregator
	headers      []string
	orderBy      []SortOrder
	scanned      int64
	scanStats    tinykvs.ScanStats
	matchCount   int
	bufferedRows [][]string
	lastProgress time.Time
	startTime    time.Time
	interrupted  int32
	scanErr      error
}

func (s *Shell) handleSelect(stmt *sqlparser.Select, orderBy []SortOrder) {
	ctx := s.parseSelectStatement(stmt, orderBy)

	sigChan := setupInterruptHandler(&ctx.interrupted)
	defer signal.Stop(sigChan)

	progressCallback := ctx.createProgressCallback()
	processRow := ctx.createRowProcessor()
	safeProcessRow := ctx.wrapRowProcessor(processRow)

	err := s.executeScan(ctx, safeProcessRow, progressCallback)

	ctx.clearProgressLine()

	wasInterrupted := atomic.LoadInt32(&ctx.interrupted) != 0
	if wasInterrupted {
		fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
		fmt.Println("^C")
	}

	if ctx.scanErr != nil {
		fmt.Printf("Scan error: %v\n", ctx.scanErr)
		return
	}
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	ctx.renderResults()
	ctx.reportStatistics(wasInterrupted)
}

func (s *Shell) parseSelectStatement(stmt *sqlparser.Select, orderBy []SortOrder) *selectContext {
	ctx := &selectContext{
		limit:     100,
		orderBy:   orderBy,
		startTime: time.Now(),
	}

	ctx.fields, ctx.aggs = parseSelectExpressions(stmt.SelectExprs)

	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		if val, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok {
			if n, err := strconv.Atoi(string(val.Val)); err == nil {
				ctx.limit = n
			}
		}
	}

	if stmt.Where != nil {
		s.parseWhere(stmt.Where.Expr, &ctx.keyEquals, &ctx.keyPrefix, &ctx.keyStart, &ctx.keyEnd, &ctx.valueFilters)
	}

	if len(ctx.fields) > 0 {
		ctx.headers = append([]string{"k"}, ctx.fields...)
	} else {
		ctx.headers = []string{"k", "v"}
	}

	return ctx
}

func parseSelectExpressions(exprs sqlparser.SelectExprs) ([]string, []*aggregator) {
	var fields []string
	var aggs []*aggregator

	for _, expr := range exprs {
		switch e := expr.(type) {
		case *sqlparser.AliasedExpr:
			if funcExpr, ok := e.Expr.(*sqlparser.FuncExpr); ok {
				if agg := parseAggregateFunc(funcExpr); agg != nil {
					aggs = append(aggs, agg)
					continue
				}
			}
			if col, ok := e.Expr.(*sqlparser.ColName); ok {
				qualifier := strings.ToLower(col.Qualifier.Name.String())
				fieldName := col.Name.String()
				if qualifier == "v" {
					fields = append(fields, fieldName)
				} else if qualifier != "" && qualifier != "kv" {
					fields = append(fields, qualifier+"."+fieldName)
				}
			}
		case *sqlparser.StarExpr:
			fields = nil
		}
	}
	return fields, aggs
}

func setupInterruptHandler(interrupted *int32) chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)
	go func() {
		<-sigChan
		atomic.StoreInt32(interrupted, 1)
	}()
	return sigChan
}

func (ctx *selectContext) createProgressCallback() tinykvs.ScanProgress {
	return func(stats tinykvs.ScanStats) bool {
		if atomic.LoadInt32(&ctx.interrupted) != 0 {
			return false
		}
		ctx.scanStats = stats
		if time.Since(ctx.lastProgress) > time.Second {
			elapsed := time.Since(ctx.startTime)
			rate := int64(float64(stats.KeysExamined) / elapsed.Seconds())
			fmt.Fprintf(os.Stderr, "\rScanned %s keys (%s blocks) in %s (%s keys/sec)...    ",
				formatIntCommas(stats.KeysExamined), formatIntCommas(stats.BlocksLoaded),
				formatDuration(elapsed), formatIntCommas(rate))
			ctx.lastProgress = time.Now()
		}
		return true
	}
}

func (ctx *selectContext) createRowProcessor() func([]byte, tinykvs.Value) bool {
	isAggregate := len(ctx.aggs) > 0
	hasOrderBy := len(ctx.orderBy) > 0
	hasValueFilters := len(ctx.valueFilters) > 0

	return func(key []byte, val tinykvs.Value) bool {
		if atomic.LoadInt32(&ctx.interrupted) != 0 {
			return false
		}
		ctx.scanned++

		if hasValueFilters && time.Since(ctx.lastProgress) > time.Second {
			ctx.printFilterProgress()
		}

		if !isAggregate && !hasOrderBy && ctx.matchCount >= ctx.limit {
			return false
		}

		for _, vf := range ctx.valueFilters {
			if !vf.matches(val) {
				return true
			}
		}

		if isAggregate {
			for _, agg := range ctx.aggs {
				agg.update(val)
			}
		} else {
			row := extractRowFields(key, val, ctx.fields)
			ctx.bufferedRows = append(ctx.bufferedRows, row)
			ctx.matchCount++
		}
		return true
	}
}

func (ctx *selectContext) printFilterProgress() {
	elapsed := time.Since(ctx.startTime)
	rate := int64(float64(ctx.scanned) / elapsed.Seconds())
	fmt.Fprintf(os.Stderr, "\rScanned %s keys (%s blocks) in %s (%s keys/sec), found %d matches...    ",
		formatIntCommas(ctx.scanned), formatIntCommas(ctx.scanStats.BlocksLoaded),
		formatDuration(elapsed), formatIntCommas(rate), ctx.matchCount)
	ctx.lastProgress = time.Now()
}

func (ctx *selectContext) wrapRowProcessor(processRow func([]byte, tinykvs.Value) bool) func([]byte, tinykvs.Value) bool {
	return func(key []byte, val tinykvs.Value) bool {
		defer func() {
			if r := recover(); r != nil {
				ctx.scanErr = fmt.Errorf("panic processing key %x: %v", key[:min(8, len(key))], r)
			}
		}()
		return processRow(key, val)
	}
}

func (s *Shell) executeScan(ctx *selectContext, processRow func([]byte, tinykvs.Value) bool, progress tinykvs.ScanProgress) error {
	if ctx.keyEquals != "" {
		return s.executePointLookup(ctx, processRow)
	}
	if ctx.keyPrefix != "" {
		var err error
		ctx.scanStats, err = s.store.ScanPrefixWithStats([]byte(ctx.keyPrefix), processRow, progress)
		return err
	}
	if ctx.keyStart != "" && ctx.keyEnd != "" {
		return s.store.ScanRange([]byte(ctx.keyStart), []byte(ctx.keyEnd), processRow)
	}
	var err error
	ctx.scanStats, err = s.store.ScanPrefixWithStats(nil, processRow, progress)
	return err
}

func (s *Shell) executePointLookup(ctx *selectContext, processRow func([]byte, tinykvs.Value) bool) error {
	val, err := s.store.Get([]byte(ctx.keyEquals))
	if err == tinykvs.ErrKeyNotFound {
		if len(ctx.aggs) > 0 {
			printAggregateResults(ctx.aggs)
		} else {
			printTable(ctx.headers, nil)
			fmt.Printf("(0 rows)\n")
		}
		return nil
	}
	if err != nil {
		return err
	}
	processRow([]byte(ctx.keyEquals), val)
	return nil
}

func (ctx *selectContext) clearProgressLine() {
	hasValueFilters := len(ctx.valueFilters) > 0
	if (hasValueFilters || ctx.scanned > 10000) && ctx.scanned > 0 {
		fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
	}
}

func (ctx *selectContext) renderResults() {
	if len(ctx.aggs) > 0 {
		printAggregateResults(ctx.aggs)
		return
	}

	if len(ctx.orderBy) > 0 {
		SortRows(ctx.headers, ctx.bufferedRows, ctx.orderBy)
	}

	if len(ctx.bufferedRows) > ctx.limit {
		ctx.bufferedRows = ctx.bufferedRows[:ctx.limit]
	}
	ctx.matchCount = len(ctx.bufferedRows)

	printTable(ctx.headers, ctx.bufferedRows)
}

func (ctx *selectContext) reportStatistics(wasInterrupted bool) {
	if ctx.scanned == 0 && ctx.scanStats.BlocksLoaded == 0 {
		fmt.Printf("(%d rows)\n", ctx.matchCount)
		return
	}

	elapsed := time.Since(ctx.startTime)
	rate := float64(ctx.scanned) / elapsed.Seconds()

	if wasInterrupted {
		fmt.Printf("(%d rows) - interrupted, scanned %s keys (%s blocks) in %s (%s keys/sec)\n",
			ctx.matchCount, formatIntCommas(ctx.scanned),
			formatIntCommas(ctx.scanStats.BlocksLoaded), formatDuration(elapsed), formatIntCommas(int64(rate)))
		return
	}

	blockDetails := fmt.Sprintf("%d blocks", ctx.scanStats.BlocksLoaded)
	if ctx.scanStats.BlocksCacheHit > 0 || ctx.scanStats.BlocksDiskRead > 0 {
		blockDetails = fmt.Sprintf("%d blocks (%d cache, %d disk)",
			ctx.scanStats.BlocksLoaded, ctx.scanStats.BlocksCacheHit, ctx.scanStats.BlocksDiskRead)
	}
	tableDetails := ""
	if ctx.scanStats.TablesChecked > 0 {
		tableDetails = fmt.Sprintf(", %d/%d tables", ctx.scanStats.TablesAdded, ctx.scanStats.TablesChecked)
	}
	fmt.Printf("(%d rows) scanned %s keys, %s%s, %s\n",
		ctx.matchCount, formatIntCommas(ctx.scanned), blockDetails, tableDetails, formatDuration(elapsed))
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
	var err error
	switch val.Type {
	case tinykvs.ValueTypeRecord:
		record = val.Record
	case tinykvs.ValueTypeMsgpack:
		record, err = msgpck.UnmarshalMapStringAny(val.Bytes, false)
		if err != nil {
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
		record, err := msgpck.UnmarshalMapStringAny(val.Bytes, false)
		if err != nil {
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

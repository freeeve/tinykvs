package main

import (
	"sort"
	"strconv"
	"strings"
)

// SortOrder represents a single ORDER BY column
type SortOrder struct {
	Field      string // "k" for key, or field name like "ttl", "v.name"
	Descending bool
}

// SortableRows wraps rows for sorting with ORDER BY
type SortableRows struct {
	Headers []string
	Rows    [][]string
	Orders  []SortOrder
}

func (s *SortableRows) Len() int {
	return len(s.Rows)
}

func (s *SortableRows) Swap(i, j int) {
	s.Rows[i], s.Rows[j] = s.Rows[j], s.Rows[i]
}

func (s *SortableRows) Less(i, j int) bool {
	for _, order := range s.Orders {
		colIdx := s.findColumn(order.Field)
		if colIdx < 0 {
			continue
		}

		valI := s.Rows[i][colIdx]
		valJ := s.Rows[j][colIdx]

		cmp := compareValues(valI, valJ)
		if cmp == 0 {
			continue
		}

		if order.Descending {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

func (s *SortableRows) findColumn(field string) int {
	// Normalize field name
	field = strings.TrimPrefix(field, "v.")
	field = strings.ToLower(field)

	for i, h := range s.Headers {
		if strings.ToLower(h) == field {
			return i
		}
	}
	return -1
}

// compareValues compares two string values, attempting numeric comparison first
func compareValues(a, b string) int {
	// Try numeric comparison first
	numA, errA := strconv.ParseFloat(a, 64)
	numB, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		if numA < numB {
			return -1
		} else if numA > numB {
			return 1
		}
		return 0
	}

	// Fall back to string comparison
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// SortRows sorts rows according to the ORDER BY clauses
func SortRows(headers []string, rows [][]string, orders []SortOrder) {
	if len(orders) == 0 || len(rows) == 0 {
		return
	}

	sortable := &SortableRows{
		Headers: headers,
		Rows:    rows,
		Orders:  orders,
	}
	sort.Stable(sortable)
}

// ParseOrderBy extracts ORDER BY clauses from SQL
// Returns the modified SQL (with ORDER BY removed) and the sort orders
func ParseOrderBy(sql string) (string, []SortOrder) {
	lower := strings.ToLower(sql)
	idx := strings.LastIndex(lower, " order by ")
	if idx == -1 {
		return sql, nil
	}

	orderPart := sql[idx+len(" order by "):]
	sql = sql[:idx]

	// Remove any trailing LIMIT clause from orderPart
	limitIdx := strings.Index(strings.ToLower(orderPart), " limit ")
	if limitIdx != -1 {
		// Put LIMIT back on the main SQL
		sql = sql + orderPart[limitIdx:]
		orderPart = orderPart[:limitIdx]
	}

	// Parse order columns
	var orders []SortOrder
	parts := strings.Split(orderPart, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		order := SortOrder{}
		tokens := strings.Fields(part)
		if len(tokens) >= 1 {
			order.Field = tokens[0]
		}
		if len(tokens) >= 2 {
			if strings.ToUpper(tokens[1]) == "DESC" {
				order.Descending = true
			}
		}
		orders = append(orders, order)
	}

	return sql, orders
}

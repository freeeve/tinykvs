package tinykvs

import (
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

// AggregateResult holds the result of an aggregation query.
type AggregateResult struct {
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
	hasValue bool
}

// Avg returns the average, or 0 if no values.
func (r AggregateResult) Avg() float64 {
	if r.Count == 0 {
		return 0
	}
	return r.Sum / float64(r.Count)
}

// Count returns the number of keys matching the prefix.
func (s *Store) Count(prefix []byte) (int64, error) {
	var count int64
	err := s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		count++
		return true
	})
	return count, err
}

// Sum returns the sum of a numeric field across all records matching the prefix.
// For simple numeric values (no field), pass empty string for field.
func (s *Store) Sum(prefix []byte, field string) (float64, error) {
	var sum float64
	err := s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		if v, ok := extractNumeric(val, field); ok {
			sum += v
		}
		return true
	})
	return sum, err
}

// Avg returns the average of a numeric field across all records matching the prefix.
func (s *Store) Avg(prefix []byte, field string) (float64, error) {
	var sum float64
	var count int64
	err := s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		if v, ok := extractNumeric(val, field); ok {
			sum += v
			count++
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}
	return sum / float64(count), nil
}

// Min returns the minimum of a numeric field across all records matching the prefix.
func (s *Store) Min(prefix []byte, field string) (float64, error) {
	var min float64
	var hasValue bool
	err := s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		if v, ok := extractNumeric(val, field); ok {
			if !hasValue || v < min {
				min = v
				hasValue = true
			}
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if !hasValue {
		return 0, nil
	}
	return min, nil
}

// Max returns the maximum of a numeric field across all records matching the prefix.
func (s *Store) Max(prefix []byte, field string) (float64, error) {
	var max float64
	var hasValue bool
	err := s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		if v, ok := extractNumeric(val, field); ok {
			if !hasValue || v > max {
				max = v
				hasValue = true
			}
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if !hasValue {
		return 0, nil
	}
	return max, nil
}

// Aggregate computes multiple aggregations in a single scan.
// For simple numeric values, pass empty string for field.
func (s *Store) Aggregate(prefix []byte, field string) (AggregateResult, error) {
	var r AggregateResult
	err := s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		r.Count++
		if v, ok := extractNumeric(val, field); ok {
			r.Sum += v
			if !r.hasValue || v < r.Min {
				r.Min = v
			}
			if !r.hasValue || v > r.Max {
				r.Max = v
			}
			r.hasValue = true
		}
		return true
	})
	return r, err
}

// extractNumeric extracts a numeric value from a Value.
// If field is empty, extracts from the value directly.
// If field is specified, extracts from a record/msgpack field (supports nested paths like "address.zip").
func extractNumeric(val Value, field string) (float64, bool) {
	if field == "" {
		switch val.Type {
		case ValueTypeInt64:
			return float64(val.Int64), true
		case ValueTypeFloat64:
			return val.Float64, true
		default:
			return 0, false
		}
	}

	// Extract from record/msgpack
	var record map[string]any
	switch val.Type {
	case ValueTypeRecord:
		record = val.Record
	case ValueTypeMsgpack:
		if err := msgpack.Unmarshal(val.Bytes, &record); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if record == nil {
		return 0, false
	}

	fieldVal := extractNestedField(record, field)
	if fieldVal == nil {
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

// extractNestedField extracts a field from a map, supporting nested paths like "address.city".
func extractNestedField(m map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = m
	for _, part := range parts {
		if currentMap, ok := current.(map[string]any); ok {
			current = currentMap[part]
		} else {
			return nil
		}
	}
	return current
}

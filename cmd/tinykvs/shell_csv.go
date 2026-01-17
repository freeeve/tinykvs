package main

import (
	"bufio"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/freeeve/tinykvs"
	"github.com/vmihailenco/msgpack/v5"
)

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

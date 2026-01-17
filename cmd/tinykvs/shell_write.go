package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/freeeve/tinykvs"
)

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
		var dummyFilters []*valueFilter
		s.parseWhere(stmt.Where.Expr, &keyEquals, &dummy1, &dummy2, &dummy3, &dummyFilters)
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

	// Detect JSON and store as record (same logic as INSERT)
	if strings.HasPrefix(newValue, "{") && strings.HasSuffix(newValue, "}") {
		var record map[string]any
		if err := json.Unmarshal([]byte(newValue), &record); err == nil {
			if err := s.store.PutMap([]byte(keyEquals), record); err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
			fmt.Println("UPDATE 1")
			return
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
	var dummyFilters []*valueFilter

	if stmt.Where != nil {
		s.parseWhere(stmt.Where.Expr, &keyEquals, &keyPrefix, &keyStart, &keyEnd, &dummyFilters)
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

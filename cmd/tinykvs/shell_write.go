package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/freeeve/tinykvs"
)

const msgErrFmt = "Error: %v\n"

func (s *Shell) handleInsert(stmt *sqlparser.Insert) {
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok || len(rows) == 0 {
		fmt.Println("Error: Invalid INSERT syntax")
		return
	}

	inserted := 0
	for _, row := range rows {
		if s.insertRow(row) {
			inserted++
		}
	}

	fmt.Printf("INSERT %d\n", inserted)
}

// insertRow processes a single INSERT row and returns true on success.
func (s *Shell) insertRow(row sqlparser.ValTuple) bool {
	if len(row) < 2 {
		fmt.Println("Error: INSERT requires (key, value)")
		return false
	}

	key := extractValue(row[0])
	if key == "" {
		fmt.Println("Error: key cannot be empty")
		return false
	}

	value, hexBytes, isHex := extractValueAndType(row[1])
	return s.storeInsertValue(key, value, hexBytes, isHex)
}

// storeInsertValue stores a value, trying msgpack, JSON, bytes, or string in order.
func (s *Shell) storeInsertValue(key, value string, hexBytes []byte, isHex bool) bool {
	keyBytes := []byte(key)

	// Try msgpack first if it's hex data that looks like a msgpack map
	if isHex && isMsgpackMap(hexBytes) {
		if s.tryStoreMsgpack(keyBytes, hexBytes) {
			return true
		}
		// Fall through to store as bytes if msgpack decode fails
	}

	// Detect JSON and store as record
	if s.tryStoreJSON(keyBytes, value) {
		return true
	}

	// Store hex data as bytes, strings as strings
	return s.storeRawValue(keyBytes, value, hexBytes, isHex)
}

// tryStoreMsgpack attempts to decode and store msgpack data.
func (s *Shell) tryStoreMsgpack(key, hexBytes []byte) bool {
	record, err := tinykvs.DecodeMsgpack(hexBytes)
	if err != nil {
		return false
	}
	if err := s.store.PutMap(key, record); err != nil {
		fmt.Printf(msgErrFmt, err)
		return false
	}
	return true
}

// tryStoreJSON attempts to parse and store JSON data.
func (s *Shell) tryStoreJSON(key []byte, value string) bool {
	if !strings.HasPrefix(value, "{") || !strings.HasSuffix(value, "}") {
		return false
	}
	var record map[string]any
	if json.Unmarshal([]byte(value), &record) != nil {
		return false
	}
	if err := s.store.PutMap(key, record); err != nil {
		fmt.Printf(msgErrFmt, err)
		return false
	}
	return true
}

// storeRawValue stores a value as either bytes or string.
func (s *Shell) storeRawValue(key []byte, value string, hexBytes []byte, isHex bool) bool {
	var err error
	if isHex {
		err = s.store.PutBytes(key, hexBytes)
	} else {
		err = s.store.PutString(key, value)
	}
	if err != nil {
		fmt.Printf(msgErrFmt, err)
		return false
	}
	return true
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
				fmt.Printf(msgErrFmt, err)
				return
			}
			fmt.Println("UPDATE 1")
			return
		}
	}

	if err := s.store.PutString([]byte(keyEquals), newValue); err != nil {
		fmt.Printf(msgErrFmt, err)
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
			fmt.Printf(msgErrFmt, err)
			return
		}
		fmt.Println("DELETE 1")
	} else if keyPrefix != "" {
		// Delete by prefix
		deleted, err := s.store.DeletePrefix([]byte(keyPrefix))
		if err != nil {
			fmt.Printf(msgErrFmt, err)
			return
		}
		fmt.Printf("DELETE %d\n", deleted)
	} else if keyStart != "" && keyEnd != "" {
		// Delete range
		deleted, err := s.store.DeleteRange([]byte(keyStart), []byte(keyEnd))
		if err != nil {
			fmt.Printf(msgErrFmt, err)
			return
		}
		fmt.Printf("DELETE %d\n", deleted)
	} else {
		fmt.Println("Error: DELETE requires WHERE clause")
	}
}

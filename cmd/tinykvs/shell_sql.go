package main

import (
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// preprocessFunctions expands SQL function calls to hex literals.
// Supported functions:
//   - uint64_be(n) → 8-byte big-endian encoding of n
//   - uint64_le(n) → 8-byte little-endian encoding of n
//   - uint32_be(n) → 4-byte big-endian encoding of n
//   - uint32_le(n) → 4-byte little-endian encoding of n
//   - byte(n) → single byte (0-255)
//   - fnv64(s) → FNV-1a 64-bit hash of string s
//
// Concatenation with || operator:
//   - x'14' || uint64_be(28708) → x'140000000000007024'
//
// Examples:
//   - k STARTS WITH x'10' || uint64_be(7341141)
//   - k STARTS WITH byte(0x14) || uint64_be(28708) || fnv64('library-123')
func preprocessFunctions(sql string) string {
	// First expand individual functions to x'...' literals
	sql = expandFunction(sql, "uint64_be", func(arg string) ([]byte, bool) {
		val, err := parseUint(arg)
		if err != nil {
			return nil, false
		}
		b := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			b[7-i] = byte(val >> (i * 8))
		}
		return b, true
	})

	sql = expandFunction(sql, "uint64_le", func(arg string) ([]byte, bool) {
		val, err := parseUint(arg)
		if err != nil {
			return nil, false
		}
		b := make([]byte, 8)
		for i := 0; i < 8; i++ {
			b[i] = byte(val >> (i * 8))
		}
		return b, true
	})

	sql = expandFunction(sql, "uint32_be", func(arg string) ([]byte, bool) {
		val, err := parseUint(arg)
		if err != nil {
			return nil, false
		}
		b := make([]byte, 4)
		for i := 3; i >= 0; i-- {
			b[3-i] = byte(val >> (i * 8))
		}
		return b, true
	})

	sql = expandFunction(sql, "uint32_le", func(arg string) ([]byte, bool) {
		val, err := parseUint(arg)
		if err != nil {
			return nil, false
		}
		b := make([]byte, 4)
		for i := 0; i < 4; i++ {
			b[i] = byte(val >> (i * 8))
		}
		return b, true
	})

	sql = expandFunction(sql, "byte", func(arg string) ([]byte, bool) {
		val, err := parseUint(arg)
		if err != nil || val > 255 {
			return nil, false
		}
		return []byte{byte(val)}, true
	})

	sql = expandFunction(sql, "fnv64", func(arg string) ([]byte, bool) {
		// Strip quotes if present
		s := strings.Trim(arg, "'\"")
		h := fnv64a(s)
		b := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			b[7-i] = byte(h >> (i * 8))
		}
		return b, true
	})

	// Now concatenate adjacent x'...' || x'...' literals
	sql = concatenateHexLiterals(sql)

	return sql
}

// parseUint parses a uint64 from decimal or hex (0x...) string
func parseUint(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToLower(s), "0x") {
		return strconv.ParseUint(s[2:], 16, 64)
	}
	return strconv.ParseUint(s, 10, 64)
}

// fnv64a computes FNV-1a 64-bit hash
func fnv64a(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// expandFunction finds and expands a single-argument function call to hex literal
func expandFunction(sql, funcName string, encode func(string) ([]byte, bool)) string {
	for {
		lower := strings.ToLower(sql)
		idx := strings.Index(lower, strings.ToLower(funcName)+"(")
		if idx == -1 {
			return sql
		}

		start := idx + len(funcName) + 1
		depth := 1
		end := start
		for end < len(sql) && depth > 0 {
			if sql[end] == '(' {
				depth++
			} else if sql[end] == ')' {
				depth--
			}
			end++
		}
		if depth != 0 {
			return sql
		}

		arg := strings.TrimSpace(sql[start : end-1])
		bytes, ok := encode(arg)
		if !ok {
			return sql
		}

		hexStr := hex.EncodeToString(bytes)
		sql = sql[:idx] + "x'" + hexStr + "'" + sql[end:]
	}
}

// concatenateHexLiterals joins adjacent hex literals: x'ab' || x'cd' → x'abcd'
func concatenateHexLiterals(sql string) string {
	for {
		// Find pattern: x'...' followed by optional whitespace, ||, optional whitespace, x'...'
		idx := strings.Index(sql, "' ||")
		if idx == -1 {
			return sql
		}

		// Check if there's x' before the closing quote
		startQuote := strings.LastIndex(sql[:idx], "x'")
		if startQuote == -1 {
			// Try next occurrence
			sql = sql[:idx] + "'\x00||" + sql[idx+4:] // Mark as processed
			continue
		}

		// Get the first hex value
		hex1 := sql[startQuote+2 : idx]

		// Find x' after ||
		afterPipe := sql[idx+4:]
		afterPipe = strings.TrimSpace(afterPipe)
		if !strings.HasPrefix(strings.ToLower(afterPipe), "x'") {
			sql = sql[:idx] + "'\x00||" + sql[idx+4:]
			continue
		}

		// Find end of second hex literal
		endQuote := strings.Index(afterPipe[2:], "'")
		if endQuote == -1 {
			return sql
		}
		hex2 := afterPipe[2 : 2+endQuote]

		// Calculate where the second literal ends in original string
		afterPipeStart := idx + 4 + (len(sql[idx+4:]) - len(afterPipe))
		secondLiteralEnd := afterPipeStart + 2 + endQuote + 1

		// Replace both literals with concatenated version
		sql = sql[:startQuote] + "x'" + hex1 + hex2 + "'" + sql[secondLiteralEnd:]
	}
}

// preprocessStartsWith converts "STARTS WITH" syntax to LIKE syntax before SQL parsing.
// Converts:
//   - k STARTS WITH x'14' → k LIKE '$$HEX$$14%'
//   - k STARTS WITH '14' → k LIKE '14%'
func preprocessStartsWith(sql string) string {
	// Case-insensitive match for "STARTS WITH"
	lower := strings.ToLower(sql)
	idx := strings.Index(lower, "starts with")
	if idx == -1 {
		return sql
	}

	// Find what comes after "starts with"
	afterIdx := idx + len("starts with")
	after := strings.TrimSpace(sql[afterIdx:])

	// Check if it's a hex literal x'...' or X'...'
	if len(after) >= 4 && (after[0] == 'x' || after[0] == 'X') && after[1] == '\'' {
		// Find closing quote
		endQuote := strings.Index(after[2:], "'")
		if endQuote != -1 {
			hexVal := after[2 : 2+endQuote]
			rest := after[2+endQuote+1:]
			// Convert to: LIKE '$$HEX$$<hexval>%'
			return sql[:idx] + "LIKE '$$HEX$$" + hexVal + "%'" + rest
		}
	} else if len(after) >= 2 && after[0] == '\'' {
		// String literal '...'
		endQuote := strings.Index(after[1:], "'")
		if endQuote != -1 {
			strVal := after[1 : 1+endQuote]
			rest := after[1+endQuote+1:]
			// Convert to: LIKE '<strval>%'
			return sql[:idx] + "LIKE '" + strVal + "%'" + rest
		}
	}

	return sql
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

// extractValueForLike extracts a value, with special handling for hex LIKE patterns.
// Supports:
//   - '0x14%' syntax - string starting with 0x followed by hex bytes and %
//   - '$$HEX$$14%' syntax - preprocessed from STARTS WITH x'14'
func extractValueForLike(expr sqlparser.Expr, operator string) (string, bool) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			s := string(v.Val)
			if operator == "like" {
				// Support '$$HEX$$14%' from preprocessed STARTS WITH x'14'
				if strings.HasPrefix(s, "$$HEX$$") {
					hexPart := s[7:] // skip $$HEX$$
					if strings.HasSuffix(hexPart, "%") {
						hexPart = hexPart[:len(hexPart)-1]
						decoded, err := hex.DecodeString(hexPart)
						if err != nil {
							return s, false
						}
						return string(decoded), true
					}
				}
				// Support '0x14%' or '0X14%' syntax for hex prefix matching
				if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
					hexPart := s[2:]
					if strings.HasSuffix(hexPart, "%") {
						hexPart = hexPart[:len(hexPart)-1]
						decoded, err := hex.DecodeString(hexPart)
						if err != nil {
							return s, false
						}
						return string(decoded), true
					}
				}
			}
			return s, false
		case sqlparser.HexVal:
			decoded, _ := hexDecode(string(v.Val))
			return string(decoded), false
		case sqlparser.IntVal:
			return string(v.Val), false
		}
	}
	return "", false
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

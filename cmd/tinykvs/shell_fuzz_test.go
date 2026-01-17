package main

import (
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

// ============================================================================
// Fuzz Tests
// ============================================================================

func FuzzShellSQL(f *testing.F) {
	// Seed corpus with valid SQL
	seeds := []string{
		"SELECT * FROM kv",
		"SELECT * FROM kv WHERE k = 'test'",
		"SELECT * FROM kv WHERE k LIKE 'prefix%'",
		"SELECT * FROM kv WHERE k BETWEEN 'a' AND 'z'",
		"SELECT * FROM kv LIMIT 10",
		"INSERT INTO kv VALUES ('key', 'value')",
		"INSERT INTO kv (k, v) VALUES ('key', 'value')",
		"UPDATE kv SET v = 'new' WHERE k = 'key'",
		"DELETE FROM kv WHERE k = 'key'",
		"DELETE FROM kv WHERE k LIKE 'prefix%'",
		"SELECT v.name FROM kv WHERE k = 'user'",
		`INSERT INTO kv VALUES ('u', '{"name":"test"}')`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()
		shell := NewShell(store)

		// Execute should not panic
		captureOutput(t, func() {
			shell.execute(sql)
		})
	})
}

func FuzzShellJSONInsert(f *testing.F) {
	// Seed corpus with JSON values
	seeds := []string{
		`{"name":"Alice"}`,
		`{"a":1,"b":2}`,
		`{"nested":{"inner":"value"}}`,
		`{"array":[1,2,3]}`,
		`{"bool":true,"null":null}`,
		`{}`,
		`{"key":"value","number":42.5}`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, jsonStr string) {
		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()
		shell := NewShell(store)

		// Insert should not panic
		sql := "INSERT INTO kv VALUES ('key', '" + strings.ReplaceAll(jsonStr, "'", "''") + "')"
		captureOutput(t, func() {
			shell.execute(sql)
		})

		// Select should not panic
		captureOutput(t, func() {
			shell.execute("SELECT * FROM kv WHERE k = 'key'")
		})
	})
}

func FuzzShellCommands(f *testing.F) {
	// Seed corpus with shell commands
	seeds := []string{
		"\\help",
		"\\stats",
		"\\tables",
		"\\flush",
		"\\compact",
		"\\q",
		"\\quit",
		"\\exit",
		"\\unknown",
		"\\export /tmp/test.csv",
		"\\import /tmp/test.csv",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, cmd string) {
		// Only fuzz commands starting with backslash
		if !strings.HasPrefix(cmd, "\\") {
			return
		}

		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()
		shell := NewShell(store)

		// Command should not panic (but might return false for quit)
		captureOutput(t, func() {
			shell.execute(cmd)
		})
	})
}

func FuzzShellOrderBy(f *testing.F) {
	// Seed corpus with ORDER BY queries
	seeds := []string{
		"SELECT * FROM kv ORDER BY k",
		"SELECT * FROM kv ORDER BY k DESC",
		"SELECT * FROM kv ORDER BY k ASC",
		"SELECT * FROM kv ORDER BY k DESC LIMIT 10",
		"SELECT * FROM kv ORDER BY v.name",
		"SELECT * FROM kv ORDER BY v.age DESC",
		"SELECT * FROM kv ORDER BY v.name ASC, v.age DESC",
		"SELECT * FROM kv WHERE k LIKE 'user:%' ORDER BY v.score DESC LIMIT 5",
		"SELECT v.name, v.age FROM kv ORDER BY v.age",
		"SELECT * FROM kv ORDER BY v.nested.field DESC",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Only fuzz queries with ORDER BY
		if !strings.Contains(strings.ToUpper(sql), "ORDER BY") {
			return
		}

		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()

		// Insert some test data
		shell := NewShell(store)
		captureOutput(t, func() {
			shell.execute(`INSERT INTO kv VALUES ('user:1', '{"name":"Alice","age":30,"score":95}')`)
			shell.execute(`INSERT INTO kv VALUES ('user:2', '{"name":"Bob","age":25,"score":87}')`)
			shell.execute(`INSERT INTO kv VALUES ('user:3', '{"name":"Carol","age":35,"score":92}')`)
		})

		// Execute should not panic
		captureOutput(t, func() {
			shell.execute(sql)
		})
	})
}

func FuzzShellBinaryFunctions(f *testing.F) {
	// Seed corpus with binary key function queries
	seeds := []string{
		"SELECT * FROM kv WHERE k = uint64_be(12345)",
		"SELECT * FROM kv WHERE k = uint64_le(12345)",
		"SELECT * FROM kv WHERE k = uint32_be(12345)",
		"SELECT * FROM kv WHERE k = uint32_le(12345)",
		"SELECT * FROM kv WHERE k = byte(0x14)",
		"SELECT * FROM kv WHERE k = fnv64('test')",
		"SELECT * FROM kv WHERE k = x'deadbeef'",
		"SELECT * FROM kv WHERE k = x'14' || uint64_be(28708)",
		"SELECT * FROM kv WHERE k = byte(0x10) || uint64_be(12345)",
		"SELECT * FROM kv WHERE k LIKE byte(0x14) || fnv64('user') || '%'",
		"SELECT * FROM kv WHERE k = x'14' || uint64_be(100) || fnv64('item')",
		"INSERT INTO kv VALUES (uint64_be(999), 'test')",
		"INSERT INTO kv VALUES (x'cafe' || uint32_be(123), 'value')",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Only fuzz queries with binary functions or hex literals
		upper := strings.ToUpper(sql)
		hasBinaryFunc := strings.Contains(upper, "UINT64") ||
			strings.Contains(upper, "UINT32") ||
			strings.Contains(upper, "BYTE(") ||
			strings.Contains(upper, "FNV64") ||
			strings.Contains(sql, "x'") ||
			strings.Contains(sql, "X'")
		if !hasBinaryFunc {
			return
		}

		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()
		shell := NewShell(store)

		// Execute should not panic
		captureOutput(t, func() {
			shell.execute(sql)
		})
	})
}

func FuzzShellAggregations(f *testing.F) {
	// Seed corpus with aggregation queries
	seeds := []string{
		"SELECT count() FROM kv",
		"SELECT count() FROM kv WHERE k LIKE 'user:%'",
		"SELECT sum(v.age) FROM kv",
		"SELECT avg(v.age) FROM kv",
		"SELECT min(v.score) FROM kv",
		"SELECT max(v.score) FROM kv",
		"SELECT count(), sum(v.age), avg(v.age) FROM kv",
		"SELECT min(v.score), max(v.score) FROM kv WHERE k LIKE 'user:%'",
		"SELECT sum(v.nested.value) FROM kv",
		"SELECT count(), avg(v.price) FROM kv WHERE k BETWEEN 'a' AND 'z'",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Only fuzz queries with aggregation functions
		upper := strings.ToUpper(sql)
		hasAgg := strings.Contains(upper, "COUNT(") ||
			strings.Contains(upper, "SUM(") ||
			strings.Contains(upper, "AVG(") ||
			strings.Contains(upper, "MIN(") ||
			strings.Contains(upper, "MAX(")
		if !hasAgg {
			return
		}

		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()

		// Insert some test data
		shell := NewShell(store)
		captureOutput(t, func() {
			shell.execute(`INSERT INTO kv VALUES ('user:1', '{"name":"Alice","age":30,"score":95}')`)
			shell.execute(`INSERT INTO kv VALUES ('user:2', '{"name":"Bob","age":25,"score":87}')`)
			shell.execute(`INSERT INTO kv VALUES ('item:1', '{"price":19.99,"nested":{"value":10}}')`)
		})

		// Execute should not panic
		captureOutput(t, func() {
			shell.execute(sql)
		})
	})
}

func FuzzShellNestedFields(f *testing.F) {
	// Seed corpus with nested field access
	seeds := []string{
		"SELECT v.name FROM kv WHERE k = 'user:1'",
		"SELECT v.address.city FROM kv WHERE k = 'user:1'",
		"SELECT v.`address.geo.lat` FROM kv WHERE k = 'user:1'",
		"SELECT v.a.b.c FROM kv",
		"SELECT v.deeply.nested.field.value FROM kv LIMIT 10",
		"SELECT v.arr FROM kv WHERE k LIKE 'data:%'",
		"SELECT v.missing.field FROM kv",
		"SELECT v.`weird.key.with.dots` FROM kv",
		"SELECT v.name, v.address.city, v.address.zip FROM kv",
		"UPDATE kv SET v = 'test' WHERE k = 'user:1'",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Only fuzz queries with field access
		if !strings.Contains(sql, "v.") {
			return
		}

		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()

		// Insert test data with nested structures
		shell := NewShell(store)
		captureOutput(t, func() {
			shell.execute(`INSERT INTO kv VALUES ('user:1', '{"name":"Alice","address":{"city":"NYC","geo":{"lat":40.7,"lon":-74.0}}}')`)
			shell.execute(`INSERT INTO kv VALUES ('data:1', '{"arr":[1,2,3],"deeply":{"nested":{"field":{"value":42}}}}')`)
		})

		// Execute should not panic
		captureOutput(t, func() {
			shell.execute(sql)
		})
	})
}

func FuzzShellHexValues(f *testing.F) {
	// Seed corpus with hex value inserts and queries
	seeds := []string{
		"INSERT INTO kv VALUES ('hex1', x'deadbeef')",
		"INSERT INTO kv VALUES ('hex2', x'00112233')",
		"INSERT INTO kv VALUES ('hex3', x'cafebabe')",
		"INSERT INTO kv VALUES ('msgpack', x'82a46e616d65a3426f62a3616765')",
		"SELECT * FROM kv WHERE k = 'hex1'",
		"INSERT INTO kv VALUES (x'0102', 'binary key')",
		"INSERT INTO kv VALUES ('empty', x'')",
		"INSERT INTO kv VALUES ('single', x'ff')",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Only fuzz queries with hex literals
		if !strings.Contains(sql, "x'") && !strings.Contains(sql, "X'") {
			return
		}

		dir := t.TempDir()
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Skip("Could not open store")
		}
		defer store.Close()
		shell := NewShell(store)

		// Execute should not panic
		captureOutput(t, func() {
			shell.execute(sql)
		})
	})
}

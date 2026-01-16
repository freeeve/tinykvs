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

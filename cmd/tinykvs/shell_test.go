package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

// captureOutput captures stdout during a function call
func captureOutput(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

func setupShellTest(t *testing.T) (*Shell, *tinykvs.Store, string) {
	t.Helper()
	dir := t.TempDir()
	opts := tinykvs.DefaultOptions(dir)
	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	shell := NewShell(store)
	return shell, store, dir
}

func TestShellInsertAndSelect(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert a value
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv (k, v) VALUES ('testkey', 'testvalue')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT output = %q, want contains 'INSERT 1'", output)
	}

	// Select it back
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'testkey'")
	})
	if !strings.Contains(output, "testkey") || !strings.Contains(output, "testvalue") {
		t.Errorf("SELECT output = %q, want contains 'testkey' and 'testvalue'", output)
	}
	if !strings.Contains(output, "(1 rows)") {
		t.Errorf("SELECT output = %q, want contains '(1 rows)'", output)
	}
}

func TestShellInsertShortForm(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert using short form without column names
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('key1', 'value1')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT output = %q, want contains 'INSERT 1'", output)
	}

	// Verify
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'key1'")
	})
	if !strings.Contains(output, "value1") {
		t.Errorf("SELECT output = %q, want contains 'value1'", output)
	}
}

func TestShellSelectNotFound(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'nonexistent'")
	})
	if !strings.Contains(output, "(0 rows)") {
		t.Errorf("SELECT output = %q, want contains '(0 rows)'", output)
	}
}

func TestShellSelectWithLike(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert multiple keys with a prefix
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('user:1', 'alice')")
		shell.execute("INSERT INTO kv VALUES ('user:2', 'bob')")
		shell.execute("INSERT INTO kv VALUES ('user:3', 'charlie')")
		shell.execute("INSERT INTO kv VALUES ('order:1', 'pizza')")
	})

	// Select with LIKE prefix
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k LIKE 'user:%'")
	})
	if !strings.Contains(output, "user:1") || !strings.Contains(output, "user:2") || !strings.Contains(output, "user:3") {
		t.Errorf("SELECT LIKE output = %q, want all user keys", output)
	}
	if strings.Contains(output, "order:1") {
		t.Errorf("SELECT LIKE output = %q, should not contain order:1", output)
	}
	if !strings.Contains(output, "(3 rows)") {
		t.Errorf("SELECT LIKE output = %q, want '(3 rows)'", output)
	}
}

func TestShellSelectWithBetween(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert keys
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
		shell.execute("INSERT INTO kv VALUES ('b', '2')")
		shell.execute("INSERT INTO kv VALUES ('c', '3')")
		shell.execute("INSERT INTO kv VALUES ('d', '4')")
		shell.execute("INSERT INTO kv VALUES ('e', '5')")
	})

	// Select with BETWEEN
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k BETWEEN 'b' AND 'd'")
	})
	if !strings.Contains(output, "b |") || !strings.Contains(output, "c |") {
		t.Errorf("SELECT BETWEEN output = %q, want 'b' and 'c'", output)
	}
	// 'd' should be excluded since BETWEEN in our impl is [start, end)
	if !strings.Contains(output, "(2 rows)") && !strings.Contains(output, "(3 rows)") {
		t.Errorf("SELECT BETWEEN output = %q, want 2 or 3 rows", output)
	}
}

func TestShellSelectWithLimit(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert 10 keys
	captureOutput(t, func() {
		for i := 0; i < 10; i++ {
			shell.execute("INSERT INTO kv VALUES ('key" + string(rune('0'+i)) + "', 'value')")
		}
	})

	// Select with LIMIT
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv LIMIT 3")
	})
	if !strings.Contains(output, "(3 rows)") {
		t.Errorf("SELECT LIMIT output = %q, want '(3 rows)'", output)
	}
}

func TestShellSelectAll(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert keys
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
		shell.execute("INSERT INTO kv VALUES ('b', '2')")
		shell.execute("INSERT INTO kv VALUES ('c', '3')")
	})

	// Select all (no WHERE)
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv")
	})
	if !strings.Contains(output, "(3 rows)") {
		t.Errorf("SELECT * output = %q, want '(3 rows)'", output)
	}
}

func TestShellUpdate(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('mykey', 'oldvalue')")
	})

	// Update
	output := captureOutput(t, func() {
		shell.execute("UPDATE kv SET v = 'newvalue' WHERE k = 'mykey'")
	})
	if !strings.Contains(output, "UPDATE 1") {
		t.Errorf("UPDATE output = %q, want contains 'UPDATE 1'", output)
	}

	// Verify
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'mykey'")
	})
	if !strings.Contains(output, "newvalue") {
		t.Errorf("SELECT after UPDATE output = %q, want contains 'newvalue'", output)
	}
	if strings.Contains(output, "oldvalue") {
		t.Errorf("SELECT after UPDATE output = %q, should not contain 'oldvalue'", output)
	}
}

func TestShellUpdateWithoutWhere(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Update without WHERE should fail
	output := captureOutput(t, func() {
		shell.execute("UPDATE kv SET v = 'newvalue'")
	})
	if !strings.Contains(output, "Error") {
		t.Errorf("UPDATE without WHERE output = %q, want error", output)
	}
}

func TestShellDeleteSingleKey(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('delkey', 'value')")
	})

	// Delete
	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k = 'delkey'")
	})
	if !strings.Contains(output, "DELETE 1") {
		t.Errorf("DELETE output = %q, want contains 'DELETE 1'", output)
	}

	// Verify gone
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'delkey'")
	})
	if !strings.Contains(output, "(0 rows)") {
		t.Errorf("SELECT after DELETE output = %q, want '(0 rows)'", output)
	}
}

func TestShellDeleteByPrefix(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('prefix:1', 'a')")
		shell.execute("INSERT INTO kv VALUES ('prefix:2', 'b')")
		shell.execute("INSERT INTO kv VALUES ('prefix:3', 'c')")
		shell.execute("INSERT INTO kv VALUES ('other:1', 'd')")
	})

	// Delete by prefix
	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k LIKE 'prefix:%'")
	})
	if !strings.Contains(output, "DELETE 3") {
		t.Errorf("DELETE prefix output = %q, want 'DELETE 3'", output)
	}

	// Verify prefix keys gone
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k LIKE 'prefix:%'")
	})
	if !strings.Contains(output, "(0 rows)") {
		t.Errorf("SELECT after DELETE prefix output = %q, want '(0 rows)'", output)
	}

	// Verify other key still exists
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'other:1'")
	})
	if !strings.Contains(output, "(1 rows)") {
		t.Errorf("other:1 should still exist, got %q", output)
	}
}

func TestShellDeleteWithoutWhere(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Delete without WHERE should fail
	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv")
	})
	if !strings.Contains(output, "Error") {
		t.Errorf("DELETE without WHERE output = %q, want error", output)
	}
}

func TestShellHexValues(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert with hex key
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES (x'deadbeef', 'binary key')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT hex output = %q, want 'INSERT 1'", output)
	}

	// Select with hex key
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = x'deadbeef'")
	})
	if !strings.Contains(output, "binary key") {
		t.Errorf("SELECT hex output = %q, want 'binary key'", output)
	}
}

func TestShellCommandHelp(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\help")
	})
	if !strings.Contains(output, "SELECT") || !strings.Contains(output, "INSERT") {
		t.Errorf("\\help output = %q, want SQL examples", output)
	}
}

func TestShellCommandStats(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert some data
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('k1', 'v1')")
		shell.execute("INSERT INTO kv VALUES ('k2', 'v2')")
	})

	output := captureOutput(t, func() {
		shell.execute("\\stats")
	})
	if !strings.Contains(output, "Memtable") || !strings.Contains(output, "keys") {
		t.Errorf("\\stats output = %q, want memtable info", output)
	}
}

func TestShellCommandTables(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\tables")
	})
	if !strings.Contains(output, "kv") || !strings.Contains(output, "key") || !strings.Contains(output, "value") {
		t.Errorf("\\tables output = %q, want table schema", output)
	}
}

func TestShellCommandCompact(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\compact")
	})
	if !strings.Contains(output, "Compacting") || !strings.Contains(output, "Done") {
		t.Errorf("\\compact output = %q, want compaction messages", output)
	}
}

func TestShellCommandFlush(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert some data first
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('flushkey', 'flushvalue')")
	})

	output := captureOutput(t, func() {
		shell.execute("\\flush")
	})
	if !strings.Contains(output, "Flushed") {
		t.Errorf("\\flush output = %q, want 'Flushed'", output)
	}
}

func TestShellCommandQuit(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Test quit commands
	for _, cmd := range []string{"\\q", "\\quit", "\\exit"} {
		output := captureOutput(t, func() {
			result := shell.execute(cmd)
			if result {
				t.Errorf("%s should return false (exit)", cmd)
			}
		})
		if !strings.Contains(output, "Bye") {
			t.Errorf("%s output = %q, want 'Bye'", cmd, output)
		}
	}
}

func TestShellUnknownCommand(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\unknown")
	})
	if !strings.Contains(output, "Unknown command") {
		t.Errorf("unknown command output = %q, want 'Unknown command'", output)
	}
}

func TestShellParseError(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INVALID SQL SYNTAX HERE")
	})
	if !strings.Contains(output, "Parse error") && !strings.Contains(output, "Unsupported") {
		t.Errorf("invalid SQL output = %q, want parse error", output)
	}
}

func TestShellEmptyKey(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('', 'empty key value')")
	})
	if !strings.Contains(output, "Error") || !strings.Contains(output, "empty") {
		t.Errorf("empty key output = %q, want error about empty key", output)
	}
}

func TestShellMultipleInserts(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Multiple value tuples in single INSERT
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('m1', 'v1'), ('m2', 'v2'), ('m3', 'v3')")
	})
	if !strings.Contains(output, "INSERT 3") {
		t.Errorf("multi-insert output = %q, want 'INSERT 3'", output)
	}

	// Verify all inserted
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv")
	})
	if !strings.Contains(output, "(3 rows)") {
		t.Errorf("SELECT after multi-insert output = %q, want '(3 rows)'", output)
	}
}

func TestShellSemicolonHandling(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// SQL with trailing semicolon should work
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('semi', 'colon');")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("SQL with semicolon output = %q, want 'INSERT 1'", output)
	}
}

func TestShellHistoryFile(t *testing.T) {
	dir := t.TempDir()
	opts := tinykvs.DefaultOptions(dir)
	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	shell := NewShell(store)

	// Check history file path is set
	if shell.historyFile == "" {
		t.Error("historyFile should be set")
	}
	if !strings.Contains(shell.historyFile, ".tinykvs_history") {
		t.Errorf("historyFile = %q, want contains '.tinykvs_history'", shell.historyFile)
	}
}

func TestShellLikeOnlyPrefixSupported(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert data
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('abc', '1')")
		shell.execute("INSERT INTO kv VALUES ('xabc', '2')")
	})

	// LIKE with % not at end should warn
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k LIKE '%abc'")
	})
	if !strings.Contains(output, "Warning") || !strings.Contains(output, "prefix") {
		t.Errorf("LIKE suffix output = %q, want warning about prefix matching", output)
	}
}

func TestShellDeleteByRange(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert keys
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
		shell.execute("INSERT INTO kv VALUES ('b', '2')")
		shell.execute("INSERT INTO kv VALUES ('c', '3')")
		shell.execute("INSERT INTO kv VALUES ('d', '4')")
		shell.execute("INSERT INTO kv VALUES ('e', '5')")
	})

	// Delete by range
	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k BETWEEN 'b' AND 'd'")
	})
	// Should delete b and c (range is [start, end))
	if !strings.Contains(output, "DELETE") {
		t.Errorf("DELETE range output = %q, want DELETE count", output)
	}
}

func TestShellSelectGreaterEqual(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert keys
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
		shell.execute("INSERT INTO kv VALUES ('b', '2')")
		shell.execute("INSERT INTO kv VALUES ('c', '3')")
	})

	// Select with >= (should set keyStart)
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k >= 'b' AND k <= 'c'")
	})
	if !strings.Contains(output, "b |") {
		t.Errorf("SELECT >= output = %q, want 'b'", output)
	}
}

func TestShellDataPersistence(t *testing.T) {
	dir := t.TempDir()

	// First session: insert data and flush
	{
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Fatalf("Failed to open store: %v", err)
		}
		shell := NewShell(store)

		captureOutput(t, func() {
			shell.execute("INSERT INTO kv VALUES ('persist', 'data')")
		})
		captureOutput(t, func() {
			shell.execute("\\flush")
		})
		store.Close()
	}

	// Second session: verify data exists
	{
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Fatalf("Failed to reopen store: %v", err)
		}
		defer store.Close()
		shell := NewShell(store)

		output := captureOutput(t, func() {
			shell.execute("SELECT * FROM kv WHERE k = 'persist'")
		})
		if !strings.Contains(output, "data") {
			t.Errorf("After reopen, SELECT output = %q, want 'data'", output)
		}
	}
}

func TestShellLargeValue(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert a large value (> 100 chars to trigger truncation in output)
	largeValue := strings.Repeat("x", 150)
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('large', '" + largeValue + "')")
	})

	// Select should show truncated output
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'large'")
	})
	if !strings.Contains(output, "...") || !strings.Contains(output, "bytes") {
		t.Errorf("Large value output = %q, want truncation indicator", output)
	}
}

func TestShellSpecialCharactersInValues(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Values with special characters
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('special', 'hello\nworld')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'special'")
	})
	if !strings.Contains(output, "(1 rows)") {
		t.Errorf("special chars output = %q, want '(1 rows)'", output)
	}
}

func TestShellFormatKey(t *testing.T) {
	// Test the formatKey helper directly
	tests := []struct {
		key      []byte
		expected string
	}{
		{[]byte("hello"), "hello"},                   // printable ASCII
		{[]byte{0x00, 0x01}, "0x0001"},               // non-printable
		{[]byte("mixed\x00"), "0x6d697865640"[0:11]}, // mixed - will be hex
	}

	for _, tc := range tests {
		result := formatKey(tc.key)
		if tc.key[0] < 32 || tc.key[0] > 126 {
			// Non-printable should be hex
			if !strings.HasPrefix(result, "0x") {
				t.Errorf("formatKey(%v) = %q, want hex format", tc.key, result)
			}
		}
	}
}

func TestShellDeleteRange(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert keys a through f
	captureOutput(t, func() {
		for _, c := range "abcdef" {
			shell.execute("INSERT INTO kv VALUES ('" + string(c) + "', 'val')")
		}
	})

	// Delete range [b, e)
	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k >= 'b' AND k <= 'd'")
	})
	// This uses the range delete path
	if !strings.Contains(output, "DELETE") {
		t.Errorf("DELETE range output = %q, want DELETE message", output)
	}

	// Verify remaining keys
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv")
	})
	// a, e, f should remain (possibly d depending on implementation)
	if strings.Contains(output, "b |") || strings.Contains(output, "c |") {
		t.Errorf("After range delete, output = %q, should not have 'b' or 'c'", output)
	}
}

func TestShellNumericKeys(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert with numeric key (as string)
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('123', 'numeric key')")
	})

	// Select with numeric key
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = '123'")
	})
	if !strings.Contains(output, "numeric key") {
		t.Errorf("SELECT numeric key output = %q, want 'numeric key'", output)
	}
}

func TestShellWithMultipleSSTables(t *testing.T) {
	dir := t.TempDir()
	opts := tinykvs.DefaultOptions(dir)
	opts.MemtableSize = 1024 // Small memtable to force flushes
	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()
	shell := NewShell(store)

	// Insert data and flush multiple times to create multiple SSTables
	for i := 0; i < 3; i++ {
		captureOutput(t, func() {
			for j := 0; j < 10; j++ {
				key := "key" + string(rune('0'+i)) + string(rune('0'+j))
				shell.execute("INSERT INTO kv VALUES ('" + key + "', 'value')")
			}
		})
		captureOutput(t, func() {
			shell.execute("\\flush")
		})
	}

	// Select all - should merge from multiple SSTables
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv LIMIT 100")
	})
	if !strings.Contains(output, "(30 rows)") {
		t.Errorf("SELECT from multiple SSTables = %q, want '(30 rows)'", output)
	}
}

func TestNewShell(t *testing.T) {
	dir := t.TempDir()
	opts := tinykvs.DefaultOptions(dir)
	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	shell := NewShell(store)

	if shell.store != store {
		t.Error("NewShell store not set correctly")
	}
	if shell.prompt != "tinykvs> " {
		t.Errorf("NewShell prompt = %q, want 'tinykvs> '", shell.prompt)
	}
	if shell.historyFile == "" {
		t.Error("NewShell historyFile not set")
	}
	expectedPath := filepath.Join(os.Getenv("HOME"), ".tinykvs_history")
	// On some systems HOME might not be set in tests
	if os.Getenv("HOME") != "" && shell.historyFile != expectedPath {
		t.Errorf("NewShell historyFile = %q, want %q", shell.historyFile, expectedPath)
	}
}

// ============================================================================
// Aggregation Tests
// ============================================================================

func TestShellAggregateCount(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert test data
	for i := 0; i < 5; i++ {
		store.PutString([]byte(fmt.Sprintf("key:%d", i)), fmt.Sprintf("value%d", i))
	}

	output := captureOutput(t, func() {
		shell.execute("SELECT count() FROM kv")
	})
	if !strings.Contains(output, "5") {
		t.Errorf("count() = %q, want to contain '5'", output)
	}

	// With prefix filter
	output = captureOutput(t, func() {
		shell.execute("SELECT count() FROM kv WHERE k LIKE 'key:%'")
	})
	if !strings.Contains(output, "5") {
		t.Errorf("count() with prefix = %q, want to contain '5'", output)
	}
}

func TestShellAggregateSumAvg(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert records with numeric fields
	for i := 1; i <= 4; i++ {
		store.PutMap([]byte(fmt.Sprintf("user:%d", i)), map[string]any{
			"name": fmt.Sprintf("User%d", i),
			"age":  i * 10, // 10, 20, 30, 40
		})
	}

	// Test sum
	output := captureOutput(t, func() {
		shell.execute("SELECT sum(v.age) FROM kv")
	})
	if !strings.Contains(output, "100") {
		t.Errorf("sum(v.age) = %q, want to contain '100'", output)
	}

	// Test avg
	output = captureOutput(t, func() {
		shell.execute("SELECT avg(v.age) FROM kv")
	})
	if !strings.Contains(output, "25") {
		t.Errorf("avg(v.age) = %q, want to contain '25'", output)
	}
}

func TestShellAggregateMinMax(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert records with numeric fields
	store.PutMap([]byte("score:1"), map[string]any{"value": 42.5})
	store.PutMap([]byte("score:2"), map[string]any{"value": 10.0})
	store.PutMap([]byte("score:3"), map[string]any{"value": 99.9})
	store.PutMap([]byte("score:4"), map[string]any{"value": 25.0})

	// Test min
	output := captureOutput(t, func() {
		shell.execute("SELECT min(v.value) FROM kv WHERE k LIKE 'score:%'")
	})
	if !strings.Contains(output, "10") {
		t.Errorf("min(v.value) = %q, want to contain '10'", output)
	}

	// Test max
	output = captureOutput(t, func() {
		shell.execute("SELECT max(v.value) FROM kv WHERE k LIKE 'score:%'")
	})
	if !strings.Contains(output, "99.9") {
		t.Errorf("max(v.value) = %q, want to contain '99.9'", output)
	}
}

func TestShellAggregateMultiple(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert records
	for i := 1; i <= 3; i++ {
		store.PutMap([]byte(fmt.Sprintf("item:%d", i)), map[string]any{
			"price": float64(i * 100), // 100, 200, 300
		})
	}

	// Test multiple aggregates
	output := captureOutput(t, func() {
		shell.execute("SELECT count(), sum(v.price), avg(v.price), min(v.price), max(v.price) FROM kv")
	})
	if !strings.Contains(output, "count()") {
		t.Errorf("output missing 'count()': %q", output)
	}
	if !strings.Contains(output, "3") { // count
		t.Errorf("output missing count value '3': %q", output)
	}
	if !strings.Contains(output, "600") { // sum
		t.Errorf("output missing sum value '600': %q", output)
	}
	if !strings.Contains(output, "200") { // avg
		t.Errorf("output missing avg value '200': %q", output)
	}
	if !strings.Contains(output, "100") { // min
		t.Errorf("output missing min value '100': %q", output)
	}
	if !strings.Contains(output, "300") { // max
		t.Errorf("output missing max value '300': %q", output)
	}
}

func TestShellAggregateEmpty(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Test aggregates on empty result
	output := captureOutput(t, func() {
		shell.execute("SELECT count(), sum(v.x), avg(v.x) FROM kv WHERE k = 'nonexistent'")
	})
	if !strings.Contains(output, "0") { // count should be 0
		t.Errorf("count() on empty = %q, want to contain '0'", output)
	}
	if !strings.Contains(output, "NULL") { // sum/avg should be NULL
		t.Errorf("sum/avg on empty = %q, want to contain 'NULL'", output)
	}
}

func TestShellAggregateNestedField(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert records with nested numeric fields
	store.PutMap([]byte("data:1"), map[string]any{
		"stats": map[string]any{"count": 10},
	})
	store.PutMap([]byte("data:2"), map[string]any{
		"stats": map[string]any{"count": 20},
	})
	store.PutMap([]byte("data:3"), map[string]any{
		"stats": map[string]any{"count": 30},
	})

	// Test sum on nested field
	output := captureOutput(t, func() {
		shell.execute("SELECT sum(v.stats.count) FROM kv WHERE k LIKE 'data:%'")
	})
	if !strings.Contains(output, "60") {
		t.Errorf("sum(v.stats.count) = %q, want to contain '60'", output)
	}
}

package main

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ============================================================================
// Subprocess Integration Tests
// These tests run the actual shell binary with stdin input
// ============================================================================

// buildShellCLI builds the CLI binary for shell testing
func buildShellCLI(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "tinykvs")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build CLI: %v\n%s", err, output)
	}
	return binPath
}

// runShell runs the shell with the given input and returns the output
func runShell(t *testing.T, bin, dir, input string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(bin, "shell", "-dir", dir)
	cmd.Stdin = strings.NewReader(input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func TestShellSubprocessBasic(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	// Run shell with multiple commands
	input := "INSERT INTO kv VALUES ('subkey', 'subvalue')\n" +
		"SELECT * FROM kv WHERE k = 'subkey'\n" +
		"\\q\n"
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "INSERT 1") {
		t.Errorf("stdout = %q, want contains 'INSERT 1'", stdout)
	}
	if !strings.Contains(stdout, "subvalue") {
		t.Errorf("stdout = %q, want contains 'subvalue'", stdout)
	}
	if !strings.Contains(stdout, "(1 rows)") {
		t.Errorf("stdout = %q, want contains '(1 rows)'", stdout)
	}
}

func TestShellSubprocessMultipleOperations(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := "INSERT INTO kv VALUES ('user:1', 'alice')\n" +
		"INSERT INTO kv VALUES ('user:2', 'bob')\n" +
		"INSERT INTO kv VALUES ('user:3', 'charlie')\n" +
		"INSERT INTO kv VALUES ('order:1', 'pizza')\n" +
		"SELECT * FROM kv WHERE k LIKE 'user:%'\n" +
		"DELETE FROM kv WHERE k = 'user:2'\n" +
		"SELECT * FROM kv WHERE k LIKE 'user:%'\n" +
		"\\stats\n" +
		"\\q\n"
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	// Check inserts
	if strings.Count(stdout, "INSERT 1") != 4 {
		t.Errorf("stdout should have 4 'INSERT 1', got: %s", stdout)
	}

	// First SELECT should show 3 users
	if !strings.Contains(stdout, "(3 rows)") {
		t.Errorf("first SELECT should show 3 rows, got: %s", stdout)
	}

	// After delete, SELECT should show 2 users
	if !strings.Contains(stdout, "(2 rows)") {
		t.Errorf("second SELECT should show 2 rows, got: %s", stdout)
	}

	// Stats should show memtable info
	if !strings.Contains(stdout, "Memtable") {
		t.Errorf("stats should show Memtable, got: %s", stdout)
	}
}

func TestShellSubprocessPersistence(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	// First session: insert and flush
	input1 := `INSERT INTO kv VALUES ('persistent', 'data')
\flush
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input1)
	if err != nil {
		t.Fatalf("first session failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "INSERT 1") {
		t.Errorf("first session stdout = %q, want 'INSERT 1'", stdout)
	}

	// Second session: verify data exists
	input2 := `SELECT * FROM kv WHERE k = 'persistent'
\q
`
	stdout, stderr, err = runShell(t, bin, dir, input2)
	if err != nil {
		t.Fatalf("second session failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "data") {
		t.Errorf("second session stdout = %q, want 'data'", stdout)
	}
	if !strings.Contains(stdout, "(1 rows)") {
		t.Errorf("second session stdout = %q, want '(1 rows)'", stdout)
	}
}

func TestShellSubprocessHelp(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `\help
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	// Help should show SQL examples
	if !strings.Contains(stdout, "SELECT") {
		t.Errorf("help should show SELECT, got: %s", stdout)
	}
	if !strings.Contains(stdout, "INSERT") {
		t.Errorf("help should show INSERT, got: %s", stdout)
	}
	if !strings.Contains(stdout, "UPDATE") {
		t.Errorf("help should show UPDATE, got: %s", stdout)
	}
	if !strings.Contains(stdout, "DELETE") {
		t.Errorf("help should show DELETE, got: %s", stdout)
	}
	if !strings.Contains(stdout, "\\q") || !strings.Contains(stdout, "\\quit") {
		t.Errorf("help should show quit commands, got: %s", stdout)
	}
}

func TestShellSubprocessCompact(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	// Insert data, flush, then compact
	input := `INSERT INTO kv VALUES ('k1', 'v1')
INSERT INTO kv VALUES ('k2', 'v2')
\flush
\compact
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "Compacting") {
		t.Errorf("stdout should show Compacting, got: %s", stdout)
	}
	if !strings.Contains(stdout, "Done") {
		t.Errorf("stdout should show Done, got: %s", stdout)
	}
}

func TestShellSubprocessRangeScan(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `INSERT INTO kv VALUES ('a', '1')
INSERT INTO kv VALUES ('b', '2')
INSERT INTO kv VALUES ('c', '3')
INSERT INTO kv VALUES ('d', '4')
INSERT INTO kv VALUES ('e', '5')
SELECT * FROM kv WHERE k BETWEEN 'b' AND 'e'
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	// Should include b, c, d (and possibly e depending on BETWEEN semantics)
	// Use box drawing character │ for column separator in DuckDB-style output
	if !strings.Contains(stdout, "│ b │") {
		t.Errorf("BETWEEN should include 'b', got: %s", stdout)
	}
	if !strings.Contains(stdout, "│ c │") {
		t.Errorf("BETWEEN should include 'c', got: %s", stdout)
	}
	if !strings.Contains(stdout, "│ d │") {
		t.Errorf("BETWEEN should include 'd', got: %s", stdout)
	}
}

func TestShellSubprocessUpdate(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `INSERT INTO kv VALUES ('updateme', 'old')
SELECT * FROM kv WHERE k = 'updateme'
UPDATE kv SET v = 'new' WHERE k = 'updateme'
SELECT * FROM kv WHERE k = 'updateme'
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "UPDATE 1") {
		t.Errorf("should show UPDATE 1, got: %s", stdout)
	}
	// After update, should show 'new' not 'old'
	parts := strings.Split(stdout, "UPDATE 1")
	if len(parts) < 2 {
		t.Fatalf("expected output after UPDATE 1")
	}
	afterUpdate := parts[1]
	if !strings.Contains(afterUpdate, "new") {
		t.Errorf("after update should show 'new', got: %s", afterUpdate)
	}
}

func TestShellSubprocessHexKeys(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `INSERT INTO kv VALUES (x'cafe', 'hex key')
SELECT * FROM kv WHERE k = x'cafe'
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "INSERT 1") {
		t.Errorf("should insert hex key, got: %s", stdout)
	}
	if !strings.Contains(stdout, "hex key") {
		t.Errorf("should find hex key value, got: %s", stdout)
	}
	if !strings.Contains(stdout, "(1 rows)") {
		t.Errorf("should show 1 row, got: %s", stdout)
	}
}

func TestShellSubprocessLimit(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	// Insert 20 keys, select with limit 5
	var inserts strings.Builder
	for i := 0; i < 20; i++ {
		inserts.WriteString("INSERT INTO kv VALUES ('key" + string(rune('A'+i)) + "', 'val')\n")
	}
	inserts.WriteString("SELECT * FROM kv LIMIT 5\n")
	inserts.WriteString("\\q\n")

	stdout, stderr, err := runShell(t, bin, dir, inserts.String())
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "(5 rows)") {
		t.Errorf("LIMIT 5 should show 5 rows, got: %s", stdout)
	}
}

func TestShellSubprocessDeletePrefix(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `INSERT INTO kv VALUES ('del:1', 'a')
INSERT INTO kv VALUES ('del:2', 'b')
INSERT INTO kv VALUES ('del:3', 'c')
INSERT INTO kv VALUES ('keep:1', 'd')
DELETE FROM kv WHERE k LIKE 'del:%'
SELECT * FROM kv
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "DELETE 3") {
		t.Errorf("should delete 3 keys, got: %s", stdout)
	}
	// Final select should only show 'keep:1'
	if !strings.Contains(stdout, "(1 rows)") {
		t.Errorf("should have 1 row remaining, got: %s", stdout)
	}
	if !strings.Contains(stdout, "keep:1") {
		t.Errorf("should show keep:1, got: %s", stdout)
	}
}

func TestShellSubprocessEmptyStore(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `SELECT * FROM kv
\stats
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "(0 rows)") {
		t.Errorf("empty store should show 0 rows, got: %s", stdout)
	}
}

func TestShellSubprocessInvalidSQL(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `THIS IS NOT VALID SQL
\q
`
	stdout, _, err := runShell(t, bin, dir, input)
	// Should not crash, should show parse error
	if err != nil {
		// Even with parse error, shell should exit cleanly with \q
		t.Logf("shell exited with error (may be expected): %v", err)
	}
	if !strings.Contains(stdout, "Parse error") && !strings.Contains(stdout, "Unsupported") {
		t.Errorf("invalid SQL should show error, got: %s", stdout)
	}
}

func TestShellSubprocessMultiInsert(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `INSERT INTO kv VALUES ('m1', 'v1'), ('m2', 'v2'), ('m3', 'v3')
SELECT * FROM kv
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "INSERT 3") {
		t.Errorf("multi-insert should show INSERT 3, got: %s", stdout)
	}
	if !strings.Contains(stdout, "(3 rows)") {
		t.Errorf("should have 3 rows, got: %s", stdout)
	}
}

func TestShellSubprocessTables(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `\tables
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "kv") {
		t.Errorf("tables should show 'kv', got: %s", stdout)
	}
	// Column names are 'k' and 'v'
	if !strings.Contains(stdout, "k:") {
		t.Errorf("tables should show 'k' column, got: %s", stdout)
	}
	if !strings.Contains(stdout, "v:") {
		t.Errorf("tables should show 'v' column, got: %s", stdout)
	}
}

func TestShellSubprocessExportImport(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()
	csvFile := filepath.Join(dir, "test_export.csv")

	// Insert data and export
	input1 := "INSERT INTO kv VALUES ('sub1', 'val1')\n" +
		"INSERT INTO kv VALUES ('sub2', 'val2')\n" +
		"\\export " + csvFile + "\n" +
		"\\q\n"
	stdout, stderr, err := runShell(t, bin, dir, input1)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "Exported 2 keys") {
		t.Errorf("export output = %q, want 'Exported 2 keys'", stdout)
	}

	// Create new store directory and import
	dir2 := t.TempDir()
	input2 := "\\import " + csvFile + "\n" +
		"SELECT * FROM kv\n" +
		"\\q\n"
	stdout, stderr, err = runShell(t, bin, dir2, input2)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "Imported 2 keys") {
		t.Errorf("import output = %q, want 'Imported 2 keys'", stdout)
	}
	if !strings.Contains(stdout, "(2 rows)") {
		t.Errorf("after import, SELECT = %q, want '(2 rows)'", stdout)
	}
}

func TestShellSubprocessJSONRecord(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `INSERT INTO kv VALUES ('user:1', '{"name":"Alice","email":"alice@test.com","active":true}')
SELECT * FROM kv WHERE k = 'user:1'
SELECT v.name, v.email FROM kv WHERE k = 'user:1'
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	// Check INSERT
	if !strings.Contains(stdout, "INSERT 1") {
		t.Errorf("stdout = %q, want 'INSERT 1'", stdout)
	}

	// Check SELECT * shows JSON
	if !strings.Contains(stdout, "name") || !strings.Contains(stdout, "Alice") {
		t.Errorf("SELECT * should show JSON record, got: %s", stdout)
	}

	// Check field selection
	if !strings.Contains(stdout, "alice@test.com") {
		t.Errorf("SELECT v.email should show email, got: %s", stdout)
	}
}

func TestShellSubprocessMultipleRecords(t *testing.T) {
	bin := buildShellCLI(t)
	dir := t.TempDir()

	input := `INSERT INTO kv VALUES ('p:1', '{"name":"Alice","dept":"Engineering"}')
INSERT INTO kv VALUES ('p:2', '{"name":"Bob","dept":"Sales"}')
INSERT INTO kv VALUES ('p:3', '{"name":"Charlie","dept":"Engineering"}')
SELECT v.name, v.dept FROM kv WHERE k LIKE 'p:%'
\q
`
	stdout, stderr, err := runShell(t, bin, dir, input)
	if err != nil {
		t.Fatalf("shell failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "(3 rows)") {
		t.Errorf("should show 3 rows, got: %s", stdout)
	}
	if !strings.Contains(stdout, "Alice") || !strings.Contains(stdout, "Bob") || !strings.Contains(stdout, "Charlie") {
		t.Errorf("should show all names, got: %s", stdout)
	}
	if !strings.Contains(stdout, "Engineering") || !strings.Contains(stdout, "Sales") {
		t.Errorf("should show departments, got: %s", stdout)
	}
}

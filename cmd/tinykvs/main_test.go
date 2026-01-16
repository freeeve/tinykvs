package main

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

// buildCLI builds the CLI binary for testing
func buildCLI(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "tinykvs")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build CLI: %v\n%s", err, output)
	}
	return binPath
}

// runCLI runs a CLI command and returns stdout, stderr, and error
func runCLI(t *testing.T, bin string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func TestCLIPutGet(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Put a string value
	stdout, stderr, err := runCLI(t, bin, "put", "-dir", dir, "-key", "hello", "-value", "world", "-flush")
	if err != nil {
		t.Fatalf("put failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "OK") {
		t.Errorf("put output = %q, want contains 'OK'", stdout)
	}

	// Get it back
	stdout, stderr, err = runCLI(t, bin, "get", "-dir", dir, "-key", "hello")
	if err != nil {
		t.Fatalf("get failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "world") {
		t.Errorf("get output = %q, want contains 'world'", stdout)
	}
}

func TestCLIPutGetHex(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Put with hex key and value
	keyHex := hex.EncodeToString([]byte{0x00, 0x01, 0x02})
	valueHex := hex.EncodeToString([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	stdout, stderr, err := runCLI(t, bin, "put", "-dir", dir, "-key-hex", keyHex, "-value-hex", valueHex, "-flush")
	if err != nil {
		t.Fatalf("put failed: %v\nstderr: %s", err, stderr)
	}

	// Get with hex key
	stdout, stderr, err = runCLI(t, bin, "get", "-dir", dir, "-key-hex", keyHex)
	if err != nil {
		t.Fatalf("get failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "deadbeef") {
		t.Errorf("get output = %q, want contains 'deadbeef'", stdout)
	}
}

func TestCLIPutValueTypes(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	tests := []struct {
		key      string
		flag     string
		value    string
		expected string
	}{
		{"int-key", "-value-int", "42", "(int64) 42"},
		{"float-key", "-value-float", "3.14", "(float64)"},
		{"bool-key", "-value-bool", "true", "(bool) true"},
		{"string-key", "-value", "hello", "(string) \"hello\""},
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			_, stderr, err := runCLI(t, bin, "put", "-dir", dir, "-key", tc.key, tc.flag, tc.value, "-flush")
			if err != nil {
				t.Fatalf("put failed: %v\nstderr: %s", err, stderr)
			}

			stdout, stderr, err := runCLI(t, bin, "get", "-dir", dir, "-key", tc.key)
			if err != nil {
				t.Fatalf("get failed: %v\nstderr: %s", err, stderr)
			}
			if !strings.Contains(stdout, tc.expected) {
				t.Errorf("get output = %q, want contains %q", stdout, tc.expected)
			}
		})
	}
}

func TestCLIDelete(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Put a value
	runCLI(t, bin, "put", "-dir", dir, "-key", "toDelete", "-value", "temp", "-flush")

	// Verify it exists
	stdout, _, err := runCLI(t, bin, "get", "-dir", dir, "-key", "toDelete")
	if err != nil {
		t.Fatalf("get before delete failed: %v", err)
	}
	if !strings.Contains(stdout, "temp") {
		t.Fatalf("key should exist before delete")
	}

	// Delete it
	_, stderr, err := runCLI(t, bin, "delete", "-dir", dir, "-key", "toDelete", "-flush")
	if err != nil {
		t.Fatalf("delete failed: %v\nstderr: %s", err, stderr)
	}

	// Verify it's gone
	stdout, _, _ = runCLI(t, bin, "get", "-dir", dir, "-key", "toDelete")
	if !strings.Contains(stdout, "not found") {
		t.Errorf("get after delete = %q, want 'not found'", stdout)
	}
}

func TestCLIScan(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Put multiple keys
	for i := 0; i < 10; i++ {
		runCLI(t, bin, "put", "-dir", dir, "-key", "user:"+string(rune('0'+i)), "-value", "value", "-flush")
	}
	for i := 0; i < 5; i++ {
		runCLI(t, bin, "put", "-dir", dir, "-key", "post:"+string(rune('0'+i)), "-value", "value", "-flush")
	}

	// Scan with prefix
	_, stderr, err := runCLI(t, bin, "scan", "-dir", dir, "-prefix", "user:")
	if err != nil {
		t.Fatalf("scan failed: %v\nstderr: %s", err, stderr)
	}

	// Should have 10 user keys
	if !strings.Contains(stderr, "10 results") {
		t.Errorf("scan stderr = %q, want contains '10 results'", stderr)
	}

	// Scan with limit
	_, stderr, err = runCLI(t, bin, "scan", "-dir", dir, "-prefix", "user:", "-limit", "3")
	if err != nil {
		t.Fatalf("scan with limit failed: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stderr, "3 results") {
		t.Errorf("scan stderr = %q, want contains '3 results'", stderr)
	}
}

func TestCLIScanKeysOnly(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	runCLI(t, bin, "put", "-dir", dir, "-key", "test:1", "-value", "secret", "-flush")
	runCLI(t, bin, "put", "-dir", dir, "-key", "test:2", "-value", "secret", "-flush")

	// Scan with keys-only
	stdout, _, err := runCLI(t, bin, "scan", "-dir", dir, "-prefix", "test:", "-keys-only")
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	// Should have keys but not values
	if !strings.Contains(stdout, "test:1") {
		t.Errorf("output should contain 'test:1'")
	}
	if strings.Contains(stdout, "secret") {
		t.Errorf("output should not contain 'secret' in keys-only mode")
	}
}

func TestCLIStats(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Create some data
	for i := 0; i < 100; i++ {
		runCLI(t, bin, "put", "-dir", dir, "-key", "key"+string(rune(i)), "-value", "value")
	}
	runCLI(t, bin, "put", "-dir", dir, "-key", "final", "-value", "value", "-flush")

	// Get stats
	stdout, stderr, err := runCLI(t, bin, "stats", "-dir", dir)
	if err != nil {
		t.Fatalf("stats failed: %v\nstderr: %s", err, stderr)
	}

	// Should show memtable and level info
	if !strings.Contains(stdout, "Memtable:") {
		t.Errorf("stats should contain 'Memtable:'")
	}
	if !strings.Contains(stdout, "Levels:") {
		t.Errorf("stats should contain 'Levels:'")
	}
}

func TestCLICompact(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Create multiple L0 tables
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 20; i++ {
			runCLI(t, bin, "put", "-dir", dir, "-key", "key", "-value", "value")
		}
		runCLI(t, bin, "put", "-dir", dir, "-key", "final", "-value", "value", "-flush")
	}

	// Compact
	stdout, stderr, err := runCLI(t, bin, "compact", "-dir", dir)
	if err != nil {
		t.Fatalf("compact failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "Done") {
		t.Errorf("compact output = %q, want contains 'Done'", stdout)
	}
}

func TestCLIExportImport(t *testing.T) {
	bin := buildCLI(t)
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	exportFile := filepath.Join(t.TempDir(), "export.csv")

	// Create data in dir1
	runCLI(t, bin, "put", "-dir", dir1, "-key", "key1", "-value", "value1", "-flush")
	runCLI(t, bin, "put", "-dir", dir1, "-key", "key2", "-value-int", "42", "-flush")
	runCLI(t, bin, "put", "-dir", dir1, "-key", "key3", "-value-bool", "true", "-flush")

	// Export
	_, stderr, err := runCLI(t, bin, "export", "-dir", dir1, "-output", exportFile)
	if err != nil {
		t.Fatalf("export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify CSV was created
	data, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("failed to read export file: %v", err)
	}
	if !strings.Contains(string(data), "key,type,value") {
		t.Errorf("export file should contain header")
	}

	// Import into dir2
	_, stderr, err = runCLI(t, bin, "import", "-dir", dir2, "-input", exportFile)
	if err != nil {
		t.Fatalf("import failed: %v\nstderr: %s", err, stderr)
	}

	// Verify data was imported
	stdout, _, err := runCLI(t, bin, "get", "-dir", dir2, "-key", "key1")
	if err != nil {
		t.Fatalf("get after import failed: %v", err)
	}
	if !strings.Contains(stdout, "value1") {
		t.Errorf("imported value should be 'value1', got %q", stdout)
	}

	stdout, _, err = runCLI(t, bin, "get", "-dir", dir2, "-key", "key2")
	if err != nil {
		t.Fatalf("get key2 after import failed: %v", err)
	}
	if !strings.Contains(stdout, "42") {
		t.Errorf("imported int should be 42, got %q", stdout)
	}
}

func TestCLIEnvVar(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Set env var
	os.Setenv("TINYKVS_STORE", dir)
	defer os.Unsetenv("TINYKVS_STORE")

	// Put without -dir flag
	cmd := exec.Command(bin, "put", "-key", "envtest", "-value", "works", "-flush")
	cmd.Env = append(os.Environ(), "TINYKVS_STORE="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("put with env var failed: %v\n%s", err, output)
	}

	// Get without -dir flag
	cmd = exec.Command(bin, "get", "-key", "envtest")
	cmd.Env = append(os.Environ(), "TINYKVS_STORE="+dir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("get with env var failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "works") {
		t.Errorf("get output = %q, want contains 'works'", output)
	}
}

func TestCLIRepairDryRun(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Create a store with some data
	runCLI(t, bin, "put", "-dir", dir, "-key", "test", "-value", "value", "-flush")

	// Create an orphan file
	orphanPath := filepath.Join(dir, "999999.sst")
	os.WriteFile(orphanPath, []byte("fake sst data"), 0644)

	// Run repair with dry-run
	stdout, stderr, err := runCLI(t, bin, "repair", "-dir", dir, "-dry-run")
	if err != nil {
		t.Fatalf("repair failed: %v\nstderr: %s", err, stderr)
	}

	// Should report orphan but not delete
	if !strings.Contains(stdout, "orphan") {
		t.Errorf("repair output should mention orphan files")
	}

	// Orphan file should still exist
	if _, err := os.Stat(orphanPath); os.IsNotExist(err) {
		t.Errorf("orphan file should still exist in dry-run mode")
	}
}

func TestCLIInfo(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Create data
	runCLI(t, bin, "put", "-dir", dir, "-key", "infotest", "-value", "testvalue", "-flush")

	// Get info
	stdout, stderr, err := runCLI(t, bin, "info", "-dir", dir, "-key", "infotest")
	if err != nil {
		t.Fatalf("info failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "Key:") {
		t.Errorf("info output should contain 'Key:'")
	}
	if !strings.Contains(stdout, "Value type:") {
		t.Errorf("info output should contain 'Value type:'")
	}
}

func TestCLIKeyNotFound(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()

	// Create empty store
	opts := tinykvs.DefaultOptions(dir)
	store, _ := tinykvs.Open(dir, opts)
	store.Close()

	// Get non-existent key
	stdout, _, err := runCLI(t, bin, "get", "-dir", dir, "-key", "nonexistent")
	if err != nil {
		t.Fatalf("get non-existent key should not return error")
	}
	if !strings.Contains(stdout, "not found") {
		t.Errorf("output should say 'not found', got %q", stdout)
	}
}

func TestCLIMissingDir(t *testing.T) {
	bin := buildCLI(t)

	// Try to run without -dir and without env var
	os.Unsetenv("TINYKVS_STORE")
	_, stderr, err := runCLI(t, bin, "get", "-key", "test")
	if err == nil {
		t.Errorf("should fail without -dir")
	}
	if !strings.Contains(stderr, "required") {
		t.Errorf("error should mention -dir is required, got %q", stderr)
	}
}

func TestCLIExportWithPrefix(t *testing.T) {
	bin := buildCLI(t)
	dir := t.TempDir()
	exportFile := filepath.Join(t.TempDir(), "export.csv")

	// Create data with different prefixes
	runCLI(t, bin, "put", "-dir", dir, "-key", "user:1", "-value", "alice", "-flush")
	runCLI(t, bin, "put", "-dir", dir, "-key", "user:2", "-value", "bob", "-flush")
	runCLI(t, bin, "put", "-dir", dir, "-key", "post:1", "-value", "hello", "-flush")

	// Export only user: prefix
	_, stderr, err := runCLI(t, bin, "export", "-dir", dir, "-output", exportFile, "-prefix", "user:")
	if err != nil {
		t.Fatalf("export failed: %v\nstderr: %s", err, stderr)
	}

	// Read and verify
	data, _ := os.ReadFile(exportFile)
	reader := csv.NewReader(bytes.NewReader(data))
	records, _ := reader.ReadAll()

	// Should have header + 2 user records
	if len(records) != 3 {
		t.Errorf("export with prefix should have 3 rows (header + 2 data), got %d", len(records))
	}
}

func TestCLIHelpCommands(t *testing.T) {
	bin := buildCLI(t)

	// Test main help
	stdout, _, err := runCLI(t, bin, "help")
	if err != nil {
		t.Fatalf("help failed: %v", err)
	}
	if !strings.Contains(stdout, "Commands:") {
		t.Errorf("help should list commands")
	}

	// Test -h flag
	stdout, _, _ = runCLI(t, bin, "-h")
	if !strings.Contains(stdout, "Commands:") {
		t.Errorf("-h should show help")
	}

	// Test unknown command
	_, stderr, err := runCLI(t, bin, "unknowncommand")
	if err == nil {
		t.Errorf("unknown command should fail")
	}
	if !strings.Contains(stderr, "Unknown command") {
		t.Errorf("should report unknown command")
	}
}

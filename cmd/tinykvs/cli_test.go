package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

// newTestCLI creates a CLI with captured stdout/stderr for testing.
func newTestCLI() (*CLI, *bytes.Buffer, *bytes.Buffer) {
	var stdout, stderr bytes.Buffer
	cli := &CLI{
		Stdout: &stdout,
		Stderr: &stderr,
		Getenv: func(string) string { return "" },
	}
	return cli, &stdout, &stderr
}

// newTestCLIWithEnv creates a CLI with a custom env function.
func newTestCLIWithEnv(envFunc func(string) string) (*CLI, *bytes.Buffer, *bytes.Buffer) {
	var stdout, stderr bytes.Buffer
	cli := &CLI{
		Stdout: &stdout,
		Stderr: &stderr,
		Getenv: envFunc,
	}
	return cli, &stdout, &stderr
}

// createTestStore creates a store with optional data for testing.
func createTestStore(t *testing.T, data map[string]tinykvs.Value) string {
	t.Helper()
	dir := t.TempDir()
	opts := tinykvs.DefaultOptions(dir)
	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}
	for k, v := range data {
		if err := store.Put([]byte(k), v); err != nil {
			t.Fatalf("failed to put test data: %v", err)
		}
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("failed to flush test store: %v", err)
	}
	store.Close()
	return dir
}

func TestCLI_Run_NoArgs(t *testing.T) {
	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("expected usage in output, got %q", stdout.String())
	}
}

func TestCLI_Run_Help(t *testing.T) {
	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "help"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Commands:") {
		t.Errorf("expected Commands in output, got %q", stdout.String())
	}
}

func TestCLI_Run_Version(t *testing.T) {
	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "version"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "tinykvs") {
		t.Errorf("expected 'tinykvs' in output, got %q", stdout.String())
	}
}

func TestCLI_Run_UnknownCommand(t *testing.T) {
	cli, stdout, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "unknown"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Unknown command") {
		t.Errorf("expected 'Unknown command' in stderr, got %q", stderr.String())
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("expected usage in stdout, got %q", stdout.String())
	}
}

func TestCLI_Get_Success(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"testkey": tinykvs.StringValue("testvalue"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "testkey"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "testvalue") {
		t.Errorf("expected 'testvalue' in output, got %q", stdout.String())
	}
}

func TestCLI_Get_NotFound(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "nonexistent"})

	if code != 0 {
		t.Errorf("expected exit code 0 for not found, got %d", code)
	}
	if !strings.Contains(stdout.String(), "not found") {
		t.Errorf("expected 'not found' in output, got %q", stdout.String())
	}
}

func TestCLI_Get_HexKey(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"\x00\x01\x02": tinykvs.StringValue("hexvalue"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "get", "-dir", dir, "-key-hex", "000102"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "hexvalue") {
		t.Errorf("expected 'hexvalue' in output, got %q", stdout.String())
	}
}

func TestCLI_Get_MissingDir(t *testing.T) {
	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "get", "-key", "test"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "required") {
		t.Errorf("expected 'required' in stderr, got %q", stderr.String())
	}
}

func TestCLI_Get_MissingKey(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "get", "-dir", dir})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "key") {
		t.Errorf("expected 'key' in stderr, got %q", stderr.String())
	}
}

func TestCLI_Get_InvalidHexKey(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "get", "-dir", dir, "-key-hex", "invalid"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "decoding hex") {
		t.Errorf("expected hex decoding error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Put_Success(t *testing.T) {
	dir := t.TempDir()

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "put", "-dir", dir, "-key", "newkey", "-value", "newvalue", "-flush"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected 'OK' in output, got %q", stdout.String())
	}

	// Verify the value was stored
	cli2, stdout2, _ := newTestCLI()
	cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "newkey"})
	if !strings.Contains(stdout2.String(), "newvalue") {
		t.Errorf("stored value not found: %q", stdout2.String())
	}
}

func TestCLI_Put_IntValue(t *testing.T) {
	dir := t.TempDir()

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "put", "-dir", dir, "-key", "intkey", "-value-int", "42", "-flush"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected 'OK' in output, got %q", stdout.String())
	}

	cli2, stdout2, _ := newTestCLI()
	cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "intkey"})
	if !strings.Contains(stdout2.String(), "(int64) 42") {
		t.Errorf("expected '(int64) 42' in output, got %q", stdout2.String())
	}
}

func TestCLI_Put_FloatValue(t *testing.T) {
	dir := t.TempDir()

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "put", "-dir", dir, "-key", "floatkey", "-value-float", "3.14", "-flush"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected 'OK' in output, got %q", stdout.String())
	}

	cli2, stdout2, _ := newTestCLI()
	cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "floatkey"})
	if !strings.Contains(stdout2.String(), "(float64)") {
		t.Errorf("expected '(float64)' in output, got %q", stdout2.String())
	}
}

func TestCLI_Put_BoolValue(t *testing.T) {
	dir := t.TempDir()

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "put", "-dir", dir, "-key", "boolkey", "-value-bool", "true", "-flush"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected 'OK' in output, got %q", stdout.String())
	}

	cli2, stdout2, _ := newTestCLI()
	cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "boolkey"})
	if !strings.Contains(stdout2.String(), "(bool) true") {
		t.Errorf("expected '(bool) true' in output, got %q", stdout2.String())
	}
}

func TestCLI_Put_HexKeyValue(t *testing.T) {
	dir := t.TempDir()

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "put", "-dir", dir, "-key-hex", "000102", "-value-hex", "deadbeef", "-flush"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected 'OK' in output, got %q", stdout.String())
	}

	cli2, stdout2, _ := newTestCLI()
	cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key-hex", "000102"})
	if !strings.Contains(stdout2.String(), "deadbeef") {
		t.Errorf("expected 'deadbeef' in output, got %q", stdout2.String())
	}
}

func TestCLI_Put_MissingValue(t *testing.T) {
	dir := t.TempDir()

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "put", "-dir", dir, "-key", "test"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "value flag is required") {
		t.Errorf("expected value error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Delete_Success(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"deletekey": tinykvs.StringValue("value"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "delete", "-dir", dir, "-key", "deletekey", "-flush"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected 'OK' in output, got %q", stdout.String())
	}

	// Verify the key is deleted
	cli2, stdout2, _ := newTestCLI()
	cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "deletekey"})
	if !strings.Contains(stdout2.String(), "not found") {
		t.Errorf("expected 'not found' after delete, got %q", stdout2.String())
	}
}

func TestCLI_Scan_Success(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"user:1": tinykvs.StringValue("alice"),
		"user:2": tinykvs.StringValue("bob"),
		"post:1": tinykvs.StringValue("hello"),
	})

	cli, stdout, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "scan", "-dir", dir, "-prefix", "user:"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "alice") {
		t.Errorf("expected 'alice' in output, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "bob") {
		t.Errorf("expected 'bob' in output, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "2 results") {
		t.Errorf("expected '2 results' in stderr, got %q", stderr.String())
	}
}

func TestCLI_Scan_KeysOnly(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"test:1": tinykvs.StringValue("secret"),
		"test:2": tinykvs.StringValue("secret"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "scan", "-dir", dir, "-prefix", "test:", "-keys-only"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "test:1") {
		t.Errorf("expected 'test:1' in output, got %q", stdout.String())
	}
	if strings.Contains(stdout.String(), "secret") {
		t.Errorf("should not contain 'secret' in keys-only mode, got %q", stdout.String())
	}
}

func TestCLI_Scan_Limit(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"key:1": tinykvs.StringValue("v1"),
		"key:2": tinykvs.StringValue("v2"),
		"key:3": tinykvs.StringValue("v3"),
		"key:4": tinykvs.StringValue("v4"),
		"key:5": tinykvs.StringValue("v5"),
	})

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "scan", "-dir", dir, "-prefix", "key:", "-limit", "2"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "2 results") {
		t.Errorf("expected '2 results' in stderr, got %q", stderr.String())
	}
}

func TestCLI_Stats_Success(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"test": tinykvs.StringValue("value"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "stats", "-dir", dir})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Memtable:") {
		t.Errorf("expected 'Memtable:' in output, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "Levels:") {
		t.Errorf("expected 'Levels:' in output, got %q", stdout.String())
	}
}

func TestCLI_Compact_Success(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"test": tinykvs.StringValue("value"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "compact", "-dir", dir})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Done") {
		t.Errorf("expected 'Done' in output, got %q", stdout.String())
	}
}

func TestCLI_Info_Success(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"infokey": tinykvs.StringValue("infovalue"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "info", "-dir", dir, "-key", "infokey"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Key:") {
		t.Errorf("expected 'Key:' in output, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "Value type:") {
		t.Errorf("expected 'Value type:' in output, got %q", stdout.String())
	}
}

func TestCLI_Info_NotFound(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "info", "-dir", dir, "-key", "nonexistent"})

	if code != 0 {
		t.Errorf("expected exit code 0 for not found, got %d", code)
	}
	if !strings.Contains(stdout.String(), "not found") {
		t.Errorf("expected 'not found' in output, got %q", stdout.String())
	}
}

func TestCLI_Repair_DryRun(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"test": tinykvs.StringValue("value"),
	})

	// Create an orphan file
	orphanPath := filepath.Join(dir, "999999.sst")
	os.WriteFile(orphanPath, []byte("fake sst data"), 0644)

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "repair", "-dir", dir, "-dry-run"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "orphan") {
		t.Errorf("expected 'orphan' in output, got %q", stdout.String())
	}

	// Verify orphan file still exists
	if _, err := os.Stat(orphanPath); os.IsNotExist(err) {
		t.Error("orphan file should still exist in dry-run mode")
	}
}

func TestCLI_Repair_NoOrphans(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"test": tinykvs.StringValue("value"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "repair", "-dir", dir})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "No orphan files") {
		t.Errorf("expected 'No orphan files' in output, got %q", stdout.String())
	}
}

func TestCLI_Export_Success(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"key1": tinykvs.StringValue("value1"),
		"key2": tinykvs.Int64Value(42),
	})

	outputFile := filepath.Join(t.TempDir(), "export.csv")

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "export", "-dir", dir, "-output", outputFile})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Exported") {
		t.Errorf("expected 'Exported' in stderr, got %q", stderr.String())
	}

	// Verify file was created
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("failed to read export file: %v", err)
	}
	if !strings.Contains(string(data), "key,type,value") {
		t.Errorf("expected CSV header in file, got %q", string(data))
	}
}

func TestCLI_Export_MissingOutput(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "export", "-dir", dir})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "-output is required") {
		t.Errorf("expected '-output is required' in stderr, got %q", stderr.String())
	}
}

func TestCLI_Import_Success(t *testing.T) {
	sourceDir := createTestStore(t, map[string]tinykvs.Value{
		"import1": tinykvs.StringValue("value1"),
		"import2": tinykvs.Int64Value(99),
	})

	// Export from source
	exportFile := filepath.Join(t.TempDir(), "export.csv")
	cli1, _, _ := newTestCLI()
	cli1.Run([]string{"tinykvs", "export", "-dir", sourceDir, "-output", exportFile})

	// Import to new store
	targetDir := t.TempDir()
	cli2, _, stderr := newTestCLI()
	code := cli2.Run([]string{"tinykvs", "import", "-dir", targetDir, "-input", exportFile})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Imported") {
		t.Errorf("expected 'Imported' in stderr, got %q", stderr.String())
	}

	// Verify data was imported
	cli3, stdout3, _ := newTestCLI()
	cli3.Run([]string{"tinykvs", "get", "-dir", targetDir, "-key", "import1"})
	if !strings.Contains(stdout3.String(), "value1") {
		t.Errorf("expected 'value1' in output, got %q", stdout3.String())
	}
}

func TestCLI_Import_MissingInput(t *testing.T) {
	dir := t.TempDir()

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "import", "-dir", dir})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "-input is required") {
		t.Errorf("expected '-input is required' in stderr, got %q", stderr.String())
	}
}

func TestCLI_EnvVar(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"envtest": tinykvs.StringValue("envvalue"),
	})

	// Create CLI that returns the dir for TINYKVS_STORE
	cli, stdout, _ := newTestCLIWithEnv(func(key string) string {
		if key == "TINYKVS_STORE" {
			return dir
		}
		return ""
	})

	// Get without -dir flag
	code := cli.Run([]string{"tinykvs", "get", "-key", "envtest"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "envvalue") {
		t.Errorf("expected 'envvalue' in output, got %q", stdout.String())
	}
}

func TestCLI_ValueTypes(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name     string
		putArgs  []string
		expected string
	}{
		{
			name:     "string value",
			putArgs:  []string{"-key", "str", "-value", "hello world"},
			expected: "(string) \"hello world\"",
		},
		{
			name:     "int64 value",
			putArgs:  []string{"-key", "int", "-value-int", "12345"},
			expected: "(int64) 12345",
		},
		{
			name:     "float64 value",
			putArgs:  []string{"-key", "float", "-value-float", "2.718"},
			expected: "(float64)",
		},
		{
			name:     "bool true",
			putArgs:  []string{"-key", "bool_t", "-value-bool", "true"},
			expected: "(bool) true",
		},
		{
			name:     "bool false",
			putArgs:  []string{"-key", "bool_f", "-value-bool", "false"},
			expected: "(bool) false",
		},
		{
			name:     "bytes value",
			putArgs:  []string{"-key", "bytes", "-value-hex", "cafebabe"},
			expected: "(bytes) cafebabe",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli1, _, _ := newTestCLI()
			args := append([]string{"tinykvs", "put", "-dir", dir}, tc.putArgs...)
			args = append(args, "-flush")
			code := cli1.Run(args)
			if code != 0 {
				t.Fatalf("put failed with code %d", code)
			}

			cli2, stdout, _ := newTestCLI()
			code = cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key", tc.putArgs[1]})
			if code != 0 {
				t.Fatalf("get failed with code %d", code)
			}
			if !strings.Contains(stdout.String(), tc.expected) {
				t.Errorf("expected %q in output, got %q", tc.expected, stdout.String())
			}
		})
	}
}

func TestCLI_GetDir(t *testing.T) {
	tests := []struct {
		name     string
		flagDir  string
		envDir   string
		expected string
	}{
		{
			name:     "flag takes precedence",
			flagDir:  "/flag/dir",
			envDir:   "/env/dir",
			expected: "/flag/dir",
		},
		{
			name:     "env when no flag",
			flagDir:  "",
			envDir:   "/env/dir",
			expected: "/env/dir",
		},
		{
			name:     "empty when neither",
			flagDir:  "",
			envDir:   "",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli := &CLI{
				Getenv: func(key string) string {
					if key == "TINYKVS_STORE" {
						return tc.envDir
					}
					return ""
				},
			}
			result := cli.getDir(tc.flagDir)
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

func TestCLI_RequireDir(t *testing.T) {
	t.Run("returns dir when set", func(t *testing.T) {
		var stderr bytes.Buffer
		cli := &CLI{
			Stderr: &stderr,
			Getenv: func(string) string { return "" },
		}
		dir, ok := cli.requireDir("/some/dir")
		if !ok {
			t.Error("expected ok=true")
		}
		if dir != "/some/dir" {
			t.Errorf("expected '/some/dir', got %q", dir)
		}
	})

	t.Run("returns error when not set", func(t *testing.T) {
		var stderr bytes.Buffer
		cli := &CLI{
			Stderr: &stderr,
			Getenv: func(string) string { return "" },
		}
		_, ok := cli.requireDir("")
		if ok {
			t.Error("expected ok=false")
		}
		if !strings.Contains(stderr.String(), "required") {
			t.Errorf("expected 'required' in error, got %q", stderr.String())
		}
	})
}

func TestNewCLI(t *testing.T) {
	cli := NewCLI()
	if cli.Stdout == nil {
		t.Error("Stdout should not be nil")
	}
	if cli.Stderr == nil {
		t.Error("Stderr should not be nil")
	}
	if cli.Getenv == nil {
		t.Error("Getenv should not be nil")
	}
	// Verify Getenv works
	result := cli.Getenv("PATH")
	if result == "" {
		t.Error("Getenv should return PATH")
	}
}

func TestCLI_Repair_DeleteOrphans(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"test": tinykvs.StringValue("value"),
	})

	// Create orphan files
	orphan1 := filepath.Join(dir, "999998.sst")
	orphan2 := filepath.Join(dir, "999999.sst")
	os.WriteFile(orphan1, []byte("fake sst data 1"), 0644)
	os.WriteFile(orphan2, []byte("fake sst data 2"), 0644)

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "repair", "-dir", dir})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Deleted") {
		t.Errorf("expected 'Deleted' in output, got %q", stdout.String())
	}

	// Verify orphan files were deleted
	if _, err := os.Stat(orphan1); !os.IsNotExist(err) {
		t.Error("orphan1 should have been deleted")
	}
	if _, err := os.Stat(orphan2); !os.IsNotExist(err) {
		t.Error("orphan2 should have been deleted")
	}
}

func TestCLI_Delete_HexKey(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"\x00\x01\x02": tinykvs.StringValue("hexvalue"),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "delete", "-dir", dir, "-key-hex", "000102", "-flush"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "OK") {
		t.Errorf("expected 'OK' in output, got %q", stdout.String())
	}

	// Verify deleted
	cli2, stdout2, _ := newTestCLI()
	cli2.Run([]string{"tinykvs", "get", "-dir", dir, "-key-hex", "000102"})
	if !strings.Contains(stdout2.String(), "not found") {
		t.Errorf("expected 'not found' after delete, got %q", stdout2.String())
	}
}

func TestCLI_Delete_InvalidHexKey(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "delete", "-dir", dir, "-key-hex", "zzzz"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "hex") {
		t.Errorf("expected hex error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Delete_MissingKey(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "delete", "-dir", dir})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "key") {
		t.Errorf("expected key error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Scan_HexPrefix(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"\x01key1": tinykvs.StringValue("v1"),
		"\x01key2": tinykvs.StringValue("v2"),
		"\x02key1": tinykvs.StringValue("v3"),
	})

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "scan", "-dir", dir, "-prefix-hex", "01"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "2 results") {
		t.Errorf("expected '2 results' in stderr, got %q", stderr.String())
	}
}

func TestCLI_Scan_InvalidHexPrefix(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "scan", "-dir", dir, "-prefix-hex", "zzzz"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "hex") {
		t.Errorf("expected hex error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Info_HexKey(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"\x00\x01": tinykvs.Int64Value(42),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "info", "-dir", dir, "-key-hex", "0001"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Key:") {
		t.Errorf("expected 'Key:' in output, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "int64") {
		t.Errorf("expected 'int64' in output, got %q", stdout.String())
	}
}

func TestCLI_Info_InvalidHexKey(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "info", "-dir", dir, "-key-hex", "zzzz"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "hex") {
		t.Errorf("expected hex error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Info_MissingKey(t *testing.T) {
	dir := createTestStore(t, nil)

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "info", "-dir", dir})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "key") {
		t.Errorf("expected key error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Export_HexPrefix(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"\x01key1": tinykvs.StringValue("v1"),
		"\x01key2": tinykvs.StringValue("v2"),
		"\x02key1": tinykvs.StringValue("v3"),
	})

	outputFile := filepath.Join(t.TempDir(), "export.csv")

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "export", "-dir", dir, "-output", outputFile, "-prefix-hex", "01"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "2 keys") {
		t.Errorf("expected '2 keys' in stderr, got %q", stderr.String())
	}
}

func TestCLI_Export_InvalidHexPrefix(t *testing.T) {
	dir := createTestStore(t, nil)
	outputFile := filepath.Join(t.TempDir(), "export.csv")

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "export", "-dir", dir, "-output", outputFile, "-prefix-hex", "zzzz"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "hex") {
		t.Errorf("expected hex error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Import_FileNotFound(t *testing.T) {
	dir := t.TempDir()

	cli, _, stderr := newTestCLI()
	code := cli.Run([]string{"tinykvs", "import", "-dir", dir, "-input", "/nonexistent/file.csv"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Error opening") {
		t.Errorf("expected file error in stderr, got %q", stderr.String())
	}
}

func TestCLI_PrintValue_LargeBytes(t *testing.T) {
	// Test that large byte values are truncated
	largeBytes := make([]byte, 100)
	for i := range largeBytes {
		largeBytes[i] = byte(i)
	}

	dir := createTestStore(t, map[string]tinykvs.Value{
		"largekey": tinykvs.BytesValue(largeBytes),
	})

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "get", "-dir", dir, "-key", "largekey"})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	// Should show truncated output with "..." and byte count
	if !strings.Contains(stdout.String(), "...") {
		t.Errorf("expected '...' for truncated bytes, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "100 bytes") {
		t.Errorf("expected '100 bytes' in output, got %q", stdout.String())
	}
}

func TestCLI_Get_StoreOpenError(t *testing.T) {
	cli, _, stderr := newTestCLI()
	// Use a file as dir to cause open error
	tmpFile := filepath.Join(t.TempDir(), "notadir")
	os.WriteFile(tmpFile, []byte("test"), 0644)

	code := cli.Run([]string{"tinykvs", "get", "-dir", tmpFile, "-key", "test"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Error opening store") {
		t.Errorf("expected store error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Stats_CacheHitRate(t *testing.T) {
	dir := createTestStore(t, map[string]tinykvs.Value{
		"key1": tinykvs.StringValue("value1"),
	})

	// Open store and do some reads to generate cache hits
	opts := tinykvs.DefaultOptions(dir)
	store, _ := tinykvs.Open(dir, opts)
	store.Get([]byte("key1"))
	store.Get([]byte("key1"))
	store.Close()

	cli, stdout, _ := newTestCLI()
	code := cli.Run([]string{"tinykvs", "stats", "-dir", dir})

	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Cache:") {
		t.Errorf("expected 'Cache:' in output, got %q", stdout.String())
	}
}

func TestCLI_Run_VersionFlags(t *testing.T) {
	tests := []string{"-v", "--version", "version"}
	for _, flag := range tests {
		t.Run(flag, func(t *testing.T) {
			cli, stdout, _ := newTestCLI()
			code := cli.Run([]string{"tinykvs", flag})

			if code != 0 {
				t.Errorf("expected exit code 0, got %d", code)
			}
			if !strings.Contains(stdout.String(), "tinykvs") {
				t.Errorf("expected 'tinykvs' in output, got %q", stdout.String())
			}
		})
	}
}

func TestCLI_Run_HelpFlags(t *testing.T) {
	tests := []string{"-h", "--help", "help"}
	for _, flag := range tests {
		t.Run(flag, func(t *testing.T) {
			cli, stdout, _ := newTestCLI()
			code := cli.Run([]string{"tinykvs", flag})

			if code != 0 {
				t.Errorf("expected exit code 0, got %d", code)
			}
			if !strings.Contains(stdout.String(), "Commands:") {
				t.Errorf("expected 'Commands:' in output, got %q", stdout.String())
			}
		})
	}
}

func TestCLI_Compact_StoreError(t *testing.T) {
	cli, _, stderr := newTestCLI()
	tmpFile := filepath.Join(t.TempDir(), "notadir")
	os.WriteFile(tmpFile, []byte("test"), 0644)

	code := cli.Run([]string{"tinykvs", "compact", "-dir", tmpFile})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Error opening store") {
		t.Errorf("expected store error in stderr, got %q", stderr.String())
	}
}

func TestCLI_Put_StoreError(t *testing.T) {
	cli, _, stderr := newTestCLI()
	tmpFile := filepath.Join(t.TempDir(), "notadir")
	os.WriteFile(tmpFile, []byte("test"), 0644)

	code := cli.Run([]string{"tinykvs", "put", "-dir", tmpFile, "-key", "k", "-value", "v"})

	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Error opening store") {
		t.Errorf("expected store error in stderr, got %q", stderr.String())
	}
}

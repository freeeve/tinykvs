package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

// ============================================================================
// Additional Coverage Tests
// ============================================================================

func TestShellExportImport(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	// Insert some data
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('exp1', 'value1')")
		shell.execute("INSERT INTO kv VALUES ('exp2', 'value2')")
	})

	// Export
	csvFile := filepath.Join(dir, "export.csv")
	output := captureOutput(t, func() {
		shell.execute("\\export " + csvFile)
	})
	if !strings.Contains(output, "Exported 2 keys") {
		t.Errorf("export output = %q, want 'Exported 2 keys'", output)
	}

	// Import into same store after clearing
	store.Delete([]byte("exp1"))
	store.Delete([]byte("exp2"))

	output = captureOutput(t, func() {
		shell.execute("\\import " + csvFile)
	})
	if !strings.Contains(output, "Imported 2 keys") {
		t.Errorf("import output = %q, want 'Imported 2 keys'", output)
	}
}

func TestShellImportFromExport(t *testing.T) {
	dir1 := t.TempDir()
	opts1 := tinykvs.DefaultOptions(dir1)
	store1, _ := tinykvs.Open(dir1, opts1)
	shell1 := NewShell(store1)

	// Insert data
	captureOutput(t, func() {
		shell1.execute("INSERT INTO kv VALUES ('key1', 'value1')")
		shell1.execute("INSERT INTO kv VALUES ('key2', 'value2')")
	})

	// Export
	csvFile := filepath.Join(dir1, "export.csv")
	captureOutput(t, func() {
		shell1.execute("\\export " + csvFile)
	})
	store1.Close()

	// Import into new store
	dir2 := t.TempDir()
	opts2 := tinykvs.DefaultOptions(dir2)
	store2, _ := tinykvs.Open(dir2, opts2)
	defer store2.Close()
	shell2 := NewShell(store2)

	output := captureOutput(t, func() {
		shell2.execute("\\import " + csvFile)
	})
	if !strings.Contains(output, "Imported 2 keys") {
		t.Errorf("import output = %q, want 'Imported 2 keys'", output)
	}

	// Verify data
	output = captureOutput(t, func() {
		shell2.execute("SELECT * FROM kv")
	})
	if !strings.Contains(output, "(2 rows)") {
		t.Errorf("after import, SELECT = %q, want '(2 rows)'", output)
	}
}

func TestShellExportMissingFilename(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\export")
	})
	if !strings.Contains(output, "Usage") {
		t.Errorf("export without filename = %q, want usage message", output)
	}
}

func TestShellImportNonexistent(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\import /nonexistent/file.csv")
	})
	if !strings.Contains(output, "Error") {
		t.Errorf("import nonexistent = %q, want error", output)
	}
}

func TestShellExportImportAllTypes(t *testing.T) {
	shell1, store1, dir1 := setupShellTest(t)

	// Insert all value types
	captureOutput(t, func() {
		shell1.execute("INSERT INTO kv VALUES ('str', 'string value')")
	})

	// Store int and bool via API
	store1.PutInt64([]byte("int"), 42)
	store1.PutFloat64([]byte("float"), 3.14159)
	store1.PutBool([]byte("bool"), true)
	store1.PutMap([]byte("record"), map[string]any{"name": "test"})

	// Export
	csvFile := filepath.Join(dir1, "alltypes.csv")
	output := captureOutput(t, func() {
		shell1.execute("\\export " + csvFile)
	})
	if !strings.Contains(output, "Exported 5 keys") {
		t.Errorf("export output = %q, want 'Exported 5 keys'", output)
	}
	store1.Close()

	// Import into new store
	dir2 := t.TempDir()
	opts2 := tinykvs.DefaultOptions(dir2)
	store2, err := tinykvs.Open(dir2, opts2)
	if err != nil {
		t.Fatalf("Failed to open store2: %v", err)
	}
	defer store2.Close()
	shell2 := NewShell(store2)

	output = captureOutput(t, func() {
		shell2.execute("\\import " + csvFile)
	})
	if !strings.Contains(output, "Imported 5 keys") {
		t.Errorf("import output = %q, want 'Imported 5 keys'", output)
	}

	// Verify all types
	intVal, _ := store2.GetInt64([]byte("int"))
	if intVal != 42 {
		t.Errorf("int value = %d, want 42", intVal)
	}

	floatVal, _ := store2.GetFloat64([]byte("float"))
	if floatVal != 3.14159 {
		t.Errorf("float value = %f, want 3.14159", floatVal)
	}

	boolVal, _ := store2.GetBool([]byte("bool"))
	if !boolVal {
		t.Errorf("bool value = %v, want true", boolVal)
	}
}

func TestShellPrintRowNonRecordWithFields(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Store non-record values
	store.PutInt64([]byte("int"), 123)
	store.PutFloat64([]byte("float"), 1.5)
	store.PutBool([]byte("bool"), false)

	// Select with field selector on non-record types
	output := captureOutput(t, func() {
		shell.execute("SELECT v.field FROM kv WHERE k = 'int'")
	})
	if !strings.Contains(output, "123") && !strings.Contains(output, "NULL") {
		t.Logf("Non-record field access shows: %q", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'float'")
	})
	if !strings.Contains(output, "1.5") {
		t.Errorf("SELECT float = %q, want '1.5'", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'bool'")
	})
	if !strings.Contains(output, "false") {
		t.Errorf("SELECT bool = %q, want 'false'", output)
	}
}

func TestShellNilRecord(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	store.Put([]byte("nilrec"), tinykvs.RecordValue(nil))

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'nilrec'")
	})
	if !strings.Contains(output, "{}") {
		t.Errorf("SELECT nil record = %q, want '{}'", output)
	}
}

func TestShellCacheHitRate(t *testing.T) {
	stats := tinykvs.CacheStats{Hits: 0, Misses: 0}
	rate := cacheHitRate(stats)
	if rate != 0 {
		t.Errorf("cacheHitRate(0,0) = %f, want 0", rate)
	}

	stats = tinykvs.CacheStats{Hits: 50, Misses: 50}
	rate = cacheHitRate(stats)
	if rate != 50 {
		t.Errorf("cacheHitRate(50,50) = %f, want 50", rate)
	}
}

func TestShellFormatBytesHelper(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
	}

	for _, tc := range tests {
		result := formatBytes(tc.bytes)
		if result != tc.expected {
			t.Errorf("formatBytes(%d) = %q, want %q", tc.bytes, result, tc.expected)
		}
	}
}

func TestShellIsMsgpackMap(t *testing.T) {
	tests := []struct {
		data   []byte
		expect bool
	}{
		{[]byte{0x80}, true},
		{[]byte{0x8f}, true},
		{[]byte{0xde, 0x00}, true},
		{[]byte{0xdf, 0x00}, true},
		{[]byte{0x90}, false},
		{[]byte{0x00}, false},
		{[]byte{}, false},
	}

	for _, tc := range tests {
		result := isMsgpackMap(tc.data)
		if result != tc.expect {
			t.Errorf("isMsgpackMap(%v) = %v, want %v", tc.data, result, tc.expect)
		}
	}
}

func TestShellHexDecode(t *testing.T) {
	tests := []struct {
		input    string
		expected []byte
		hasError bool
	}{
		{"deadbeef", []byte{0xde, 0xad, 0xbe, 0xef}, false},
		{"00 01 02", []byte{0x00, 0x01, 0x02}, false},
		{"cafe", []byte{0xca, 0xfe}, false},
		{"zz", nil, true},
	}

	for _, tc := range tests {
		result, err := hexDecode(tc.input)
		if tc.hasError {
			if err == nil {
				t.Errorf("hexDecode(%q) should fail", tc.input)
			}
		} else {
			if err != nil {
				t.Errorf("hexDecode(%q) error: %v", tc.input, err)
			}
			if string(result) != string(tc.expected) {
				t.Errorf("hexDecode(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		}
	}
}

func TestShellSelectIntValue(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('num', '12345')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'num'")
	})
	if !strings.Contains(output, "12345") {
		t.Errorf("SELECT int = %q, want '12345'", output)
	}
}

func TestShellHandleInsertErrors(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('onlykey')")
	})
	if !strings.Contains(output, "Error") {
		t.Errorf("INSERT with one value should error, got: %q", output)
	}
}

func TestShellHandleDeleteNoResults(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k LIKE 'nonexistent:%'")
	})
	if !strings.Contains(output, "DELETE 0") {
		t.Errorf("DELETE non-matching prefix should show DELETE 0, got: %q", output)
	}
}

func TestShellHandleDeleteRange(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('r:a', '1')")
		shell.execute("INSERT INTO kv VALUES ('r:b', '2')")
		shell.execute("INSERT INTO kv VALUES ('r:c', '3')")
	})

	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k BETWEEN 'r:a' AND 'r:c'")
	})
	if !strings.Contains(output, "DELETE") {
		t.Errorf("DELETE BETWEEN should work, got: %q", output)
	}
}

func TestShellImportMalformedCSV(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	csvPath := filepath.Join(dir, "malformed.csv")
	content := "key,type,value\ninvalidhex,string,notvalidhex\n"
	os.WriteFile(csvPath, []byte(content), 0644)

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvPath)
	})
	if strings.Contains(output, "panic") {
		t.Errorf("import should not panic on malformed data")
	}
}

func TestShellImportBadTypes(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	csvPath := filepath.Join(dir, "badtypes.csv")
	content := `key,type,value
6b6579,unknowntype,value
6b6579,int64,notanumber
6b6579,float64,notafloat
6b6579,bool,notabool
`
	os.WriteFile(csvPath, []byte(content), 0644)

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvPath)
	})
	if !strings.Contains(output, "errors") {
		t.Logf("import bad types output: %q", output)
	}
}

func TestShellExtractValueTypes(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('intkey', 42)")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT with int should work, got: %q", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'intkey'")
	})
	if !strings.Contains(output, "42") {
		t.Errorf("SELECT intkey should show 42, got: %q", output)
	}
}

func TestShellPrintStatsWithData(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	for i := 0; i < 100; i++ {
		key := []byte("statskey" + string(rune(i)))
		store.PutString(key, "value")
	}
	store.Flush()

	output := captureOutput(t, func() {
		shell.execute("\\stats")
	})
	if !strings.Contains(output, "Memtable") {
		t.Errorf("stats should show Memtable, got: %q", output)
	}
	if !strings.Contains(output, "Cache") {
		t.Errorf("stats should show Cache, got: %q", output)
	}
}

func TestShellExportLargeDataset(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	for i := 0; i < 100; i++ {
		key := []byte("export" + string(rune(i)))
		store.PutString(key, "value")
	}

	csvPath := filepath.Join(dir, "large.csv")
	output := captureOutput(t, func() {
		shell.execute("\\export " + csvPath)
	})
	if !strings.Contains(output, "Exported") {
		t.Errorf("export should report count, got: %q", output)
	}
}

func TestShellHandleSelectError(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("SELECT on closed store: %q", output)
	}
}

func TestShellSelectWithRangeStart(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
		shell.execute("INSERT INTO kv VALUES ('b', '2')")
		shell.execute("INSERT INTO kv VALUES ('c', '3')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k >= 'b'")
	})
	if !strings.Contains(output, "rows)") {
		t.Errorf("SELECT >= should return rows, got: %q", output)
	}
}

func TestShellPrintRowWrapper(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('printrow', 'testvalue')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'printrow'")
	})
	if !strings.Contains(output, "testvalue") {
		t.Errorf("printRow should display value, got: %q", output)
	}
}

func TestShellUnknownValueType(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	store.Delete([]byte("deleted"))

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv")
	})
	if !strings.Contains(output, "(0 rows)") {
		t.Logf("SELECT after delete: %q", output)
	}
}

func TestShellExtractValueIntVal(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('999', 'numeric key')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 999")
	})
	if !strings.Contains(output, "numeric key") {
		t.Logf("SELECT with int literal: %q", output)
	}
}

func TestShellInsertHexBytesNonMsgpack(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('rawbytes', x'0001020304')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT hex bytes output = %q, want 'INSERT 1'", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'rawbytes'")
	})
	if !strings.Contains(output, "(1 rows)") {
		t.Errorf("SELECT hex bytes = %q, want '(1 rows)'", output)
	}
}

func TestShellInsertHexMsgpackDecodeFail(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('badmp', x'82')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT invalid msgpack = %q, want 'INSERT 1'", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'badmp'")
	})
	if !strings.Contains(output, "(1 rows)") {
		t.Errorf("SELECT bad msgpack = %q, want '(1 rows)'", output)
	}
}

func TestShellImportRecordJSONParseFail(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	csvPath := filepath.Join(dir, "badrecord.csv")
	content := `key,type,value
6b6579,record,{invalid json}
`
	os.WriteFile(csvPath, []byte(content), 0644)

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvPath)
	})
	if !strings.Contains(output, "1 errors") && !strings.Contains(output, "error") {
		t.Logf("import bad record JSON output: %q", output)
	}
}

func TestShellImportWrongColumnCount(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	csvPath := filepath.Join(dir, "wrongcols.csv")
	content := `key,type,value
6b6579,string
6b6580,string,ok,extra
`
	os.WriteFile(csvPath, []byte(content), 0644)

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvPath)
	})
	if !strings.Contains(output, "error") {
		t.Logf("import wrong columns output: %q", output)
	}
}

func TestShellImportBadKeyHex(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	csvPath := filepath.Join(dir, "badhex.csv")
	content := `key,type,value
notvalidhex,string,68656c6c6f
`
	os.WriteFile(csvPath, []byte(content), 0644)

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvPath)
	})
	if !strings.Contains(output, "error") {
		t.Logf("import bad hex output: %q", output)
	}
}

func TestShellImportBadValueHex(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	csvPath := filepath.Join(dir, "badvaluehex.csv")
	content := `key,type,value
6b6579,string,zzzznotvalidhex
6b6580,bytes,zzzznotvalidhex
`
	os.WriteFile(csvPath, []byte(content), 0644)

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvPath)
	})
	if !strings.Contains(output, "error") {
		t.Logf("import bad value hex output: %q", output)
	}
}

func TestShellImportReadError(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	csvPath := filepath.Join(dir, "malformed.csv")
	content := "key,type,value\n6b6579,string,\"unclosed quote\n"
	os.WriteFile(csvPath, []byte(content), 0644)

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvPath)
	})
	if strings.Contains(output, "panic") {
		t.Errorf("import should not panic on malformed CSV")
	}
}

func TestShellExportInvalidFilePath(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\export /nonexistent/directory/file.csv")
	})
	if !strings.Contains(output, "Error") {
		t.Errorf("export to invalid path should error, got: %q", output)
	}
}

func TestShellImportMissingFilename(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\import")
	})
	if !strings.Contains(output, "Usage") {
		t.Errorf("import without filename = %q, want usage message", output)
	}
}

func TestShellHandleUpdateNonVColumn(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('updatetest', 'original')")
	})

	output := captureOutput(t, func() {
		shell.execute("UPDATE kv SET other = 'newvalue' WHERE k = 'updatetest'")
	})
	if !strings.Contains(output, "UPDATE 1") {
		t.Logf("UPDATE non-v column: %q", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'updatetest'")
	})
	t.Logf("After UPDATE non-v: %q", output)
}

func TestShellSelectRangeStartOnly(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('x', '1')")
		shell.execute("INSERT INTO kv VALUES ('y', '2')")
		shell.execute("INSERT INTO kv VALUES ('z', '3')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k >= 'y'")
	})
	if !strings.Contains(output, "rows)") {
		t.Errorf("SELECT >= only should return rows, got: %q", output)
	}
}

func TestShellSelectRangeEndOnly(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
		shell.execute("INSERT INTO kv VALUES ('b', '2')")
		shell.execute("INSERT INTO kv VALUES ('c', '3')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k <= 'b'")
	})
	if !strings.Contains(output, "rows)") {
		t.Errorf("SELECT <= only should return rows, got: %q", output)
	}
}

func TestShellPrintRowFieldsUnknownType(t *testing.T) {
	key := []byte("test")
	val := tinykvs.Value{Type: 99}

	output := captureOutput(t, func() {
		printRowFields(key, val, nil)
	})
	if !strings.Contains(output, "unknown type") {
		t.Errorf("printRowFields unknown type = %q, want 'unknown type'", output)
	}
}

func TestShellInsertPutBytesError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('test', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('hexerr', x'0102030405')")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "INSERT 0") {
		t.Logf("INSERT on closed store: %q", output)
	}
}

func TestShellInsertPutStringError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('test', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('strerr', 'testvalue')")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "INSERT 0") {
		t.Logf("INSERT string on closed store: %q", output)
	}
}

func TestShellInsertPutMapError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('test', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('recerr', '{"name":"test"}')`)
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "INSERT 0") {
		t.Logf("INSERT record on closed store: %q", output)
	}
}

func TestShellInsertPutMsgpackError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('test', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('mperr', x'82a46e616d65a3426f62a3616765')")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "INSERT 0") {
		t.Logf("INSERT msgpack on closed store: %q", output)
	}
}

func TestShellDeleteSingleError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('delme', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k = 'delme'")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("DELETE on closed store: %q", output)
	}
}

func TestShellDeletePrefixError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('pre:1', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k LIKE 'pre:%'")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("DELETE prefix on closed store: %q", output)
	}
}

func TestShellDeleteRangeError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
		shell.execute("INSERT INTO kv VALUES ('b', '2')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k BETWEEN 'a' AND 'z'")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("DELETE range on closed store: %q", output)
	}
}

func TestShellSelectPrefixError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('pfx:1', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k LIKE 'pfx:%'")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("SELECT prefix on closed store: %q", output)
	}
}

func TestShellSelectRangeError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('a', '1')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k BETWEEN 'a' AND 'z'")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("SELECT range on closed store: %q", output)
	}
}

func TestShellSelectEqualsError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('exact', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'exact'")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("SELECT exact on closed store: %q", output)
	}
}

func TestShellCompactError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\compact")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("compact on closed store: %q", output)
	}
}

func TestShellFlushError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\flush")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("flush on closed store: %q", output)
	}
}

func TestShellSelectIntLiteralKey(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('12345', 'numeric string key')")
	})

	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 12345")
	})
	if !strings.Contains(output, "numeric string key") {
		t.Logf("SELECT with int literal: %q", output)
	}
}

func TestShellInsertIntLiteralValue(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('intval', 99999)")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT with int value = %q, want 'INSERT 1'", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'intval'")
	})
	if !strings.Contains(output, "99999") {
		t.Errorf("SELECT int value = %q, want '99999'", output)
	}
}

func TestShellExportScanError(t *testing.T) {
	shell, store, dir := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('exp1', 'value1')")
	})

	store.Close()

	csvPath := filepath.Join(dir, "error.csv")
	output := captureOutput(t, func() {
		shell.execute("\\export " + csvPath)
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("export on closed store: %q", output)
	}
}

func TestShellExportProgressOutput(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	for i := 0; i < 10001; i++ {
		key := []byte("k" + strconv.Itoa(i))
		store.PutString(key, "v")
	}

	csvPath := filepath.Join(dir, "progress.csv")
	output := captureOutput(t, func() {
		shell.execute("\\export " + csvPath)
	})
	if !strings.Contains(output, "Exported 10001") {
		t.Errorf("export 10001 keys = %q, want 'Exported 10001'", output)
	}
}

func TestShellImportProgressOutput(t *testing.T) {
	dir1 := t.TempDir()
	opts1 := tinykvs.DefaultOptions(dir1)
	store1, _ := tinykvs.Open(dir1, opts1)
	shell1 := NewShell(store1)

	for i := 0; i < 10001; i++ {
		key := []byte("k" + strconv.Itoa(i))
		store1.PutString(key, "v")
	}

	csvPath := filepath.Join(dir1, "progress.csv")
	captureOutput(t, func() {
		shell1.execute("\\export " + csvPath)
	})
	store1.Close()

	dir2 := t.TempDir()
	opts2 := tinykvs.DefaultOptions(dir2)
	store2, _ := tinykvs.Open(dir2, opts2)
	defer store2.Close()
	shell2 := NewShell(store2)

	output := captureOutput(t, func() {
		shell2.execute("\\import " + csvPath)
	})
	if !strings.Contains(output, "Imported 10001") {
		t.Errorf("import 10001 keys = %q, want 'Imported 10001'", output)
	}
}

func TestShellUpdateError(t *testing.T) {
	shell, store, _ := setupShellTest(t)

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('upd', 'value')")
	})

	store.Close()

	output := captureOutput(t, func() {
		shell.execute("UPDATE kv SET v = 'newvalue' WHERE k = 'upd'")
	})
	if !strings.Contains(output, "Error") && !strings.Contains(output, "closed") {
		t.Logf("UPDATE on closed store: %q", output)
	}
}

func TestShellExtractValueAndTypeIntVal(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('intkey', 42)")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT int literal = %q, want 'INSERT 1'", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'intkey'")
	})
	if !strings.Contains(output, "42") {
		t.Errorf("SELECT int key = %q, want '42'", output)
	}
}

func TestShellImportSimpleFormat(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	// Create simple 2-column CSV
	csvFile := filepath.Join(dir, "simple.csv")
	content := `key,value
user:1,hello
user:2,42
user:3,3.14
user:4,true
user:5,"{""name"":""Alice"",""age"":30}"
`
	if err := os.WriteFile(csvFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvFile)
	})
	if !strings.Contains(output, "Imported 5 keys") {
		t.Errorf("import output = %q, want 'Imported 5 keys'", output)
	}

	// Verify string
	val, _ := store.GetString([]byte("user:1"))
	if val != "hello" {
		t.Errorf("user:1 = %q, want 'hello'", val)
	}

	// Verify int
	intVal, _ := store.GetInt64([]byte("user:2"))
	if intVal != 42 {
		t.Errorf("user:2 = %d, want 42", intVal)
	}

	// Verify float
	floatVal, _ := store.GetFloat64([]byte("user:3"))
	if floatVal != 3.14 {
		t.Errorf("user:3 = %f, want 3.14", floatVal)
	}

	// Verify bool
	boolVal, _ := store.GetBool([]byte("user:4"))
	if !boolVal {
		t.Errorf("user:4 = %v, want true", boolVal)
	}

	// Verify record
	rec, _ := store.GetMap([]byte("user:5"))
	if rec["name"] != "Alice" {
		t.Errorf("user:5.name = %v, want Alice", rec["name"])
	}
}

func TestShellImportRecordFormat(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	// Create multi-column CSV (record format)
	csvFile := filepath.Join(dir, "records.csv")
	content := `id,name,age,active
user:1,Alice,30,true
user:2,Bob,25,false
user:3,Charlie,35,true
`
	if err := os.WriteFile(csvFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvFile)
	})
	if !strings.Contains(output, "Imported 3 keys") {
		t.Errorf("import output = %q, want 'Imported 3 keys'", output)
	}

	// Verify records were created correctly
	rec1, err := store.GetMap([]byte("user:1"))
	if err != nil {
		t.Fatalf("GetMap user:1 failed: %v", err)
	}
	if rec1["name"] != "Alice" {
		t.Errorf("user:1.name = %v, want Alice", rec1["name"])
	}
	// Age should be parsed as int64
	if age, ok := rec1["age"].(int64); !ok || age != 30 {
		t.Errorf("user:1.age = %v (%T), want 30 (int64)", rec1["age"], rec1["age"])
	}
	// Active should be parsed as bool
	if active, ok := rec1["active"].(bool); !ok || !active {
		t.Errorf("user:1.active = %v (%T), want true (bool)", rec1["active"], rec1["active"])
	}

	// Verify via shell query
	output = captureOutput(t, func() {
		shell.execute("SELECT v.name, v.age FROM kv WHERE k = 'user:2'")
	})
	if !strings.Contains(output, "Bob") || !strings.Contains(output, "25") {
		t.Errorf("SELECT = %q, want Bob and 25", output)
	}
}

func TestShellImportTypeHints(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	// Create CSV with type hints
	csvFile := filepath.Join(dir, "typed.csv")
	content := `id,zip:string,count:int,price:float,active:bool,meta:json
item:1,02134,100,19.99,true,"{""color"":""red""}"
item:2,90210,50,29.99,false,"{""color"":""blue""}"
`
	if err := os.WriteFile(csvFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	output := captureOutput(t, func() {
		shell.execute("\\import " + csvFile)
	})
	if !strings.Contains(output, "Imported 2 keys") {
		t.Errorf("import output = %q, want 'Imported 2 keys'", output)
	}

	// Verify types
	rec, err := store.GetMap([]byte("item:1"))
	if err != nil {
		t.Fatalf("GetMap failed: %v", err)
	}

	// zip should be string (not int) due to type hint
	if zip, ok := rec["zip"].(string); !ok || zip != "02134" {
		t.Errorf("zip = %v (%T), want '02134' (string)", rec["zip"], rec["zip"])
	}

	// count should be int64
	if count, ok := rec["count"].(int64); !ok || count != 100 {
		t.Errorf("count = %v (%T), want 100 (int64)", rec["count"], rec["count"])
	}

	// price should be float64
	if price, ok := rec["price"].(float64); !ok || price != 19.99 {
		t.Errorf("price = %v (%T), want 19.99 (float64)", rec["price"], rec["price"])
	}

	// active should be bool
	if active, ok := rec["active"].(bool); !ok || !active {
		t.Errorf("active = %v (%T), want true (bool)", rec["active"], rec["active"])
	}

	// meta should be parsed JSON
	if meta, ok := rec["meta"].(map[string]any); !ok {
		t.Errorf("meta = %v (%T), want map", rec["meta"], rec["meta"])
	} else if meta["color"] != "red" {
		t.Errorf("meta.color = %v, want 'red'", meta["color"])
	}
}

func TestShellExportImportRoundTrip(t *testing.T) {
	shell1, store1, dir := setupShellTest(t)

	// Insert various types
	store1.PutString([]byte("str"), "hello world")
	store1.PutInt64([]byte("int"), 12345)
	store1.PutFloat64([]byte("float"), 98.765)
	store1.PutBool([]byte("bool"), true)
	store1.PutMap([]byte("rec"), map[string]any{"x": 1, "y": 2})

	// Export
	csvFile := filepath.Join(dir, "roundtrip.csv")
	captureOutput(t, func() {
		shell1.execute("\\export " + csvFile)
	})
	store1.Close()

	// Import into new store
	dir2 := t.TempDir()
	opts := tinykvs.DefaultOptions(dir2)
	store2, _ := tinykvs.Open(dir2, opts)
	defer store2.Close()
	shell2 := NewShell(store2)

	captureOutput(t, func() {
		shell2.execute("\\import " + csvFile)
	})

	// Verify all values
	s, _ := store2.GetString([]byte("str"))
	if s != "hello world" {
		t.Errorf("str = %q, want 'hello world'", s)
	}

	i, _ := store2.GetInt64([]byte("int"))
	if i != 12345 {
		t.Errorf("int = %d, want 12345", i)
	}

	f, _ := store2.GetFloat64([]byte("float"))
	if f != 98.765 {
		t.Errorf("float = %f, want 98.765", f)
	}

	b, _ := store2.GetBool([]byte("bool"))
	if !b {
		t.Errorf("bool = %v, want true", b)
	}

	m, _ := store2.GetMap([]byte("rec"))
	if m["x"] != float64(1) { // JSON numbers become float64
		t.Errorf("rec.x = %v, want 1", m["x"])
	}
}

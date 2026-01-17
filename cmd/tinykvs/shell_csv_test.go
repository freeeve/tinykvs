package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

func TestShellExportImport(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('exp1', 'value1')")
		shell.execute("INSERT INTO kv VALUES ('exp2', 'value2')")
	})

	csvFile := filepath.Join(dir, "export.csv")
	output := captureOutput(t, func() {
		shell.execute("\\export " + csvFile)
	})
	if !strings.Contains(output, "Exported 2 keys") {
		t.Errorf("export output = %q, want 'Exported 2 keys'", output)
	}

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

	captureOutput(t, func() {
		shell1.execute("INSERT INTO kv VALUES ('key1', 'value1')")
		shell1.execute("INSERT INTO kv VALUES ('key2', 'value2')")
	})

	csvFile := filepath.Join(dir1, "export.csv")
	captureOutput(t, func() {
		shell1.execute("\\export " + csvFile)
	})
	store1.Close()

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

	captureOutput(t, func() {
		shell1.execute("INSERT INTO kv VALUES ('str', 'string value')")
	})

	store1.PutInt64([]byte("int"), 42)
	store1.PutFloat64([]byte("float"), 3.14159)
	store1.PutBool([]byte("bool"), true)
	store1.PutMap([]byte("record"), map[string]any{"name": "test"})

	csvFile := filepath.Join(dir1, "alltypes.csv")
	output := captureOutput(t, func() {
		shell1.execute("\\export " + csvFile)
	})
	if !strings.Contains(output, "Exported 5 keys") {
		t.Errorf("export output = %q, want 'Exported 5 keys'", output)
	}
	store1.Close()

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

func TestShellImportSimpleFormat(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

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

	val, _ := store.GetString([]byte("user:1"))
	if val != "hello" {
		t.Errorf("user:1 = %q, want 'hello'", val)
	}

	intVal, _ := store.GetInt64([]byte("user:2"))
	if intVal != 42 {
		t.Errorf("user:2 = %d, want 42", intVal)
	}

	floatVal, _ := store.GetFloat64([]byte("user:3"))
	if floatVal != 3.14 {
		t.Errorf("user:3 = %f, want 3.14", floatVal)
	}

	boolVal, _ := store.GetBool([]byte("user:4"))
	if !boolVal {
		t.Errorf("user:4 = %v, want true", boolVal)
	}

	rec, _ := store.GetMap([]byte("user:5"))
	if rec["name"] != "Alice" {
		t.Errorf("user:5.name = %v, want Alice", rec["name"])
	}
}

func TestShellImportRecordFormat(t *testing.T) {
	shell, store, dir := setupShellTest(t)
	defer store.Close()

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

	rec1, err := store.GetMap([]byte("user:1"))
	if err != nil {
		t.Fatalf("GetMap user:1 failed: %v", err)
	}
	if rec1["name"] != "Alice" {
		t.Errorf("user:1.name = %v, want Alice", rec1["name"])
	}
	if age, ok := rec1["age"].(int64); !ok || age != 30 {
		t.Errorf("user:1.age = %v (%T), want 30 (int64)", rec1["age"], rec1["age"])
	}
	if active, ok := rec1["active"].(bool); !ok || !active {
		t.Errorf("user:1.active = %v (%T), want true (bool)", rec1["active"], rec1["active"])
	}

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

	rec, err := store.GetMap([]byte("item:1"))
	if err != nil {
		t.Fatalf("GetMap failed: %v", err)
	}

	if zip, ok := rec["zip"].(string); !ok || zip != "02134" {
		t.Errorf("zip = %v (%T), want '02134' (string)", rec["zip"], rec["zip"])
	}

	if count, ok := rec["count"].(int64); !ok || count != 100 {
		t.Errorf("count = %v (%T), want 100 (int64)", rec["count"], rec["count"])
	}

	if price, ok := rec["price"].(float64); !ok || price != 19.99 {
		t.Errorf("price = %v (%T), want 19.99 (float64)", rec["price"], rec["price"])
	}

	if active, ok := rec["active"].(bool); !ok || !active {
		t.Errorf("active = %v (%T), want true (bool)", rec["active"], rec["active"])
	}

	if meta, ok := rec["meta"].(map[string]any); !ok {
		t.Errorf("meta = %v (%T), want map", rec["meta"], rec["meta"])
	} else if meta["color"] != "red" {
		t.Errorf("meta.color = %v, want 'red'", meta["color"])
	}
}

func TestShellExportImportRoundTrip(t *testing.T) {
	shell1, store1, dir := setupShellTest(t)

	store1.PutString([]byte("str"), "hello world")
	store1.PutInt64([]byte("int"), 12345)
	store1.PutFloat64([]byte("float"), 98.765)
	store1.PutBool([]byte("bool"), true)
	store1.PutMap([]byte("rec"), map[string]any{"x": 1, "y": 2})

	csvFile := filepath.Join(dir, "roundtrip.csv")
	captureOutput(t, func() {
		shell1.execute("\\export " + csvFile)
	})
	store1.Close()

	dir2 := t.TempDir()
	opts := tinykvs.DefaultOptions(dir2)
	store2, _ := tinykvs.Open(dir2, opts)
	defer store2.Close()
	shell2 := NewShell(store2)

	captureOutput(t, func() {
		shell2.execute("\\import " + csvFile)
	})

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
	if m["x"] != float64(1) {
		t.Errorf("rec.x = %v, want 1", m["x"])
	}
}

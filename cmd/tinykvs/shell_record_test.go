package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

// ============================================================================
// Record and JSON Tests
// ============================================================================

func TestShellInsertJSONRecord(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON - should be auto-detected and stored as record
	output := captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('user:1', '{"name":"Alice","age":30}')`)
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT JSON output = %q, want 'INSERT 1'", output)
	}

	// Select should show JSON
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'user:1'")
	})
	if !strings.Contains(output, "name") || !strings.Contains(output, "Alice") {
		t.Errorf("SELECT JSON record output = %q, want JSON with 'name' and 'Alice'", output)
	}
}

func TestShellSelectRecordFields(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON record
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('user:1', '{"name":"Alice","age":30,"email":"alice@example.com"}')`)
	})

	// Select specific fields with v.fieldname syntax
	output := captureOutput(t, func() {
		shell.execute("SELECT v.name, v.age FROM kv WHERE k = 'user:1'")
	})
	if !strings.Contains(output, "Alice") {
		t.Errorf("SELECT v.name output = %q, want 'Alice'", output)
	}
	if !strings.Contains(output, "30") {
		t.Errorf("SELECT v.age output = %q, want '30'", output)
	}
	// Should NOT contain email since we only selected name and age
	if strings.Contains(output, "email") || strings.Contains(output, "alice@example.com") {
		t.Errorf("SELECT v.name, v.age should not show email, got: %q", output)
	}
}

func TestShellSelectSingleRecordField(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON record
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('product:1', '{"name":"Widget","price":99.99,"inStock":true}')`)
	})

	// Select just one field
	output := captureOutput(t, func() {
		shell.execute("SELECT v.name FROM kv WHERE k = 'product:1'")
	})
	if !strings.Contains(output, "Widget") {
		t.Errorf("SELECT v.name output = %q, want 'Widget'", output)
	}
}

func TestShellSelectNonexistentField(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON record
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('item:1', '{"a":"1","b":"2"}')`)
	})

	// Select field that doesn't exist
	output := captureOutput(t, func() {
		shell.execute("SELECT v.nonexistent FROM kv WHERE k = 'item:1'")
	})
	if !strings.Contains(output, "NULL") {
		t.Errorf("SELECT nonexistent field output = %q, want 'NULL'", output)
	}
}

func TestShellSelectMixedFieldsExistAndNot(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON record
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('mix:1', '{"real":"value"}')`)
	})

	// Select mix of existing and non-existing fields
	output := captureOutput(t, func() {
		shell.execute("SELECT v.real, v.fake FROM kv WHERE k = 'mix:1'")
	})
	if !strings.Contains(output, "value") {
		t.Errorf("SELECT v.real output = %q, want 'value'", output)
	}
	if !strings.Contains(output, "NULL") {
		t.Errorf("SELECT v.fake output = %q, want 'NULL'", output)
	}
}

func TestShellSelectFieldsFromMultipleRecords(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert multiple JSON records
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('person:1', '{"name":"Alice","city":"NYC"}')`)
		shell.execute(`INSERT INTO kv VALUES ('person:2', '{"name":"Bob","city":"LA"}')`)
		shell.execute(`INSERT INTO kv VALUES ('person:3', '{"name":"Charlie","city":"Chicago"}')`)
	})

	// Select fields with LIKE
	output := captureOutput(t, func() {
		shell.execute("SELECT v.name, v.city FROM kv WHERE k LIKE 'person:%'")
	})
	if !strings.Contains(output, "Alice") || !strings.Contains(output, "Bob") || !strings.Contains(output, "Charlie") {
		t.Errorf("SELECT fields from multiple records = %q, want all names", output)
	}
	if !strings.Contains(output, "NYC") || !strings.Contains(output, "LA") || !strings.Contains(output, "Chicago") {
		t.Errorf("SELECT fields from multiple records = %q, want all cities", output)
	}
	if !strings.Contains(output, "(3 rows)") {
		t.Errorf("SELECT fields output = %q, want '(3 rows)'", output)
	}
}

func TestShellSelectFieldsWithLimit(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert multiple JSON records
	captureOutput(t, func() {
		for i := 0; i < 10; i++ {
			shell.execute(`INSERT INTO kv VALUES ('item:` + string(rune('0'+i)) + `', '{"num":` + string(rune('0'+i)) + `}')`)
		}
	})

	// Select with limit
	output := captureOutput(t, func() {
		shell.execute("SELECT v.num FROM kv WHERE k LIKE 'item:%' LIMIT 3")
	})
	if !strings.Contains(output, "(3 rows)") {
		t.Errorf("SELECT with LIMIT output = %q, want '(3 rows)'", output)
	}
}

func TestShellSelectStarShowsFullJSON(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON record
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('full:1', '{"a":"1","b":"2","c":"3"}')`)
	})

	// SELECT * should show full JSON
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'full:1'")
	})
	// Should contain all fields in JSON format
	if !strings.Contains(output, `"a"`) || !strings.Contains(output, `"b"`) || !strings.Contains(output, `"c"`) {
		t.Errorf("SELECT * JSON output = %q, want all fields", output)
	}
}

func TestShellNestedJSONRecord(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert nested JSON
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('nested:1', '{"user":{"name":"Alice","address":{"city":"NYC"}},"active":true}')`)
	})

	// SELECT * should show nested structure
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'nested:1'")
	})
	if !strings.Contains(output, "user") || !strings.Contains(output, "address") {
		t.Errorf("SELECT nested JSON = %q, want nested fields", output)
	}
}

func TestShellJSONWithArrays(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON with array
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('arr:1', '{"tags":["a","b","c"],"count":3}')`)
	})

	// SELECT * should show array
	output := captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'arr:1'")
	})
	if !strings.Contains(output, "tags") {
		t.Errorf("SELECT JSON with array = %q, want 'tags'", output)
	}
}

func TestShellInsertMsgpackHex(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert msgpack hex (fixmap with "name"="Bob", "age"=20)
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('mp:1', x'82a46e616d65a3426f62a3616765')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT msgpack output = %q, want 'INSERT 1'", output)
	}

	// Select should show as JSON (records are displayed as JSON)
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'mp:1'")
	})
	if !strings.Contains(output, "name") {
		t.Errorf("SELECT msgpack record = %q, want 'name' field", output)
	}
}

func TestShellSelectFieldFromMsgpackRecord(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert msgpack hex record: {"name":"Bob","age":25}
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('mp:2', x'82a46e616d65a3426f62a361676519')")
	})

	// Select specific field
	output := captureOutput(t, func() {
		shell.execute("SELECT v.name FROM kv WHERE k = 'mp:2'")
	})
	if !strings.Contains(output, "Bob") {
		t.Errorf("SELECT v.name from msgpack = %q, want 'Bob'", output)
	}
}

func TestShellNonRecordValueFieldAccess(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert plain string (not a record)
	captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('str:1', 'plain string')")
	})

	// Select v.field on non-record should show NULL or empty
	output := captureOutput(t, func() {
		shell.execute("SELECT v.name FROM kv WHERE k = 'str:1'")
	})
	// For non-records, field access should show NULL
	if !strings.Contains(output, "NULL") && !strings.Contains(output, "plain string") {
		t.Logf("Non-record field access shows: %q", output)
	}
}

func TestShellExportImportRecords(t *testing.T) {
	shell1, store1, dir1 := setupShellTest(t)

	// Insert JSON records
	captureOutput(t, func() {
		shell1.execute(`INSERT INTO kv VALUES ('rec:1', '{"name":"Alice","age":30}')`)
		shell1.execute(`INSERT INTO kv VALUES ('rec:2', '{"name":"Bob","age":25}')`)
	})

	// Export
	csvFile := filepath.Join(dir1, "records.csv")
	output := captureOutput(t, func() {
		shell1.execute("\\export " + csvFile)
	})
	if !strings.Contains(output, "Exported 2 keys") {
		t.Errorf("export records output = %q, want 'Exported 2 keys'", output)
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
	if !strings.Contains(output, "Imported 2 keys") {
		t.Errorf("import records output = %q, want 'Imported 2 keys'", output)
	}

	// Verify records
	output = captureOutput(t, func() {
		shell2.execute("SELECT v.name FROM kv WHERE k = 'rec:1'")
	})
	if !strings.Contains(output, "Alice") {
		t.Errorf("after import, SELECT v.name = %q, want 'Alice'", output)
	}
}

func TestShellInvalidJSON(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert invalid JSON - should be stored as string, not record
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('bad:1', '{invalid json}')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT invalid JSON output = %q, want 'INSERT 1'", output)
	}

	// Select should show the string as-is
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'bad:1'")
	})
	if !strings.Contains(output, "{invalid json}") {
		t.Errorf("SELECT invalid JSON = %q, want original string", output)
	}
}

func TestShellEmptyJSONObject(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert empty JSON object
	output := captureOutput(t, func() {
		shell.execute("INSERT INTO kv VALUES ('empty:1', '{}')")
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT empty JSON output = %q, want 'INSERT 1'", output)
	}

	// Select should show empty object
	output = captureOutput(t, func() {
		shell.execute("SELECT * FROM kv WHERE k = 'empty:1'")
	})
	if !strings.Contains(output, "{}") {
		t.Errorf("SELECT empty JSON = %q, want '{}'", output)
	}
}

func TestShellJSONTypes(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON with various types
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('types:1', '{"str":"hello","num":42,"float":3.14,"bool":true,"null":null}')`)
	})

	// Select specific fields to avoid truncation issues with table display
	output := captureOutput(t, func() {
		shell.execute("SELECT v.str, v.num, v.float, v.bool FROM kv WHERE k = 'types:1'")
	})
	if !strings.Contains(output, "hello") {
		t.Errorf("SELECT v.str = %q, want 'hello'", output)
	}
	if !strings.Contains(output, "42") {
		t.Errorf("SELECT v.num = %q, want '42'", output)
	}
	if !strings.Contains(output, "3.14") {
		t.Errorf("SELECT v.float = %q, want '3.14'", output)
	}
	if !strings.Contains(output, "true") {
		t.Errorf("SELECT v.bool = %q, want 'true'", output)
	}
}

func TestShellUpdateRecord(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON record
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('upd:1', '{"name":"Alice","score":100}')`)
	})

	// Update with new JSON
	output := captureOutput(t, func() {
		shell.execute(`UPDATE kv SET v = '{"name":"Alice","score":200}' WHERE k = 'upd:1'`)
	})
	if !strings.Contains(output, "UPDATE 1") {
		t.Errorf("UPDATE JSON output = %q, want 'UPDATE 1'", output)
	}

	// Verify update
	output = captureOutput(t, func() {
		shell.execute("SELECT v.score FROM kv WHERE k = 'upd:1'")
	})
	if !strings.Contains(output, "200") {
		t.Errorf("SELECT after UPDATE = %q, want '200'", output)
	}
}

func TestShellDeleteRecords(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert JSON records
	captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('del:1', '{"name":"Alice"}')`)
		shell.execute(`INSERT INTO kv VALUES ('del:2', '{"name":"Bob"}')`)
		shell.execute(`INSERT INTO kv VALUES ('del:3', '{"name":"Charlie"}')`)
	})

	// Delete one
	output := captureOutput(t, func() {
		shell.execute("DELETE FROM kv WHERE k = 'del:2'")
	})
	if !strings.Contains(output, "DELETE 1") {
		t.Errorf("DELETE record output = %q, want 'DELETE 1'", output)
	}

	// Verify remaining
	output = captureOutput(t, func() {
		shell.execute("SELECT v.name FROM kv WHERE k LIKE 'del:%'")
	})
	if !strings.Contains(output, "(2 rows)") {
		t.Errorf("after delete, should have 2 rows, got: %q", output)
	}
	if strings.Contains(output, "Bob") {
		t.Errorf("Bob should be deleted, got: %q", output)
	}
}

func TestShellHelpShowsRecordSyntax(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	output := captureOutput(t, func() {
		shell.execute("\\help")
	})

	// Help should mention record field access
	if !strings.Contains(output, "v.") {
		t.Errorf("help should mention field access syntax, got: %q", output)
	}
	if !strings.Contains(output, "JSON") || !strings.Contains(output, "record") {
		t.Errorf("help should mention JSON/records, got: %q", output)
	}
}

func TestShellRecordPersistence(t *testing.T) {
	dir := t.TempDir()

	// First session: insert record and flush
	{
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Fatalf("Failed to open store: %v", err)
		}
		shell := NewShell(store)

		captureOutput(t, func() {
			shell.execute(`INSERT INTO kv VALUES ('persist:1', '{"data":"important"}')`)
		})
		captureOutput(t, func() {
			shell.execute("\\flush")
		})
		store.Close()
	}

	// Second session: verify record persisted
	{
		opts := tinykvs.DefaultOptions(dir)
		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			t.Fatalf("Failed to reopen store: %v", err)
		}
		defer store.Close()
		shell := NewShell(store)

		output := captureOutput(t, func() {
			shell.execute("SELECT v.data FROM kv WHERE k = 'persist:1'")
		})
		if !strings.Contains(output, "important") {
			t.Errorf("After reopen, SELECT v.data = %q, want 'important'", output)
		}
	}
}

func TestExtractNestedField(t *testing.T) {
	record := map[string]any{
		"name": "Alice",
		"address": map[string]any{
			"city":    "NYC",
			"country": "USA",
			"geo": map[string]any{
				"lat": 40.7128,
				"lng": -74.0060,
			},
		},
		"tags": []string{"admin", "vip"},
	}

	tests := []struct {
		path   string
		want   any
		wantOK bool
	}{
		{"name", "Alice", true},
		{"address.city", "NYC", true},
		{"address.country", "USA", true},
		{"address.geo.lat", 40.7128, true},
		{"address.geo.lng", -74.0060, true},
		{"missing", nil, false},
		{"address.missing", nil, false},
		{"address.geo.missing", nil, false},
		{"name.invalid", nil, false}, // name is not a map
	}

	for _, tc := range tests {
		got, ok := extractNestedField(record, tc.path)
		if ok != tc.wantOK {
			t.Errorf("extractNestedField(%q) ok = %v, want %v", tc.path, ok, tc.wantOK)
		}
		if ok && got != tc.want {
			t.Errorf("extractNestedField(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestShellSelectNestedFields(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert nested JSON record
	output := captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('user:1', '{"name":"Alice","address":{"city":"NYC","country":"USA"}}')`)
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT output = %q, want 'INSERT 1'", output)
	}

	// Select nested field using natural syntax (v.address.city)
	output = captureOutput(t, func() {
		shell.execute("SELECT v.address.city FROM kv WHERE k = 'user:1'")
	})
	if !strings.Contains(output, "NYC") {
		t.Errorf("SELECT nested field = %q, want to contain 'NYC'", output)
	}

	// Select multiple fields with nested
	output = captureOutput(t, func() {
		shell.execute("SELECT v.name, v.address.country FROM kv WHERE k = 'user:1'")
	})
	if !strings.Contains(output, "Alice") || !strings.Contains(output, "USA") {
		t.Errorf("SELECT multiple fields = %q, want Alice and USA", output)
	}

	// Select missing nested field should show NULL
	output = captureOutput(t, func() {
		shell.execute("SELECT v.address.zip FROM kv WHERE k = 'user:1'")
	})
	if !strings.Contains(output, "NULL") {
		t.Errorf("SELECT missing nested field = %q, want NULL", output)
	}

	// Backtick syntax should also work
	output = captureOutput(t, func() {
		shell.execute("SELECT v.`address.city` FROM kv WHERE k = 'user:1'")
	})
	if !strings.Contains(output, "NYC") {
		t.Errorf("SELECT backtick syntax = %q, want to contain 'NYC'", output)
	}
}

func TestShellSelectDeeplyNestedFields(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	// Insert deeply nested JSON record
	output := captureOutput(t, func() {
		shell.execute(`INSERT INTO kv VALUES ('config:1', '{"app":{"db":{"host":"localhost","port":5432}}}')`)
	})
	if !strings.Contains(output, "INSERT 1") {
		t.Errorf("INSERT output = %q, want 'INSERT 1'", output)
	}

	// 2-level nesting works without backticks
	output = captureOutput(t, func() {
		shell.execute("SELECT v.app.db FROM kv WHERE k = 'config:1'")
	})
	if !strings.Contains(output, "localhost") {
		t.Errorf("SELECT 2-level = %q, want to contain 'localhost'", output)
	}

	// 3-level nesting requires backticks
	output = captureOutput(t, func() {
		shell.execute("SELECT v.`app.db.host` FROM kv WHERE k = 'config:1'")
	})
	if !strings.Contains(output, "localhost") {
		t.Errorf("SELECT 3-level = %q, want to contain 'localhost'", output)
	}

	output = captureOutput(t, func() {
		shell.execute("SELECT v.`app.db.port` FROM kv WHERE k = 'config:1'")
	})
	if !strings.Contains(output, "5432") {
		t.Errorf("SELECT 3-level port = %q, want to contain '5432'", output)
	}
}

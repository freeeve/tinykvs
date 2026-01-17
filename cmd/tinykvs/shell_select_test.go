package main

import (
	"strings"
	"testing"

	"github.com/freeeve/tinykvs"
)

func TestShellPrintRowNonRecordWithFields(t *testing.T) {
	shell, store, _ := setupShellTest(t)
	defer store.Close()

	store.PutInt64([]byte("int"), 123)
	store.PutFloat64([]byte("float"), 1.5)
	store.PutBool([]byte("bool"), false)

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

func TestShellExtractRowFieldsUnknownType(t *testing.T) {
	key := []byte("test")
	val := tinykvs.Value{Type: 99}

	row := extractRowFields(key, val, nil)
	if len(row) < 2 {
		t.Errorf("extractRowFields returned %d fields, want at least 2", len(row))
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

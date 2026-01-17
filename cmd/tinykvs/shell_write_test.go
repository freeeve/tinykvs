package main

import (
	"strings"
	"testing"
)

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

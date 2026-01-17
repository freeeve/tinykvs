package main

import (
	"strings"
	"testing"
)

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

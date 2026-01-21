package main

import (
	"os"
	"testing"

	"github.com/freeeve/tinykvs"
)

func TestParseCLIKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		keyHex  string
		want    []byte
		wantErr bool
	}{
		{"string key", "hello", "", []byte("hello"), false},
		{"hex key", "", "48656c6c6f", []byte("Hello"), false},
		{"hex takes precedence", "ignored", "48656c6c6f", []byte("Hello"), false},
		{"invalid hex", "", "zzzz", nil, true},
		{"empty both", "", "", nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCLIKey(tc.key, tc.keyHex)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseCLIKey() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !tc.wantErr && string(got) != string(tc.want) {
				t.Errorf("parseCLIKey() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseBoolValue(t *testing.T) {
	tests := []struct {
		input   string
		want    bool
		wantErr bool
	}{
		{"true", true, false},
		{"True", true, false},
		{"TRUE", true, false},
		{"1", true, false},
		{"yes", true, false},
		{"false", false, false},
		{"False", false, false},
		{"FALSE", false, false},
		{"0", false, false},
		{"no", false, false},
		{"invalid", false, true},
		{"maybe", false, true},
		{"", false, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseBoolValue(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseBoolValue(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
				return
			}
			if !tc.wantErr && got.Bool != tc.want {
				t.Errorf("parseBoolValue(%q) = %v, want %v", tc.input, got.Bool, tc.want)
			}
		})
	}
}

func TestContainsFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
		flag string
		want bool
	}{
		{"exact match", []string{"-value-int", "42"}, "-value-int", true},
		{"equals form", []string{"-value-int=42"}, "-value-int", true},
		{"not present", []string{"-value", "hello"}, "-value-int", false},
		{"empty args", []string{}, "-value-int", false},
		{"partial match not counted", []string{"-value-integer"}, "-value-int", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := containsFlag(tc.args, tc.flag)
			if got != tc.want {
				t.Errorf("containsFlag(%v, %q) = %v, want %v", tc.args, tc.flag, got, tc.want)
			}
		})
	}
}

func TestValueTypeName(t *testing.T) {
	tests := []struct {
		vtype tinykvs.ValueType
		want  string
	}{
		{tinykvs.ValueTypeInt64, "int64"},
		{tinykvs.ValueTypeFloat64, "float64"},
		{tinykvs.ValueTypeBool, "bool"},
		{tinykvs.ValueTypeString, "string"},
		{tinykvs.ValueTypeBytes, "bytes"},
		{tinykvs.ValueTypeTombstone, "tombstone"},
		{tinykvs.ValueType(99), "unknown(99)"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := valueTypeName(tc.vtype)
			if got != tc.want {
				t.Errorf("valueTypeName(%d) = %q, want %q", tc.vtype, got, tc.want)
			}
		})
	}
}

func TestParseUint32(t *testing.T) {
	tests := []struct {
		input   string
		want    uint32
		wantErr bool
	}{
		{"0", 0, false},
		{"1", 1, false},
		{"123456", 123456, false},
		{"4294967295", 4294967295, false},
		{"abc", 0, true},
		{"", 0, true},
		{"-1", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseUint32(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseUint32(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
				return
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("parseUint32(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestFormatKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		want string
	}{
		{"printable ascii", []byte("hello"), "hello"},
		{"binary data", []byte{0x00, 0x01, 0x02}, "0x000102"},
		{"mixed", []byte("hi\x00"), "0x686900"},
		{"empty", []byte{}, "0x"},
		{"control chars", []byte{0x1f}, "0x1f"},
		{"high ascii", []byte{0x80}, "0x80"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatKey(tc.key)
			if got != tc.want {
				t.Errorf("formatKey(%v) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := formatBytes(tc.bytes)
			if got != tc.want {
				t.Errorf("formatBytes(%d) = %q, want %q", tc.bytes, got, tc.want)
			}
		})
	}
}

func TestCheckOrphanFile(t *testing.T) {
	validIDs := map[uint32]bool{1: true, 2: true}

	tests := []struct {
		name     string
		filename string
		isOrphan bool
	}{
		{"valid sst", "000001.sst", false},
		{"orphan sst", "000999.sst", true},
		{"not sst", "manifest", false},
		{"invalid name", "abc.sst", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock DirEntry
			dir := t.TempDir()
			path := dir + "/" + tc.filename
			os.WriteFile(path, []byte("test"), 0644)

			entries, _ := os.ReadDir(dir)
			if len(entries) != 1 {
				t.Fatal("expected 1 entry")
			}

			_, _, isOrphan := checkOrphanFile(entries[0], validIDs)
			if isOrphan != tc.isOrphan {
				t.Errorf("checkOrphanFile(%q) isOrphan = %v, want %v", tc.filename, isOrphan, tc.isOrphan)
			}
		})
	}
}

func TestPutFlags_ParseValue(t *testing.T) {
	tests := []struct {
		name    string
		pf      putFlags
		wantErr bool
	}{
		{
			name:    "no value flags",
			pf:      putFlags{},
			wantErr: true,
		},
		{
			name:    "string value",
			pf:      putFlags{value: "hello"},
			wantErr: false,
		},
		{
			name:    "int value with flag",
			pf:      putFlags{valueInt: 0, args: []string{"-value-int", "0"}},
			wantErr: false,
		},
		{
			name:    "float value with flag",
			pf:      putFlags{valueFloat: 0, args: []string{"-value-float", "0"}},
			wantErr: false,
		},
		{
			name:    "invalid hex value",
			pf:      putFlags{valueHex: "zzzz"},
			wantErr: true,
		},
		{
			name:    "invalid bool value",
			pf:      putFlags{valueBool: "maybe"},
			wantErr: true,
		},
		{
			name:    "multiple values",
			pf:      putFlags{value: "hello", valueInt: 42},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.pf.parseValue()
			if (err != nil) != tc.wantErr {
				t.Errorf("parseValue() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestImportSingleRecord(t *testing.T) {
	dir := t.TempDir()
	opts := tinykvs.DefaultOptions(dir)
	store, _ := tinykvs.Open(dir, opts)
	defer store.Close()

	tests := []struct {
		name   string
		record []string
		want   bool
	}{
		{"valid record", []string{"6b6579", "string", "76616c7565"}, true},
		{"wrong column count", []string{"key", "value"}, false},
		{"invalid hex key", []string{"zzzz", "string", "value"}, false},
		{"invalid type", []string{"6b6579", "invalid", "value"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := importSingleRecord(store, tc.record)
			if got != tc.want {
				t.Errorf("importSingleRecord() = %v, want %v", got, tc.want)
			}
		})
	}
}

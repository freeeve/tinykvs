package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/freeeve/tinykvs"
)

// Common error and help message constants
const (
	msgErrOpenStore    = "Error opening store: %v\n"
	msgErrDecodeHexKey = "Error decoding hex key: %v\n"
	msgErrKeyRequired  = "Error: -key or -key-hex is required"
	msgErr             = "Error: %v\n"
	flagKeyHex         = "key-hex"
)

// putFlags holds parsed flags for the put command.
type putFlags struct {
	key, keyHex                string
	value, valueHex, valueBool string
	valueInt                   int64
	valueFloat                 float64
	flush                      bool
	args                       []string
}

// parseCLIKey parses key from either string or hex flag.
func parseCLIKey(key, keyHex string) ([]byte, error) {
	if keyHex != "" {
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			return nil, fmt.Errorf("error decoding hex key: %v", err)
		}
		return keyBytes, nil
	}
	if key != "" {
		return []byte(key), nil
	}
	return nil, fmt.Errorf(msgErrKeyRequired)
}

// parseValue parses the value from put flags.
func (pf *putFlags) parseValue() (tinykvs.Value, error) {
	var zero tinykvs.Value
	var results []tinykvs.Value

	if v, ok := pf.tryParseString(); ok {
		results = append(results, v)
	}
	if v, ok, err := pf.tryParseHex(); err != nil {
		return zero, err
	} else if ok {
		results = append(results, v)
	}
	if v, ok := pf.tryParseInt(); ok {
		results = append(results, v)
	}
	if v, ok := pf.tryParseFloat(); ok {
		results = append(results, v)
	}
	if v, ok, err := pf.tryParseBool(); err != nil {
		return zero, err
	} else if ok {
		results = append(results, v)
	}

	if len(results) == 0 {
		return zero, fmt.Errorf("error: a value flag is required (-value, -value-hex, -value-int, -value-float, -value-bool)")
	}
	if len(results) > 1 {
		return zero, fmt.Errorf("error: only one value flag allowed")
	}
	return results[0], nil
}

func (pf *putFlags) tryParseString() (tinykvs.Value, bool) {
	if pf.value != "" {
		return tinykvs.StringValue(pf.value), true
	}
	return tinykvs.Value{}, false
}

func (pf *putFlags) tryParseHex() (tinykvs.Value, bool, error) {
	if pf.valueHex != "" {
		bytes, err := hex.DecodeString(pf.valueHex)
		if err != nil {
			return tinykvs.Value{}, false, fmt.Errorf("error decoding hex value: %v", err)
		}
		return tinykvs.BytesValue(bytes), true, nil
	}
	return tinykvs.Value{}, false, nil
}

func (pf *putFlags) tryParseInt() (tinykvs.Value, bool) {
	if pf.valueInt != 0 || containsFlag(pf.args, "-value-int") {
		return tinykvs.Int64Value(pf.valueInt), true
	}
	return tinykvs.Value{}, false
}

func (pf *putFlags) tryParseFloat() (tinykvs.Value, bool) {
	if pf.valueFloat != 0 || containsFlag(pf.args, "-value-float") {
		return tinykvs.Float64Value(pf.valueFloat), true
	}
	return tinykvs.Value{}, false
}

func (pf *putFlags) tryParseBool() (tinykvs.Value, bool, error) {
	if pf.valueBool != "" {
		v, err := parseBoolValue(pf.valueBool)
		if err != nil {
			return tinykvs.Value{}, false, err
		}
		return v, true, nil
	}
	return tinykvs.Value{}, false, nil
}

// parseBoolValue parses a boolean value from string.
func parseBoolValue(s string) (tinykvs.Value, error) {
	switch strings.ToLower(s) {
	case "true", "1", "yes":
		return tinykvs.BoolValue(true), nil
	case "false", "0", "no":
		return tinykvs.BoolValue(false), nil
	default:
		return tinykvs.Value{}, fmt.Errorf("error: invalid bool value: %s", s)
	}
}

func containsFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag || strings.HasPrefix(a, flag+"=") {
			return true
		}
	}
	return false
}

// checkOrphanFile checks if a dir entry is an orphan SST file.
func checkOrphanFile(e os.DirEntry, validIDs map[uint32]bool) (string, int64, bool) {
	if !strings.HasSuffix(e.Name(), ".sst") {
		return "", 0, false
	}
	name := strings.TrimSuffix(e.Name(), ".sst")
	id, err := parseUint32(name)
	if err != nil {
		return "", 0, false
	}
	if validIDs[id] {
		return "", 0, false
	}
	info, _ := e.Info()
	return e.Name(), info.Size(), true
}

func parseUint32(s string) (uint32, error) {
	var result uint32
	n, err := fmt.Sscanf(s, "%d", &result)
	if err != nil || n != 1 {
		return 0, fmt.Errorf("invalid uint32: %s", s)
	}
	return result, nil
}

func valueTypeName(t tinykvs.ValueType) string {
	switch t {
	case tinykvs.ValueTypeInt64:
		return "int64"
	case tinykvs.ValueTypeFloat64:
		return "float64"
	case tinykvs.ValueTypeBool:
		return "bool"
	case tinykvs.ValueTypeString:
		return "string"
	case tinykvs.ValueTypeBytes:
		return "bytes"
	case tinykvs.ValueTypeTombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// importSingleRecord parses and stores a single CSV record.
func importSingleRecord(store *tinykvs.Store, record []string) bool {
	if len(record) != 3 {
		return false
	}
	keyBytes, err := hex.DecodeString(record[0])
	if err != nil {
		return false
	}
	val, err := parseTypedValue(record[1], record[2])
	if err != nil {
		return false
	}
	return store.Put(keyBytes, val) == nil
}

func formatKey(key []byte) string {
	for _, b := range key {
		if b < 32 || b > 126 {
			return "0x" + hex.EncodeToString(key)
		}
	}
	if len(key) > 0 {
		return string(key)
	}
	return "0x" + hex.EncodeToString(key)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

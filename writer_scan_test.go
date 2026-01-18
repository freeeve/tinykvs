package tinykvs

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestPrefixScanBasic(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Create data with various prefixes
	prefixes := map[string]int{
		"user:":    10,
		"post:":    20,
		"comment:": 15,
		"tag:":     5,
	}

	for prefix, count := range prefixes {
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("%s%03d", prefix, i)
			if err := store.PutString([]byte(key), "value"); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}
	store.Flush()

	// Test each prefix
	for prefix, expectedCount := range prefixes {
		count := 0
		err := store.ScanPrefix([]byte(prefix), func(key []byte, value Value) bool {
			if !bytes.HasPrefix(key, []byte(prefix)) {
				t.Errorf("Key %s doesn't have prefix %s", string(key), prefix)
			}
			count++
			return true
		})
		if err != nil {
			t.Errorf("ScanPrefix(%s) failed: %v", prefix, err)
		}
		if count != expectedCount {
			t.Errorf("ScanPrefix(%s) returned %d keys, want %d", prefix, count, expectedCount)
		}
	}
}

// TestPrefixScanEmpty tests scanning with a prefix that matches no keys.
func TestPrefixScanEmpty(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Add some data
	store.PutString([]byte("aaa"), "value")
	store.PutString([]byte("bbb"), "value")
	store.PutString([]byte("ccc"), "value")
	store.Flush()

	// Scan with non-matching prefix
	count := 0
	err = store.ScanPrefix([]byte("zzz"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("ScanPrefix(zzz) returned %d keys, want 0", count)
	}
}

// TestPrefixScanWithDeletes tests that deleted keys are skipped in prefix scans.
func TestPrefixScanWithDeletes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create 20 keys with prefix
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("item:%03d", i)
		if err := store.PutString([]byte(key), "value"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	// Delete even-numbered keys
	for i := 0; i < 20; i += 2 {
		key := fmt.Sprintf("item:%03d", i)
		if err := store.Delete([]byte(key)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}
	store.Flush()

	store.Close()

	// Reopen and compact
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	store.Compact()

	// Scan should only return odd-numbered keys
	var keys []string
	err = store.ScanPrefix([]byte("item:"), func(key []byte, value Value) bool {
		keys = append(keys, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if len(keys) != 10 {
		t.Errorf("ScanPrefix returned %d keys, want 10", len(keys))
	}

	// Verify all returned keys are odd
	for _, key := range keys {
		var num int
		fmt.Sscanf(key, "item:%03d", &num)
		if num%2 == 0 {
			t.Errorf("Deleted key %s returned in scan", key)
		}
	}
}

// TestPrefixScanEarlyTermination tests that returning false stops the scan.
func TestPrefixScanEarlyTermination(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Create 100 keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := store.PutString([]byte(key), "value"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	// Scan and stop after 10
	count := 0
	err = store.ScanPrefix([]byte("key:"), func(key []byte, value Value) bool {
		count++
		return count < 10 // Stop after 10
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if count != 10 {
		t.Errorf("Scan stopped after %d keys, want 10", count)
	}
}

// TestPrefixScanAcrossLevels tests prefix scanning when data is split across L0 and L1.
func TestPrefixScanAcrossLevels(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256
	opts.L0CompactionTrigger = 4
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data in waves to create multiple levels
	for wave := 0; wave < 5; wave++ {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("data:%03d", i)
			value := fmt.Sprintf("wave%d", wave)
			if err := store.PutString([]byte(key), value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		store.Flush()
	}

	store.Close()

	// Reopen - data now in L0
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	// Partial compact (just some L0 -> L1)
	store.Compact()

	// Scan should return all 50 unique keys with latest values
	results := make(map[string]string)
	err = store.ScanPrefix([]byte("data:"), func(key []byte, value Value) bool {
		results[string(key)] = value.String()
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if len(results) != 50 {
		t.Errorf("ScanPrefix returned %d keys, want 50", len(results))
	}

	// All values should be from the last wave
	for key, value := range results {
		if value != "wave4" {
			t.Errorf("Key %s has value %s, want wave4", key, value)
		}
	}
}

// TestPrefixScanBinaryPrefix tests scanning with binary (non-string) prefixes.
func TestPrefixScanBinaryPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Create keys with binary prefixes
	prefix1 := []byte{0x01, 0x02}
	prefix2 := []byte{0x01, 0x03}
	prefix3 := []byte{0x02, 0x00}

	for i := 0; i < 10; i++ {
		key1 := append(append([]byte{}, prefix1...), byte(i))
		key2 := append(append([]byte{}, prefix2...), byte(i))
		key3 := append(append([]byte{}, prefix3...), byte(i))
		store.PutInt64(key1, int64(i))
		store.PutInt64(key2, int64(i+100))
		store.PutInt64(key3, int64(i+200))
	}
	store.Flush()

	// Scan each binary prefix
	for _, tc := range []struct {
		prefix   []byte
		expected int
		offset   int64
	}{
		{prefix1, 10, 0},
		{prefix2, 10, 100},
		{prefix3, 10, 200},
		{[]byte{0x01}, 20, 0}, // Both prefix1 and prefix2
	} {
		count := 0
		err := store.ScanPrefix(tc.prefix, func(key []byte, value Value) bool {
			count++
			return true
		})
		if err != nil {
			t.Errorf("ScanPrefix(%v) failed: %v", tc.prefix, err)
		}
		if count != tc.expected {
			t.Errorf("ScanPrefix(%v) returned %d keys, want %d", tc.prefix, count, tc.expected)
		}
	}
}

// TestPrefixScanOrder tests that prefix scan returns keys in sorted order.
func TestPrefixScanOrder(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Insert keys in random order
	keys := []string{
		"item:050", "item:010", "item:099", "item:001",
		"item:075", "item:025", "item:000", "item:100",
	}
	for _, key := range keys {
		if err := store.PutString([]byte(key), "value"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	store.Close()

	// Reopen and compact
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	store.Compact()

	// Scan and verify order
	var scannedKeys []string
	err = store.ScanPrefix([]byte("item:"), func(key []byte, value Value) bool {
		scannedKeys = append(scannedKeys, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	// Verify sorted order
	for i := 1; i < len(scannedKeys); i++ {
		if scannedKeys[i-1] >= scannedKeys[i] {
			t.Errorf("Keys not in sorted order: %s >= %s", scannedKeys[i-1], scannedKeys[i])
		}
	}
}

// TestPrefixScanEmptyPrefix tests scanning with an empty prefix (all keys).
func TestPrefixScanEmptyPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Add various keys
	store.PutString([]byte("aaa"), "value")
	store.PutString([]byte("bbb"), "value")
	store.PutString([]byte("ccc"), "value")
	store.PutString([]byte("123"), "value")
	store.PutString([]byte{0x00, 0x01}, "binary")
	store.Flush()

	// Scan with empty prefix should return all keys
	count := 0
	err = store.ScanPrefix([]byte{}, func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if count != 5 {
		t.Errorf("ScanPrefix([]) returned %d keys, want 5", count)
	}
}

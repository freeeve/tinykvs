package tinykvs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Create and write
	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	entries := []WALEntry{
		{Operation: OpPut, Key: []byte("key1"), Value: StringValue("value1"), Sequence: 1},
		{Operation: OpPut, Key: []byte("key2"), Value: Int64Value(42), Sequence: 2},
		{Operation: OpDelete, Key: []byte("key3"), Sequence: 3},
		{Operation: OpPut, Key: []byte("key4"), Value: BoolValue(true), Sequence: 4},
	}

	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	wal.Close()

	// Reopen and recover
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf("recovered %d entries, want %d", len(recovered), len(entries))
	}

	for i, e := range recovered {
		if e.Operation != entries[i].Operation {
			t.Errorf("entry %d: operation = %d, want %d", i, e.Operation, entries[i].Operation)
		}
		if !bytes.Equal(e.Key, entries[i].Key) {
			t.Errorf("entry %d: key = %s, want %s", i, e.Key, entries[i].Key)
		}
		if e.Sequence != entries[i].Sequence {
			t.Errorf("entry %d: sequence = %d, want %d", i, e.Sequence, entries[i].Sequence)
		}
	}
}

func TestWALLargeRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerBatch)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Large value that requires fragmentation (> 32KB block)
	largeValue := bytes.Repeat([]byte("x"), 100*1024) // 100KB

	entry := WALEntry{
		Operation: OpPut,
		Key:       []byte("large-key"),
		Value:     BytesValue(largeValue),
		Sequence:  1,
	}

	if err := wal.Append(entry); err != nil {
		t.Fatalf("Append large record failed: %v", err)
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	wal.Close()

	// Recover
	wal, err = OpenWAL(path, WALSyncPerBatch)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != 1 {
		t.Fatalf("recovered %d entries, want 1", len(recovered))
	}

	if !bytes.Equal(recovered[0].Value.Bytes, largeValue) {
		t.Errorf("recovered value length = %d, want %d", len(recovered[0].Value.Bytes), len(largeValue))
	}
}

func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write some entries
	wal.Append(WALEntry{Operation: OpPut, Key: []byte("key1"), Value: StringValue("v1"), Sequence: 1})
	wal.Append(WALEntry{Operation: OpPut, Key: []byte("key2"), Value: StringValue("v2"), Sequence: 2})

	// Truncate
	if err := wal.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	wal.Close()

	// Recover should find nothing
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("recovered %d entries after truncate, want 0", len(recovered))
	}
}

func TestWALSyncModes(t *testing.T) {
	tests := []struct {
		name string
		mode WALSyncMode
	}{
		{"none", WALSyncNone},
		{"per-batch", WALSyncPerBatch},
		{"per-write", WALSyncPerWrite},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.wal")

			wal, err := OpenWAL(path, tt.mode)
			if err != nil {
				t.Fatalf("OpenWAL failed: %v", err)
			}

			wal.Append(WALEntry{Operation: OpPut, Key: []byte("key"), Value: StringValue("value"), Sequence: 1})
			wal.Sync()
			wal.Close()

			// Verify file exists
			if _, err := os.Stat(path); os.IsNotExist(err) {
				t.Error("WAL file not created")
			}
		})
	}
}

func TestWALEmptyRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Recover empty WAL
	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("recovered %d entries from empty WAL, want 0", len(recovered))
	}

	wal.Close()
}

func TestWALMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerBatch)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write many small entries to span multiple blocks
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		entry := WALEntry{
			Operation: OpPut,
			Key:       []byte(string(rune('a' + (i % 26)))),
			Value:     Int64Value(int64(i)),
			Sequence:  uint64(i),
		}
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	wal.Sync()
	wal.Close()

	// Recover
	wal, err = OpenWAL(path, WALSyncPerBatch)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != numEntries {
		t.Errorf("recovered %d entries, want %d", len(recovered), numEntries)
	}
}

func TestWALOpenError(t *testing.T) {
	// Try to open WAL in non-existent directory
	_, err := OpenWAL("/nonexistent/path/test.wal", WALSyncPerWrite)
	if err == nil {
		t.Error("OpenWAL should fail for non-existent directory")
	}
}

func TestWALTruncateBefore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write entries with sequences 1-10
	for i := uint64(1); i <= 10; i++ {
		entry := WALEntry{
			Operation: OpPut,
			Key:       []byte(string(rune('a' + i - 1))),
			Value:     Int64Value(int64(i)),
			Sequence:  i,
		}
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	wal.Sync()

	// Truncate entries before sequence 6
	if err := wal.TruncateBefore(6); err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}

	wal.Close()

	// Reopen and recover
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Should only have entries 6-10 (5 entries)
	if len(recovered) != 5 {
		t.Errorf("recovered %d entries, want 5", len(recovered))
	}

	// Verify sequences
	for i, e := range recovered {
		expectedSeq := uint64(6 + i)
		if e.Sequence != expectedSeq {
			t.Errorf("entry %d: sequence = %d, want %d", i, e.Sequence, expectedSeq)
		}
	}
}

func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncNone)
	if err != nil {
		b.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	entry := WALEntry{
		Operation: OpPut,
		Key:       []byte("benchmark-key"),
		Value:     StringValue("benchmark-value"),
		Sequence:  0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Sequence = uint64(i)
		wal.Append(entry)
	}
}

func TestWALRecoverCorruptedChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write some valid entries
	for i := 0; i < 10; i++ {
		entry := WALEntry{
			Operation: OpPut,
			Key:       []byte(string(rune('a' + i))),
			Value:     Int64Value(int64(i)),
			Sequence:  uint64(i),
		}
		wal.Append(entry)
	}
	wal.Sync()
	wal.Close()

	// Corrupt the WAL file (corrupt a checksum in the middle)
	data, _ := os.ReadFile(path)
	if len(data) > 100 {
		data[50] ^= 0xFF // Corrupt a byte
	}
	os.WriteFile(path, data, 0644)

	// Reopen and recover - should recover entries before corruption
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Should recover at least some entries (before the corruption)
	// The exact number depends on where we corrupted
	t.Logf("Recovered %d entries after corruption", len(recovered))
}

func TestWALAllValueTypes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	entries := []WALEntry{
		{Operation: OpPut, Key: []byte("int"), Value: Int64Value(42), Sequence: 1},
		{Operation: OpPut, Key: []byte("float"), Value: Float64Value(3.14), Sequence: 2},
		{Operation: OpPut, Key: []byte("bool"), Value: BoolValue(true), Sequence: 3},
		{Operation: OpPut, Key: []byte("string"), Value: StringValue("hello"), Sequence: 4},
		{Operation: OpPut, Key: []byte("bytes"), Value: BytesValue([]byte{1, 2, 3}), Sequence: 5},
		{Operation: OpDelete, Key: []byte("deleted"), Sequence: 6},
	}

	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	wal.Close()

	// Recover
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf("recovered %d entries, want %d", len(recovered), len(entries))
	}

	// Verify types
	for i, e := range recovered {
		if e.Value.Type != entries[i].Value.Type {
			t.Errorf("entry %d type = %d, want %d", i, e.Value.Type, entries[i].Value.Type)
		}
	}
}

func TestWALRecoverPartialBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write entries
	for i := 0; i < 5; i++ {
		entry := WALEntry{
			Operation: OpPut,
			Key:       []byte(string(rune('a' + i))),
			Value:     Int64Value(int64(i)),
			Sequence:  uint64(i),
		}
		wal.Append(entry)
	}
	wal.Close()

	// Truncate file to partial block
	data, _ := os.ReadFile(path)
	if len(data) > 50 {
		os.WriteFile(path, data[:len(data)-10], 0644)
	}

	// Should still recover what we can
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	t.Logf("Recovered %d entries from truncated WAL", len(recovered))
}

func TestWALManySmallEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerBatch)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write many small entries to exercise block boundaries
	numEntries := 5000
	for i := 0; i < numEntries; i++ {
		entry := WALEntry{
			Operation: OpPut,
			Key:       []byte(fmt.Sprintf("key%05d", i)),
			Value:     Int64Value(int64(i)),
			Sequence:  uint64(i),
		}
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}
	wal.Sync()
	wal.Close()

	// Recover
	wal, err = OpenWAL(path, WALSyncPerBatch)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != numEntries {
		t.Errorf("recovered %d entries, want %d", len(recovered), numEntries)
	}
}

func TestWALDecodeEntryVariousTypes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Test all value types including edge cases
	entries := []WALEntry{
		{Operation: OpPut, Key: []byte("maxint"), Value: Int64Value(9223372036854775807), Sequence: 1},
		{Operation: OpPut, Key: []byte("minint"), Value: Int64Value(-9223372036854775808), Sequence: 2},
		{Operation: OpPut, Key: []byte("zero"), Value: Int64Value(0), Sequence: 3},
		{Operation: OpPut, Key: []byte("pi"), Value: Float64Value(3.14159265358979323846), Sequence: 4},
		{Operation: OpPut, Key: []byte("empty"), Value: StringValue(""), Sequence: 5},
		{Operation: OpPut, Key: []byte("nullbytes"), Value: BytesValue([]byte{0, 0, 0}), Sequence: 6},
	}

	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	wal.Close()

	// Recover and verify
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL (reopen) failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf("recovered %d entries, want %d", len(recovered), len(entries))
	}

	// Verify specific values
	if recovered[0].Value.Int64 != 9223372036854775807 {
		t.Error("maxint value mismatch")
	}
	if recovered[1].Value.Int64 != -9223372036854775808 {
		t.Error("minint value mismatch")
	}
}

func TestWALCorruptedShortEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Write a valid entry first
	wal, err := OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	wal.Append(WALEntry{Operation: OpPut, Key: []byte("key1"), Value: StringValue("value1"), Sequence: 1})
	wal.Close()

	// Corrupt by truncating the file to make entry too short
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	f.Truncate(10) // Too short to contain a valid entry
	f.Close()

	// Try to recover - should handle gracefully
	wal, err = OpenWAL(path, WALSyncPerWrite)
	if err != nil {
		t.Logf("OpenWAL on corrupted file: %v (expected)", err)
		return
	}
	defer wal.Close()

	_, err = wal.Recover()
	// Should either succeed with 0 entries or return error
	if err != nil {
		t.Logf("Recover on corrupted file: %v", err)
	}
}

func TestWALCorruptedKeyLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Create file with garbage that has invalid key length
	f, _ := os.Create(path)
	// Write record header with valid-looking size but invalid content
	// Record format: size(4) + checksum(4) + data
	// data format: op(1) + seq(8) + keylen(4) + key + value
	data := make([]byte, 100)
	// Set a huge key length that exceeds data
	data[13] = 0xFF // keylen low byte
	data[14] = 0xFF // keylen high byte
	f.Write(data)
	f.Close()

	wal, _ := OpenWAL(path, WALSyncPerWrite)
	if wal != nil {
		wal.Recover() // May fail or succeed with empty
		wal.Close()
	}
}

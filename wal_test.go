package tinykvs

import (
	"bytes"
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

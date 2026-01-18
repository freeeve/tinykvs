package tinykvs

import (
	"fmt"
	"testing"
	"time"
)

// TestEmptyValues verifies that empty byte slices can be stored and retrieved.
func TestEmptyValues(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Store empty value
	if err := store.PutBytes([]byte("empty-key"), []byte{}); err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	// Store empty string
	if err := store.PutString([]byte("empty-string"), ""); err != nil {
		t.Fatalf("PutString failed: %v", err)
	}

	store.Flush()

	// Verify before close
	val, err := store.Get([]byte("empty-key"))
	if err != nil {
		t.Fatalf("Get empty-key failed: %v", err)
	}
	if len(val.Bytes) != 0 {
		t.Errorf("Get empty-key = %v, want empty slice", val.Bytes)
	}

	val, err = store.Get([]byte("empty-string"))
	if err != nil {
		t.Fatalf("Get empty-string failed: %v", err)
	}
	if val.String() != "" {
		t.Errorf("Get empty-string = %q, want empty string", val.String())
	}

	store.Close()

	// Reopen and verify
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	store.Compact()

	val, err = store.Get([]byte("empty-key"))
	if err != nil {
		t.Fatalf("Get empty-key after reopen failed: %v", err)
	}
	if len(val.Bytes) != 0 {
		t.Errorf("Get empty-key after reopen = %v, want empty slice", val.Bytes)
	}

	val, err = store.Get([]byte("empty-string"))
	if err != nil {
		t.Fatalf("Get empty-string after reopen failed: %v", err)
	}
	if val.String() != "" {
		t.Errorf("Get empty-string after reopen = %q, want empty string", val.String())
	}
}

// TestBinaryKeysWithNullBytes verifies that keys containing null bytes work correctly.
func TestBinaryKeysWithNullBytes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Keys with null bytes in various positions
	keys := [][]byte{
		{0x00, 'a', 'b', 'c'},        // null at start
		{'a', 0x00, 'b', 'c'},        // null in middle
		{'a', 'b', 'c', 0x00},        // null at end
		{0x00, 0x00, 0x00},           // all nulls
		{'a', 0x00, 0x00, 'b', 0x00}, // multiple nulls
	}

	// Write all keys
	for i, key := range keys {
		value := fmt.Sprintf("value-%d", i)
		if err := store.PutString(key, value); err != nil {
			t.Fatalf("Put key %v failed: %v", key, err)
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

	// Verify all keys
	for i, key := range keys {
		expected := fmt.Sprintf("value-%d", i)
		val, err := store.Get(key)
		if err != nil {
			t.Errorf("Get key %v failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get key %v = %q, want %q", key, val.String(), expected)
		}
	}
}

// TestVeryLargeValues verifies that values spanning multiple blocks work correctly.
func TestVeryLargeValues(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockSize = 4096 // 4KB blocks
	opts.MemtableSize = 1024 * 1024
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create values of various sizes
	sizes := []int{
		100,   // small
		4096,  // exactly one block
		4097,  // just over one block
		16384, // 4 blocks
		65536, // 16 blocks
	}

	for _, size := range sizes {
		key := fmt.Sprintf("key-size-%d", size)
		value := make([]byte, size)
		for i := range value {
			value[i] = byte(i % 256)
		}

		if err := store.PutBytes([]byte(key), value); err != nil {
			t.Fatalf("Put size %d failed: %v", size, err)
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

	// Verify all values
	for _, size := range sizes {
		key := fmt.Sprintf("key-size-%d", size)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get size %d failed: %v", size, err)
			continue
		}
		if len(val.Bytes) != size {
			t.Errorf("Get size %d: got %d bytes, want %d", size, len(val.Bytes), size)
			continue
		}
		// Verify content
		for i := 0; i < size; i++ {
			if val.Bytes[i] != byte(i%256) {
				t.Errorf("Get size %d: byte %d = %d, want %d", size, i, val.Bytes[i], i%256)
				break
			}
		}
	}
}

// TestInterleavedDeletePutCycles tests delete → put → delete → put sequences.
func TestInterleavedDeletePutCycles(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("the-key")
	numCycles := 20

	// Cycle through put/delete
	for cycle := 0; cycle < numCycles; cycle++ {
		value := fmt.Sprintf("value-cycle-%d", cycle)
		if err := store.PutString(key, value); err != nil {
			t.Fatalf("Put cycle %d failed: %v", cycle, err)
		}
		store.Flush()

		if err := store.Delete(key); err != nil {
			t.Fatalf("Delete cycle %d failed: %v", cycle, err)
		}
		store.Flush()
	}

	// Final put
	finalValue := "final-value"
	if err := store.PutString(key, finalValue); err != nil {
		t.Fatalf("Final put failed: %v", err)
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

	// Verify final value exists
	val, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.String() != finalValue {
		t.Errorf("Get = %q, want %q", val.String(), finalValue)
	}
}

// TestAllKeysDeletedThenCompact verifies compaction with only tombstones.
func TestAllKeysDeletedThenCompact(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		if err := store.PutString([]byte(key), "some-value"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	// Delete all keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
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

	// Verify all keys are deleted
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) should return ErrKeyNotFound, got %v", key, err)
		}
	}

	// Verify we can still write new data
	if err := store.PutString([]byte("new-key"), "new-value"); err != nil {
		t.Fatalf("Put new key failed: %v", err)
	}
	val, err := store.Get([]byte("new-key"))
	if err != nil {
		t.Fatalf("Get new key failed: %v", err)
	}
	if val.String() != "new-value" {
		t.Errorf("Get new key = %q, want %q", val.String(), "new-value")
	}
}

// TestMixedValueTypesOverwrite verifies that overwriting with different types works.
func TestMixedValueTypesOverwrite(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("mixed-type-key")

	// Write as int64
	store.PutInt64(key, 42)
	store.Flush()

	// Overwrite as string
	store.PutString(key, "hello")
	store.Flush()

	// Overwrite as float64
	store.PutFloat64(key, 3.14159)
	store.Flush()

	// Overwrite as bytes
	store.PutBytes(key, []byte{0xDE, 0xAD, 0xBE, 0xEF})
	store.Flush()

	// Overwrite as bool
	store.PutBool(key, true)
	store.Flush()

	// Final value as string
	finalValue := "final-string-value"
	store.PutString(key, finalValue)
	store.Flush()

	store.Close()

	// Reopen and compact
	store, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	store.Compact()

	// Verify final type and value
	val, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.Type != ValueTypeString {
		t.Errorf("Type = %v, want ValueTypeString", val.Type)
	}
	if val.String() != finalValue {
		t.Errorf("Get = %q, want %q", val.String(), finalValue)
	}
}

// TestDeleteRangeReopenCompact verifies DeleteRange survives reopen + compact.
func TestDeleteRangeReopenCompact(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data with keys: aaa000 to aaa099, bbb000 to bbb099, ccc000 to ccc099
	for _, prefix := range []string{"aaa", "bbb", "ccc"} {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("%s%03d", prefix, i)
			if err := store.PutString([]byte(key), "value"); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}
	store.Flush()

	// Delete range bbb000 to bbb099
	deleted, err := store.DeleteRange([]byte("bbb000"), []byte("bbb999"))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	if deleted != 100 {
		t.Logf("DeleteRange deleted %d keys", deleted)
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

	// Verify: aaa and ccc keys exist, bbb keys deleted
	for _, prefix := range []string{"aaa", "ccc"} {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("%s%03d", prefix, i)
			_, err := store.Get([]byte(key))
			if err != nil {
				t.Errorf("Get(%s) should exist, got %v", key, err)
			}
		}
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("bbb%03d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) should return ErrKeyNotFound, got %v", key, err)
		}
	}
}

// TestDeletePrefixReopenCompact verifies DeletePrefix survives reopen + compact.
func TestDeletePrefixReopenCompact(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512
	opts.L0CompactionTrigger = 100
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data with prefixes: user:, post:, comment:
	for _, prefix := range []string{"user:", "post:", "comment:"} {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("%s%03d", prefix, i)
			if err := store.PutString([]byte(key), "value"); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}
	store.Flush()

	// Delete all post: keys
	deleted, err := store.DeletePrefix([]byte("post:"))
	if err != nil {
		t.Fatalf("DeletePrefix failed: %v", err)
	}
	if deleted != 50 {
		t.Logf("DeletePrefix deleted %d keys", deleted)
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

	// Verify: user: and comment: keys exist, post: keys deleted
	for _, prefix := range []string{"user:", "comment:"} {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("%s%03d", prefix, i)
			_, err := store.Get([]byte(key))
			if err != nil {
				t.Errorf("Get(%s) should exist, got %v", key, err)
			}
		}
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("post:%03d", i)
		_, err := store.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Get(%s) should return ErrKeyNotFound, got %v", key, err)
		}
	}
}

// TestReopenWithModifiedOptions verifies data survives when options change.
func TestReopenWithModifiedOptions(t *testing.T) {
	dir := t.TempDir()

	// First open with specific options
	opts1 := DefaultOptions(dir)
	opts1.BlockSize = 4096
	opts1.CompressionLevel = 1
	opts1.CompactionInterval = time.Hour

	store, err := Open(dir, opts1)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()
	store.Close()

	// Reopen with different options
	opts2 := DefaultOptions(dir)
	opts2.BlockSize = 8192     // Different block size
	opts2.CompressionLevel = 3 // Different compression
	opts2.CompactionInterval = time.Hour

	store, err = Open(dir, opts2)
	if err != nil {
		t.Fatalf("Reopen with different options failed: %v", err)
	}
	defer store.Close()

	// Verify all data is readable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("value%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) = %q, want %q", key, val.String(), expected)
		}
	}

	// Write more data with new options
	for i := numKeys; i < numKeys*2; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := store.PutString([]byte(key), value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	store.Flush()

	// Compact (mixes old and new format data)
	store.Compact()

	// Verify all data
	for i := 0; i < numKeys*2; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("value%05d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) after compact failed: %v", key, err)
			continue
		}
		if val.String() != expected {
			t.Errorf("Get(%s) after compact = %q, want %q", key, val.String(), expected)
		}
	}
}

// TestBatchWithDuplicateKeys verifies batch operations handle duplicate keys correctly.
func TestBatchWithDuplicateKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024
	opts.CompactionInterval = time.Hour

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create batch with same key multiple times
	batch := NewBatch()
	key := []byte("duplicate-key")

	batch.Put(key, StringValue("first"))
	batch.Put(key, StringValue("second"))
	batch.Put(key, StringValue("third"))
	batch.Put(key, Int64Value(42))
	batch.Put(key, StringValue("final"))

	if err := store.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Verify the last value wins
	val, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.String() != "final" {
		t.Errorf("Get = %q, want %q", val.String(), "final")
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

	// Verify after compact
	val, err = store.Get(key)
	if err != nil {
		t.Fatalf("Get after compact failed: %v", err)
	}
	if val.String() != "final" {
		t.Errorf("Get after compact = %q, want %q", val.String(), "final")
	}
}

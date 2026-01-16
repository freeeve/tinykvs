package tinykvs

import (
	"fmt"
	"testing"
)

func TestBatchWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create a batch
	batch := NewBatch()
	batch.PutString([]byte("key1"), "value1")
	batch.PutString([]byte("key2"), "value2")
	batch.PutInt64([]byte("counter"), 100)
	batch.Delete([]byte("nonexistent"))

	if batch.Len() != 4 {
		t.Errorf("batch.Len() = %d, want 4", batch.Len())
	}

	// Write batch
	if err := store.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Verify writes
	v1, _ := store.GetString([]byte("key1"))
	if v1 != "value1" {
		t.Errorf("key1 = %q, want value1", v1)
	}

	v2, _ := store.GetString([]byte("key2"))
	if v2 != "value2" {
		t.Errorf("key2 = %q, want value2", v2)
	}

	v3, _ := store.GetInt64([]byte("counter"))
	if v3 != 100 {
		t.Errorf("counter = %d, want 100", v3)
	}

	// Test batch reuse
	batch.Reset()
	if batch.Len() != 0 {
		t.Errorf("after Reset, batch.Len() = %d, want 0", batch.Len())
	}
}

func TestBatchWriteEmpty(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Empty batch should be no-op
	batch := NewBatch()
	if err := store.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch on empty batch failed: %v", err)
	}

	// Nil batch should be no-op
	if err := store.WriteBatch(nil); err != nil {
		t.Fatalf("WriteBatch on nil batch failed: %v", err)
	}
}

func TestPutIfNotExists(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// First put should succeed
	err = store.PutIfNotExists([]byte("key"), StringValue("value1"))
	if err != nil {
		t.Fatalf("first PutIfNotExists failed: %v", err)
	}

	// Second put should fail
	err = store.PutIfNotExists([]byte("key"), StringValue("value2"))
	if err != ErrKeyExists {
		t.Errorf("second PutIfNotExists: got %v, want ErrKeyExists", err)
	}

	// Verify original value
	v, _ := store.GetString([]byte("key"))
	if v != "value1" {
		t.Errorf("value = %q, want value1", v)
	}
}

func TestPutIfEquals(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Set initial value
	store.Put([]byte("key"), Int64Value(100))

	// CAS with correct expected value should succeed
	err = store.PutIfEquals([]byte("key"), Int64Value(200), Int64Value(100))
	if err != nil {
		t.Fatalf("PutIfEquals with correct expected failed: %v", err)
	}

	// Verify new value
	v, _ := store.GetInt64([]byte("key"))
	if v != 200 {
		t.Errorf("value = %d, want 200", v)
	}

	// CAS with wrong expected value should fail
	err = store.PutIfEquals([]byte("key"), Int64Value(300), Int64Value(100))
	if err != ErrConditionFailed {
		t.Errorf("PutIfEquals with wrong expected: got %v, want ErrConditionFailed", err)
	}

	// Value should be unchanged
	v, _ = store.GetInt64([]byte("key"))
	if v != 200 {
		t.Errorf("value after failed CAS = %d, want 200", v)
	}

	// CAS on non-existent key should fail
	err = store.PutIfEquals([]byte("nokey"), Int64Value(1), Int64Value(0))
	if err != ErrKeyNotFound {
		t.Errorf("PutIfEquals on missing key: got %v, want ErrKeyNotFound", err)
	}
}

func TestIncrement(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Increment non-existent key (starts at 0)
	v, err := store.Increment([]byte("counter"), 5)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if v != 5 {
		t.Errorf("after first increment: got %d, want 5", v)
	}

	// Increment existing key
	v, err = store.Increment([]byte("counter"), 10)
	if err != nil {
		t.Fatalf("second Increment failed: %v", err)
	}
	if v != 15 {
		t.Errorf("after second increment: got %d, want 15", v)
	}

	// Decrement
	v, err = store.Increment([]byte("counter"), -3)
	if err != nil {
		t.Fatalf("decrement failed: %v", err)
	}
	if v != 12 {
		t.Errorf("after decrement: got %d, want 12", v)
	}

	// Increment on wrong type should fail
	store.PutString([]byte("string"), "hello")
	_, err = store.Increment([]byte("string"), 1)
	if err != ErrTypeMismatch {
		t.Errorf("Increment on string: got %v, want ErrTypeMismatch", err)
	}
}

func TestDeleteRange(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create some keys
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		store.PutInt64([]byte(key), int64(i))
	}

	// Delete range [key03, key07)
	deleted, err := store.DeleteRange([]byte("key03"), []byte("key07"))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	if deleted != 4 {
		t.Errorf("deleted = %d, want 4", deleted)
	}

	// Verify remaining keys
	remaining := 0
	store.ScanPrefix([]byte("key"), func(key []byte, _ Value) bool {
		remaining++
		return true
	})
	if remaining != 6 {
		t.Errorf("remaining = %d, want 6", remaining)
	}
}

func TestDeletePrefix(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create keys with different prefixes
	for i := 0; i < 5; i++ {
		store.PutInt64([]byte(fmt.Sprintf("user:%d", i)), int64(i))
		store.PutInt64([]byte(fmt.Sprintf("item:%d", i)), int64(i))
	}

	// Delete all user: keys
	deleted, err := store.DeletePrefix([]byte("user:"))
	if err != nil {
		t.Fatalf("DeletePrefix failed: %v", err)
	}
	if deleted != 5 {
		t.Errorf("deleted = %d, want 5", deleted)
	}

	// Verify user: keys are gone
	userCount := 0
	store.ScanPrefix([]byte("user:"), func(key []byte, _ Value) bool {
		userCount++
		return true
	})
	if userCount != 0 {
		t.Errorf("user count = %d, want 0", userCount)
	}

	// Verify item: keys still exist
	itemCount := 0
	store.ScanPrefix([]byte("item:"), func(key []byte, _ Value) bool {
		itemCount++
		return true
	})
	if itemCount != 5 {
		t.Errorf("item count = %d, want 5", itemCount)
	}
}

func TestBatchPutMap(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	batch := NewBatch()
	batch.PutMap([]byte("user:1"), map[string]any{"name": "Alice"})
	batch.PutMap([]byte("user:2"), map[string]any{"name": "Bob"})
	batch.PutMap([]byte("user:3"), map[string]any{"name": "Charlie"})

	if err := store.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Verify all records
	got1, err := store.GetMap([]byte("user:1"))
	if err != nil {
		t.Fatalf("GetMap user:1 failed: %v", err)
	}
	if got1["name"] != "Alice" {
		t.Errorf("user:1 name = %v, want Alice", got1["name"])
	}

	got2, err := store.GetMap([]byte("user:2"))
	if err != nil {
		t.Fatalf("GetMap user:2 failed: %v", err)
	}
	if got2["name"] != "Bob" {
		t.Errorf("user:2 name = %v, want Bob", got2["name"])
	}

	got3, err := store.GetMap([]byte("user:3"))
	if err != nil {
		t.Fatalf("GetMap user:3 failed: %v", err)
	}
	if got3["name"] != "Charlie" {
		t.Errorf("user:3 name = %v, want Charlie", got3["name"])
	}
}

func TestBatchPutStruct(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type User struct {
		Name string `msgpack:"name"`
		Age  int    `msgpack:"age"`
	}

	batch := NewBatch()
	if err := batch.PutStruct([]byte("user:1"), User{Name: "Alice", Age: 30}); err != nil {
		t.Fatalf("batch.PutStruct failed: %v", err)
	}
	if err := batch.PutStruct([]byte("user:2"), User{Name: "Bob", Age: 25}); err != nil {
		t.Fatalf("batch.PutStruct failed: %v", err)
	}

	if err := store.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Verify using GetStruct
	var got1, got2 User
	if err := store.GetStruct([]byte("user:1"), &got1); err != nil {
		t.Fatalf("GetStruct user:1 failed: %v", err)
	}
	if got1.Name != "Alice" || got1.Age != 30 {
		t.Errorf("user:1 = %+v, want {Alice 30}", got1)
	}

	if err := store.GetStruct([]byte("user:2"), &got2); err != nil {
		t.Fatalf("GetStruct user:2 failed: %v", err)
	}
	if got2.Name != "Bob" || got2.Age != 25 {
		t.Errorf("user:2 = %+v, want {Bob 25}", got2)
	}
}

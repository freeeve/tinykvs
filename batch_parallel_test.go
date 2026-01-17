package tinykvs

import (
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

type TestUser struct {
	ID       int64   `msgpack:"id"`
	Name     string  `msgpack:"name"`
	Email    string  `msgpack:"email"`
	Age      int     `msgpack:"age"`
	Balance  float64 `msgpack:"balance"`
	Active   bool    `msgpack:"active"`
	Tags     []string `msgpack:"tags"`
}

func makeTestUsers(n int) []KeyStruct {
	items := make([]KeyStruct, n)
	for i := 0; i < n; i++ {
		items[i] = KeyStruct{
			Key: []byte(fmt.Sprintf("user:%08d", i)),
			Value: TestUser{
				ID:      int64(i),
				Name:    fmt.Sprintf("User %d", i),
				Email:   fmt.Sprintf("user%d@example.com", i),
				Age:     20 + (i % 50),
				Balance: float64(i) * 100.5,
				Active:  i%2 == 0,
				Tags:    []string{"tag1", "tag2", "tag3"},
			},
		}
	}
	return items
}

// BenchmarkBatchSequential benchmarks the original sequential PutStruct
func BenchmarkBatchSequential(b *testing.B) {
	items := makeTestUsers(10000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := NewBatch()
		for _, item := range items {
			batch.PutStruct(item.Key, item.Value)
		}
	}
}

// BenchmarkStorePutStructs benchmarks the simple Store.PutStructs API (encode + write)
func BenchmarkStorePutStructs(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.WALSyncMode = WALSyncNone // Fast for benchmark
	store, _ := Open(dir, opts)
	defer store.Close()

	items := makeTestUsers(10000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.PutStructs(items)
	}
}

// BenchmarkStoreSequentialPutStruct benchmarks sequential Store.PutStruct calls
func BenchmarkStoreSequentialPutStruct(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.WALSyncMode = WALSyncNone
	store, _ := Open(dir, opts)
	defer store.Close()

	items := makeTestUsers(10000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, item := range items {
			store.PutStruct(item.Key, item.Value)
		}
	}
}

func TestStorePutStructs(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Create items
	numItems := 1000
	items := make([]KeyStruct, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = KeyStruct{
			Key: []byte(fmt.Sprintf("user:%05d", i)),
			Value: TestUser{
				ID:   int64(i),
				Name: fmt.Sprintf("User %d", i),
			},
		}
	}

	// Simple one-liner API
	if err := store.PutStructs(items); err != nil {
		t.Fatalf("PutStructs failed: %v", err)
	}

	// Verify all keys
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("user:%05d", i))
		val, err := store.Get(key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		var user TestUser
		if err := msgpack.Unmarshal(val.Bytes, &user); err != nil {
			t.Errorf("Decode failed: %v", err)
			continue
		}
		if user.ID != int64(i) {
			t.Errorf("User ID = %d, want %d", user.ID, i)
		}
	}
}


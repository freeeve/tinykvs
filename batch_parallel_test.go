package tinykvs

import (
	"fmt"
	"testing"

	"github.com/freeeve/msgpck"
)

type TestUser struct {
	ID      int64    `msgpack:"id"`
	Name    string   `msgpack:"name"`
	Email   string   `msgpack:"email"`
	Age     int      `msgpack:"age"`
	Balance float64  `msgpack:"balance"`
	Active  bool     `msgpack:"active"`
	Tags    []string `msgpack:"tags"`
}

func makeTestUsers(n int) []KeyValue[TestUser] {
	items := make([]KeyValue[TestUser], n)
	for i := 0; i < n; i++ {
		user := TestUser{
			ID:      int64(i),
			Name:    fmt.Sprintf("User %d", i),
			Email:   fmt.Sprintf("user%d@example.com", i),
			Age:     20 + (i % 50),
			Balance: float64(i) * 100.5,
			Active:  i%2 == 0,
			Tags:    []string{"tag1", "tag2", "tag3"},
		}
		items[i] = KeyValue[TestUser]{
			Key:   []byte(fmt.Sprintf("user:%08d", i)),
			Value: &user,
		}
	}
	return items
}

// BenchmarkBatchSequential benchmarks sequential BatchPutStruct
func BenchmarkBatchSequential(b *testing.B) {
	items := makeTestUsers(10000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := NewBatch()
		for _, item := range items {
			BatchPutStruct(batch, item.Key, item.Value)
		}
	}
}

// BenchmarkStorePutStructs benchmarks PutStructs (parallel encode + write)
func BenchmarkStorePutStructs(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)
	opts.WALSyncMode = WALSyncNone // Fast for benchmark
	store, _ := Open(dir, opts)
	defer store.Close()

	items := makeTestUsers(10000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		PutStructs(store, items)
	}
}

// BenchmarkStoreSequentialPutStruct benchmarks sequential PutStruct calls
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
			PutStruct(store, item.Key, item.Value)
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
	items := make([]KeyValue[TestUser], numItems)
	for i := 0; i < numItems; i++ {
		user := TestUser{
			ID:   int64(i),
			Name: fmt.Sprintf("User %d", i),
		}
		items[i] = KeyValue[TestUser]{
			Key:   []byte(fmt.Sprintf("user:%05d", i)),
			Value: &user,
		}
	}

	// Parallel bulk insert
	if err := PutStructs(store, items); err != nil {
		t.Fatalf("PutStructs failed: %v", err)
	}

	// Verify all keys using msgpck decoder
	dec := msgpck.GetStructDecoder[TestUser](false)
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("user:%05d", i))
		val, err := store.Get(key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		var user TestUser
		if err := dec.Decode(val.Bytes, &user); err != nil {
			t.Errorf("Decode failed: %v", err)
			continue
		}
		if user.ID != int64(i) {
			t.Errorf("User ID = %d, want %d", user.ID, i)
		}
	}
}

func TestBatchPutStructsSequential(t *testing.T) {
	// Test the sequential encoding path (numWorkers=1)
	items := makeTestUsers(10)

	batch := NewBatch()
	// Use numWorkers=1 to hit encodeItemsSequential
	if err := batchPutStructsParallel(batch, items, 1); err != nil {
		t.Fatalf("batchPutStructsParallel failed: %v", err)
	}

	if batch.Len() != 10 {
		t.Errorf("batch length = %d, want 10", batch.Len())
	}
}

func TestBatchPutStructsSmallBatch(t *testing.T) {
	// Test with fewer items than workers to hit sequential path
	items := makeTestUsers(2)

	batch := NewBatch()
	// numWorkers > len(items) should use sequential path
	if err := batchPutStructsParallel(batch, items, 8); err != nil {
		t.Fatalf("batchPutStructsParallel failed: %v", err)
	}

	if batch.Len() != 2 {
		t.Errorf("batch length = %d, want 2", batch.Len())
	}
}

func TestBatchPutStructsEmpty(t *testing.T) {
	// Test empty items
	var items []KeyValue[TestUser]

	batch := NewBatch()
	if err := batchPutStructsParallel(batch, items, 4); err != nil {
		t.Fatalf("batchPutStructsParallel with empty items failed: %v", err)
	}

	if batch.Len() != 0 {
		t.Errorf("batch length = %d, want 0", batch.Len())
	}
}

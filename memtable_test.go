package tinykvs

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestMemtablePutGet(t *testing.T) {
	mt := NewMemtable()

	// Put some values
	mt.Put([]byte("key1"), StringValue("value1"), 1)
	mt.Put([]byte("key2"), Int64Value(42), 2)
	mt.Put([]byte("key3"), BoolValue(true), 3)

	// Get them back
	tests := []struct {
		key   []byte
		want  Value
		found bool
	}{
		{[]byte("key1"), StringValue("value1"), true},
		{[]byte("key2"), Int64Value(42), true},
		{[]byte("key3"), BoolValue(true), true},
		{[]byte("key4"), Value{}, false},
	}

	for _, tt := range tests {
		entry, found := mt.Get(tt.key)
		if found != tt.found {
			t.Errorf("Get(%s): found = %v, want %v", tt.key, found, tt.found)
			continue
		}
		if found && entry.Value.Type != tt.want.Type {
			t.Errorf("Get(%s): type = %d, want %d", tt.key, entry.Value.Type, tt.want.Type)
		}
	}
}

func TestMemtableUpdate(t *testing.T) {
	mt := NewMemtable()

	// Insert and update
	mt.Put([]byte("key"), StringValue("v1"), 1)
	mt.Put([]byte("key"), StringValue("v2"), 2)

	entry, found := mt.Get([]byte("key"))
	if !found {
		t.Fatal("key not found")
	}
	if entry.Value.String() != "v2" {
		t.Errorf("value = %s, want v2", entry.Value.String())
	}
	if entry.Sequence != 2 {
		t.Errorf("sequence = %d, want 2", entry.Sequence)
	}

	// Count should be 1 (not 2)
	if mt.Count() != 1 {
		t.Errorf("count = %d, want 1", mt.Count())
	}
}

func TestMemtableIterator(t *testing.T) {
	mt := NewMemtable()

	// Insert in random order
	keys := []string{"delta", "alpha", "charlie", "bravo"}
	for i, k := range keys {
		mt.Put([]byte(k), Int64Value(int64(i)), uint64(i))
	}

	// Iterator should return in sorted order
	expected := []string{"alpha", "bravo", "charlie", "delta"}

	iter := mt.Iterator()
	defer iter.Close()

	i := 0
	for iter.Next() {
		if i >= len(expected) {
			t.Fatalf("too many entries")
		}
		if string(iter.Key()) != expected[i] {
			t.Errorf("entry %d: key = %s, want %s", i, iter.Key(), expected[i])
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d entries, want %d", i, len(expected))
	}
}

func TestMemtableIteratorSeek(t *testing.T) {
	mt := NewMemtable()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		mt.Put([]byte(key), Int64Value(int64(i)), uint64(i))
	}

	iter := mt.Iterator()
	defer iter.Close()

	// Seek to key050
	if !iter.Seek([]byte("key050")) {
		t.Fatal("seek failed")
	}
	if string(iter.Key()) != "key050" {
		t.Errorf("after seek: key = %s, want key050", iter.Key())
	}

	// Seek to non-existent key (should land on next)
	iter2 := mt.Iterator()
	defer iter2.Close()
	if !iter2.Seek([]byte("key025x")) {
		t.Fatal("seek failed")
	}
	if string(iter2.Key()) != "key026" {
		t.Errorf("after seek: key = %s, want key026", iter2.Key())
	}
}

func TestMemtableIteratorValueAndValid(t *testing.T) {
	mt := NewMemtable()

	mt.Put([]byte("key1"), Int64Value(42), 1)
	mt.Put([]byte("key2"), StringValue("hello"), 2)

	iter := mt.Iterator()
	defer iter.Close()

	// Before Next(), Valid should be false
	if iter.Valid() {
		t.Error("Valid() should be false before Next()")
	}

	// After Next(), should be valid with correct value
	if !iter.Next() {
		t.Fatal("Next() should return true")
	}
	if !iter.Valid() {
		t.Error("Valid() should be true after Next()")
	}

	val := iter.Value()
	if val.Type != ValueTypeInt64 || val.Int64 != 42 {
		t.Errorf("Value() = %+v, want Int64(42)", val)
	}

	// Second entry
	if !iter.Next() {
		t.Fatal("Next() should return true for second entry")
	}
	val = iter.Value()
	if val.Type != ValueTypeString || string(val.Bytes) != "hello" {
		t.Errorf("Value() = %+v, want String(hello)", val)
	}

	// After exhausting, Valid should be false
	iter.Next() // exhaust
	if iter.Valid() {
		t.Error("Valid() should be false after exhausting iterator")
	}
}

func TestMemtableSizeTracking(t *testing.T) {
	mt := NewMemtable()

	if mt.Size() != 0 {
		t.Errorf("initial size = %d, want 0", mt.Size())
	}

	mt.Put([]byte("key"), StringValue("value"), 1)
	if mt.Size() == 0 {
		t.Error("size should be > 0 after put")
	}
}

func TestMemtableConcurrentReads(t *testing.T) {
	mt := NewMemtable()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%04d", i)
		mt.Put([]byte(key), Int64Value(int64(i)), uint64(i))
	}

	// Concurrent reads
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("key%04d", i*10)
				entry, found := mt.Get([]byte(key))
				if !found {
					t.Errorf("key %s not found", key)
				}
				if entry.Value.Int64 != int64(i*10) {
					t.Errorf("wrong value for %s", key)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkMemtablePut(b *testing.B) {
	mt := NewMemtable()
	value := StringValue("benchmark value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		mt.Put([]byte(key), value, uint64(i))
	}
}

func BenchmarkMemtableGet(b *testing.B) {
	mt := NewMemtable()
	value := StringValue("benchmark value")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		mt.Put([]byte(key), value, uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%10000)
		mt.Get([]byte(key))
	}
}

func BenchmarkMemtableIterator(b *testing.B) {
	mt := NewMemtable()
	value := StringValue("benchmark value")

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		mt.Put([]byte(key), value, uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := mt.Iterator()
		count := 0
		for iter.Next() {
			_ = iter.Entry()
			count++
		}
		iter.Close()
		if count != 10000 {
			b.Fatalf("wrong count: %d", count)
		}
	}
}

func TestMemtableIteratorEmpty(t *testing.T) {
	mt := NewMemtable()
	iter := mt.Iterator()
	defer iter.Close()

	if iter.Next() {
		t.Error("Next() should return false for empty memtable")
	}
}

func TestMemtableLargeKeys(t *testing.T) {
	mt := NewMemtable()

	// Large key
	largeKey := bytes.Repeat([]byte("x"), 10000)
	mt.Put(largeKey, StringValue("value"), 1)

	entry, found := mt.Get(largeKey)
	if !found {
		t.Fatal("large key not found")
	}
	if entry.Value.String() != "value" {
		t.Error("wrong value for large key")
	}
}

package tinykvs

import (
	"fmt"
	"testing"
)

func TestScanPrefix(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Insert keys with different prefixes
	testData := []struct {
		key   string
		value int64
	}{
		{"user:1:name", 1},
		{"user:1:email", 2},
		{"user:2:name", 3},
		{"user:2:email", 4},
		{"order:100", 5},
		{"order:101", 6},
		{"product:abc", 7},
	}

	for _, d := range testData {
		if err := store.PutInt64([]byte(d.key), d.value); err != nil {
			t.Fatalf("PutInt64 failed: %v", err)
		}
	}

	// Test scanning prefix "user:1:"
	var results []string
	err = store.ScanPrefix([]byte("user:1:"), func(key []byte, value Value) bool {
		results = append(results, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results for user:1:, got %d: %v", len(results), results)
	}

	// Test scanning prefix "user:"
	results = nil
	err = store.ScanPrefix([]byte("user:"), func(key []byte, value Value) bool {
		results = append(results, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if len(results) != 4 {
		t.Errorf("expected 4 results for user:, got %d: %v", len(results), results)
	}

	// Test scanning prefix "order:"
	results = nil
	err = store.ScanPrefix([]byte("order:"), func(key []byte, value Value) bool {
		results = append(results, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results for order:, got %d: %v", len(results), results)
	}

	// Test scanning non-existent prefix
	results = nil
	err = store.ScanPrefix([]byte("nonexistent:"), func(key []byte, value Value) bool {
		results = append(results, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for nonexistent:, got %d", len(results))
	}

	// Test early termination
	count := 0
	err = store.ScanPrefix([]byte("user:"), func(key []byte, value Value) bool {
		count++
		return count < 2 // Stop after 2
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if count != 2 {
		t.Errorf("expected callback called 2 times, got %d", count)
	}
}

func TestScanPrefixAcrossLevels(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Small memtable to force flushes

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Insert keys in batches with flushes
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := store.PutInt64([]byte(key), int64(i)); err != nil {
			t.Fatalf("PutInt64 failed: %v", err)
		}
	}
	store.Flush()

	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := store.PutInt64([]byte(key), int64(i)); err != nil {
			t.Fatalf("PutInt64 failed: %v", err)
		}
	}
	store.Flush()

	// Update some keys (creates duplicates across levels)
	for i := 25; i < 75; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := store.PutInt64([]byte(key), int64(i*10)); err != nil {
			t.Fatalf("PutInt64 failed: %v", err)
		}
	}

	// Scan all keys with prefix "key:"
	var results []string
	var values []int64
	err = store.ScanPrefix([]byte("key:"), func(key []byte, value Value) bool {
		results = append(results, string(key))
		values = append(values, value.Int64)
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}

	if len(results) != 100 {
		t.Errorf("expected 100 results, got %d", len(results))
	}

	// Verify keys are sorted
	for i := 1; i < len(results); i++ {
		if results[i] <= results[i-1] {
			t.Errorf("keys not sorted: %s <= %s", results[i], results[i-1])
		}
	}

	// Verify updated values (keys 25-74 should have value*10)
	for i, key := range results {
		var keyNum int
		fmt.Sscanf(key, "key:%d", &keyNum)
		expectedValue := int64(keyNum)
		if keyNum >= 25 && keyNum < 75 {
			expectedValue = int64(keyNum * 10)
		}
		if values[i] != expectedValue {
			t.Errorf("key %s: value = %d, want %d", key, values[i], expectedValue)
		}
	}
}

func TestStoreScanEmptyMemtable(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Scan empty store
	count := 0
	store.ScanPrefix([]byte("any"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestStoreAggregations(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Test with simple numeric values
	store.PutInt64([]byte("num:1"), 10)
	store.PutInt64([]byte("num:2"), 20)
	store.PutInt64([]byte("num:3"), 30)
	store.PutFloat64([]byte("num:4"), 40.5)

	// Count
	count, err := store.Count([]byte("num:"))
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Errorf("Count = %d, want 4", count)
	}

	// Sum (no field = direct value)
	sum, err := store.Sum([]byte("num:"), "")
	if err != nil {
		t.Fatal(err)
	}
	if sum != 100.5 {
		t.Errorf("Sum = %f, want 100.5", sum)
	}

	// Avg
	avg, err := store.Avg([]byte("num:"), "")
	if err != nil {
		t.Fatal(err)
	}
	if avg != 25.125 {
		t.Errorf("Avg = %f, want 25.125", avg)
	}

	// Min
	min, err := store.Min([]byte("num:"), "")
	if err != nil {
		t.Fatal(err)
	}
	if min != 10 {
		t.Errorf("Min = %f, want 10", min)
	}

	// Max
	max, err := store.Max([]byte("num:"), "")
	if err != nil {
		t.Fatal(err)
	}
	if max != 40.5 {
		t.Errorf("Max = %f, want 40.5", max)
	}

	// Aggregate (all at once)
	r, err := store.Aggregate([]byte("num:"), "")
	if err != nil {
		t.Fatal(err)
	}
	if r.Count != 4 || r.Sum != 100.5 || r.Min != 10 || r.Max != 40.5 {
		t.Errorf("Aggregate = %+v", r)
	}
}

func TestStoreAggregationsWithRecords(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Test with records (using PutMap which now uses msgpack)
	store.PutMap([]byte("user:1"), map[string]any{"name": "Alice", "age": 30, "balance": 100.50})
	store.PutMap([]byte("user:2"), map[string]any{"name": "Bob", "age": 25, "balance": 200.00})
	store.PutMap([]byte("user:3"), map[string]any{"name": "Carol", "age": 35, "balance": 150.25})

	// Count
	count, err := store.Count([]byte("user:"))
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}

	// Sum of age field
	sumAge, err := store.Sum([]byte("user:"), "age")
	if err != nil {
		t.Fatal(err)
	}
	if sumAge != 90 {
		t.Errorf("Sum(age) = %f, want 90", sumAge)
	}

	// Avg of balance field
	avgBalance, err := store.Avg([]byte("user:"), "balance")
	if err != nil {
		t.Fatal(err)
	}
	expected := (100.50 + 200.00 + 150.25) / 3
	if avgBalance != expected {
		t.Errorf("Avg(balance) = %f, want %f", avgBalance, expected)
	}

	// Min/Max age
	minAge, _ := store.Min([]byte("user:"), "age")
	maxAge, _ := store.Max([]byte("user:"), "age")
	if minAge != 25 {
		t.Errorf("Min(age) = %f, want 25", minAge)
	}
	if maxAge != 35 {
		t.Errorf("Max(age) = %f, want 35", maxAge)
	}
}

func TestStoreAggregationsNestedFields(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Test with nested fields
	store.PutMap([]byte("order:1"), map[string]any{
		"customer": "Alice",
		"payment":  map[string]any{"amount": 100.0, "tax": 10.0},
	})
	store.PutMap([]byte("order:2"), map[string]any{
		"customer": "Bob",
		"payment":  map[string]any{"amount": 200.0, "tax": 20.0},
	})

	// Sum nested field
	sumAmount, err := store.Sum([]byte("order:"), "payment.amount")
	if err != nil {
		t.Fatal(err)
	}
	if sumAmount != 300 {
		t.Errorf("Sum(payment.amount) = %f, want 300", sumAmount)
	}

	sumTax, err := store.Sum([]byte("order:"), "payment.tax")
	if err != nil {
		t.Fatal(err)
	}
	if sumTax != 30 {
		t.Errorf("Sum(payment.tax) = %f, want 30", sumTax)
	}
}

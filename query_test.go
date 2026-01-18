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
	// Test AggregateResult.Avg() method
	if r.Avg() != 25.125 {
		t.Errorf("Aggregate.Avg() = %f, want 25.125", r.Avg())
	}

	// Test Avg on empty result
	emptyResult, err := store.Aggregate([]byte("nonexistent:"), "")
	if err != nil {
		t.Fatal(err)
	}
	if emptyResult.Avg() != 0 {
		t.Errorf("empty Aggregate.Avg() = %f, want 0", emptyResult.Avg())
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

func TestScanPrefixMaps(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Insert some map records
	store.PutMap([]byte("user:1"), map[string]any{"name": "Alice", "age": 30})
	store.PutMap([]byte("user:2"), map[string]any{"name": "Bob", "age": 25})
	store.PutMap([]byte("user:3"), map[string]any{"name": "Carol", "age": 35})
	store.PutString([]byte("other:1"), "not a map") // should be skipped

	var names []string
	err = store.ScanPrefixMaps([]byte("user:"), func(key []byte, m map[string]any) bool {
		if name, ok := m["name"].(string); ok {
			names = append(names, name)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(names) != 3 {
		t.Errorf("got %d names, want 3", len(names))
	}
}

func TestScanPrefixStructs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	type User struct {
		Name string `msgpack:"name"`
		Age  int    `msgpack:"age"`
	}

	// Insert some struct records
	store.PutStruct([]byte("user:1"), User{Name: "Alice", Age: 30})
	store.PutStruct([]byte("user:2"), User{Name: "Bob", Age: 25})
	store.PutStruct([]byte("user:3"), User{Name: "Carol", Age: 35})

	var users []User
	err = ScanPrefixStructs(store, []byte("user:"), func(key []byte, u *User) bool {
		users = append(users, *u)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(users) != 3 {
		t.Errorf("got %d users, want 3", len(users))
	}
	if users[0].Name != "Alice" {
		t.Errorf("first user = %s, want Alice", users[0].Name)
	}
}

func TestScanRangeMaps(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.PutMap([]byte("key:01"), map[string]any{"v": 1})
	store.PutMap([]byte("key:02"), map[string]any{"v": 2})
	store.PutMap([]byte("key:03"), map[string]any{"v": 3})
	store.PutMap([]byte("key:04"), map[string]any{"v": 4})
	store.PutMap([]byte("key:05"), map[string]any{"v": 5})

	var count int
	err = store.ScanRangeMaps([]byte("key:02"), []byte("key:04"), func(key []byte, m map[string]any) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Errorf("got %d, want 2 (key:02, key:03)", count)
	}
}

func TestScanRangeStructs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	type Item struct {
		ID    int    `msgpack:"id"`
		Value string `msgpack:"value"`
	}

	store.PutStruct([]byte("item:01"), Item{ID: 1, Value: "one"})
	store.PutStruct([]byte("item:02"), Item{ID: 2, Value: "two"})
	store.PutStruct([]byte("item:03"), Item{ID: 3, Value: "three"})
	store.PutStruct([]byte("item:04"), Item{ID: 4, Value: "four"})
	store.PutStruct([]byte("item:05"), Item{ID: 5, Value: "five"})

	var items []Item
	err = ScanRangeStructs(store, []byte("item:02"), []byte("item:05"), func(key []byte, item *Item) bool {
		items = append(items, *item)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 3 {
		t.Errorf("got %d items, want 3 (item:02, item:03, item:04)", len(items))
	}
	if items[0].ID != 2 {
		t.Errorf("first item ID = %d, want 2", items[0].ID)
	}
	if items[2].ID != 4 {
		t.Errorf("last item ID = %d, want 4", items[2].ID)
	}
}

func TestScanRangeJson(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	type Event struct {
		Type string `json:"type"`
		Time int    `json:"time"`
	}

	store.PutJson([]byte("event:100"), Event{Type: "click", Time: 100})
	store.PutJson([]byte("event:200"), Event{Type: "view", Time: 200})
	store.PutJson([]byte("event:300"), Event{Type: "submit", Time: 300})
	store.PutJson([]byte("event:400"), Event{Type: "load", Time: 400})

	var events []Event
	err = ScanRangeJson(store, []byte("event:200"), []byte("event:400"), func(key []byte, e *Event) bool {
		events = append(events, *e)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 2 {
		t.Errorf("got %d events, want 2 (event:200, event:300)", len(events))
	}
	if events[0].Type != "view" {
		t.Errorf("first event type = %s, want view", events[0].Type)
	}
}

func TestScanPrefixJson(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	type Product struct {
		Name  string  `json:"name"`
		Price float64 `json:"price"`
	}

	store.PutJson([]byte("product:1"), Product{Name: "Widget", Price: 9.99})
	store.PutJson([]byte("product:2"), Product{Name: "Gadget", Price: 19.99})

	var products []Product
	err = ScanPrefixJson(store, []byte("product:"), func(key []byte, p *Product) bool {
		products = append(products, *p)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(products) != 2 {
		t.Errorf("got %d products, want 2", len(products))
	}
	if products[0].Name != "Widget" {
		t.Errorf("first product = %s, want Widget", products[0].Name)
	}
}

// TestAggregateWithNestedFields tests aggregations on nested msgpack fields.
func TestAggregateWithNestedFields(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Insert records with nested fields using msgpack
	type Stats struct {
		Views    int     `msgpack:"views"`
		Clicks   int     `msgpack:"clicks"`
		Revenue  float64 `msgpack:"revenue"`
		Metadata struct {
			Score int `msgpack:"score"`
		} `msgpack:"metadata"`
	}

	for i := 0; i < 10; i++ {
		s := Stats{
			Views:   100 + i*10,
			Clicks:  10 + i,
			Revenue: float64(i) * 5.5,
		}
		s.Metadata.Score = i * 2
		key := fmt.Sprintf("stats:%03d", i)
		if err := store.PutStruct([]byte(key), s); err != nil {
			t.Fatalf("PutStruct failed: %v", err)
		}
	}

	// Test Sum on nested field using store.Sum
	sumViews, err := store.Sum([]byte("stats:"), "views")
	if err != nil {
		t.Fatalf("Sum failed: %v", err)
	}
	// Sum of 100, 110, 120, ..., 190 = 1450
	if sumViews != 1450 {
		t.Errorf("Sum(views) = %v, want 1450", sumViews)
	}

	// Test Avg on float field using store.Avg
	avgRevenue, err := store.Avg([]byte("stats:"), "revenue")
	if err != nil {
		t.Fatalf("Avg failed: %v", err)
	}
	// Avg of 0, 5.5, 11, 16.5, ..., 49.5 = 24.75
	if avgRevenue < 24.7 || avgRevenue > 24.8 {
		t.Errorf("Avg(revenue) = %v, want ~24.75", avgRevenue)
	}

	// Test Count using store.Count
	count, err := store.Count([]byte("stats:"))
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 10 {
		t.Errorf("Count() = %d, want 10", count)
	}

	// Test Min and Max on views field
	minViews, err := store.Min([]byte("stats:"), "views")
	if err != nil {
		t.Fatalf("Min failed: %v", err)
	}
	if minViews != 100 {
		t.Errorf("Min(views) = %v, want 100", minViews)
	}

	maxViews, err := store.Max([]byte("stats:"), "views")
	if err != nil {
		t.Fatalf("Max failed: %v", err)
	}
	if maxViews != 190 {
		t.Errorf("Max(views) = %v, want 190", maxViews)
	}

	// Test Aggregate (all at once) on views field
	result, err := store.Aggregate([]byte("stats:"), "views")
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}
	if result.Sum != 1450 {
		t.Errorf("Aggregate.Sum = %v, want 1450", result.Sum)
	}
	if result.Count != 10 {
		t.Errorf("Aggregate.Count = %d, want 10", result.Count)
	}
	if result.Min != 100 {
		t.Errorf("Aggregate.Min = %v, want 100", result.Min)
	}
	if result.Max != 190 {
		t.Errorf("Aggregate.Max = %v, want 190", result.Max)
	}

	// Test nested field extraction (metadata.score)
	sumScore, err := store.Sum([]byte("stats:"), "metadata.score")
	if err != nil {
		t.Fatalf("Sum on nested field failed: %v", err)
	}
	// Sum of 0, 2, 4, 6, 8, 10, 12, 14, 16, 18 = 90
	if sumScore != 90 {
		t.Errorf("Sum(metadata.score) = %v, want 90", sumScore)
	}
}

// TestAggregateWithDirectValues tests aggregations on direct int64/float64 values.
func TestAggregateWithDirectValues(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Insert direct numeric values
	for i := 1; i <= 5; i++ {
		store.PutInt64([]byte(fmt.Sprintf("int:%d", i)), int64(i*10))
		store.PutFloat64([]byte(fmt.Sprintf("float:%d", i)), float64(i)*2.5)
	}

	// Test Sum on int64 values (empty field = direct value)
	sumInt, err := store.Sum([]byte("int:"), "")
	if err != nil {
		t.Fatalf("Sum on int64 failed: %v", err)
	}
	// Sum of 10, 20, 30, 40, 50 = 150
	if sumInt != 150 {
		t.Errorf("Sum() on int64 = %v, want 150", sumInt)
	}

	// Test Avg on float64 values
	avgFloat, err := store.Avg([]byte("float:"), "")
	if err != nil {
		t.Fatalf("Avg on float64 failed: %v", err)
	}
	// Avg of 2.5, 5, 7.5, 10, 12.5 = 7.5
	if avgFloat != 7.5 {
		t.Errorf("Avg() on float64 = %v, want 7.5", avgFloat)
	}

	// Test Aggregate on int64 values (all at once)
	result, err := store.Aggregate([]byte("int:"), "")
	if err != nil {
		t.Fatalf("Aggregate on int64 failed: %v", err)
	}
	if result.Sum != 150 {
		t.Errorf("Aggregate.Sum = %v, want 150", result.Sum)
	}
	if result.Count != 5 {
		t.Errorf("Aggregate.Count = %d, want 5", result.Count)
	}
	if result.Min != 10 {
		t.Errorf("Aggregate.Min = %v, want 10", result.Min)
	}
	if result.Max != 50 {
		t.Errorf("Aggregate.Max = %v, want 50", result.Max)
	}
	expectedAvg := 30.0 // (10+20+30+40+50)/5
	if result.Avg() != expectedAvg {
		t.Errorf("Aggregate.Avg() = %v, want %v", result.Avg(), expectedAvg)
	}

	// Test Min/Max on float64
	minFloat, err := store.Min([]byte("float:"), "")
	if err != nil {
		t.Fatalf("Min on float64 failed: %v", err)
	}
	if minFloat != 2.5 {
		t.Errorf("Min() on float64 = %v, want 2.5", minFloat)
	}

	maxFloat, err := store.Max([]byte("float:"), "")
	if err != nil {
		t.Fatalf("Max on float64 failed: %v", err)
	}
	if maxFloat != 12.5 {
		t.Errorf("Max() on float64 = %v, want 12.5", maxFloat)
	}
}

// TestAggregateEdgeCases tests edge cases for aggregation functions.
func TestAggregateEdgeCases(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Insert non-numeric values (strings, bools)
	store.Put([]byte("str:1"), Value{Type: ValueTypeString, Bytes: []byte("hello")})
	store.Put([]byte("str:2"), Value{Type: ValueTypeBool, Bool: true})

	// Sum/Avg on non-numeric types should return 0
	sumStr, err := store.Sum([]byte("str:"), "")
	if err != nil {
		t.Fatalf("Sum on strings failed: %v", err)
	}
	if sumStr != 0 {
		t.Errorf("Sum() on non-numeric = %v, want 0", sumStr)
	}

	// Count should still work on non-numeric
	count, err := store.Count([]byte("str:"))
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Count() = %d, want 2", count)
	}

	// Insert records with different numeric types
	// Record type test
	store.Put([]byte("rec:1"), Value{
		Type:   ValueTypeRecord,
		Record: map[string]any{"value": int(42), "name": "test"},
	})

	// Aggregation on Record type
	sumRec, err := store.Sum([]byte("rec:"), "value")
	if err != nil {
		t.Fatalf("Sum on record failed: %v", err)
	}
	if sumRec != 42 {
		t.Errorf("Sum(value) on record = %v, want 42", sumRec)
	}

	// Test aggregation on non-existent field
	sumMissing, err := store.Sum([]byte("rec:"), "nonexistent")
	if err != nil {
		t.Fatalf("Sum on missing field failed: %v", err)
	}
	if sumMissing != 0 {
		t.Errorf("Sum(nonexistent) = %v, want 0", sumMissing)
	}

	// Test aggregation on non-numeric field in record
	sumName, err := store.Sum([]byte("rec:"), "name")
	if err != nil {
		t.Fatalf("Sum on non-numeric field failed: %v", err)
	}
	if sumName != 0 {
		t.Errorf("Sum(name) = %v, want 0", sumName)
	}

	// Test Min/Max on empty prefix
	minEmpty, err := store.Min([]byte("empty:"), "")
	if err != nil {
		t.Fatalf("Min on empty failed: %v", err)
	}
	if minEmpty != 0 {
		t.Errorf("Min() on empty = %v, want 0", minEmpty)
	}

	maxEmpty, err := store.Max([]byte("empty:"), "")
	if err != nil {
		t.Fatalf("Max on empty failed: %v", err)
	}
	if maxEmpty != 0 {
		t.Errorf("Max() on empty = %v, want 0", maxEmpty)
	}
}

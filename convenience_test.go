package tinykvs

import (
	"reflect"
	"testing"
)

func TestStructToMap(t *testing.T) {
	type Address struct {
		City    string `json:"city"`
		Country string `json:"country"`
	}

	type User struct {
		Name    string  `msgpack:"name"`
		Email   string  `json:"email"`
		Age     int     // No tag, use field name
		Ignored string  `msgpack:"-"`
		Private string  `msgpack:"private,omitempty"` // has options
		Addr    Address `msgpack:"address"`
	}

	user := User{
		Name:    "Alice",
		Email:   "alice@example.com",
		Age:     30,
		Ignored: "should not appear",
		Private: "secret",
		Addr:    Address{City: "NYC", Country: "USA"},
	}

	m, err := structToMap(user)
	if err != nil {
		t.Fatalf("structToMap failed: %v", err)
	}

	// Check fields
	if m["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", m["name"])
	}
	if m["email"] != "alice@example.com" {
		t.Errorf("email = %v, want alice@example.com", m["email"])
	}
	if m["Age"] != 30 {
		t.Errorf("Age = %v, want 30", m["Age"])
	}
	if m["private"] != "secret" {
		t.Errorf("private = %v, want secret", m["private"])
	}

	// Check nested struct
	addr, ok := m["address"].(map[string]any)
	if !ok {
		t.Fatalf("address is not a map: %T", m["address"])
	}
	if addr["city"] != "NYC" {
		t.Errorf("address.city = %v, want NYC", addr["city"])
	}

	// Test with pointer
	m2, err := structToMap(&user)
	if err != nil {
		t.Fatalf("structToMap with pointer failed: %v", err)
	}
	if m2["name"] != "Alice" {
		t.Errorf("pointer: name = %v, want Alice", m2["name"])
	}

	// Test with non-struct
	_, err = structToMap("not a struct")
	if err == nil {
		t.Error("structToMap with string should fail")
	}
}

func TestConvertToAny(t *testing.T) {
	// Test nil pointer
	var nilPtr *string
	result := convertToAny(reflect.ValueOf(nilPtr))
	if result != nil {
		t.Errorf("nil pointer: got %v, want nil", result)
	}

	// Test nil slice
	var nilSlice []int
	result = convertToAny(reflect.ValueOf(nilSlice))
	if result != nil {
		t.Errorf("nil slice: got %v, want nil", result)
	}

	// Test nil map
	var nilMap map[string]int
	result = convertToAny(reflect.ValueOf(nilMap))
	if result != nil {
		t.Errorf("nil map: got %v, want nil", result)
	}

	// Test slice with values
	slice := []int{1, 2, 3}
	result = convertToAny(reflect.ValueOf(slice))
	resultSlice, ok := result.([]any)
	if !ok {
		t.Fatalf("slice result is not []any: %T", result)
	}
	if len(resultSlice) != 3 {
		t.Errorf("slice length = %d, want 3", len(resultSlice))
	}

	// Test map with values
	m := map[string]int{"a": 1, "b": 2}
	result = convertToAny(reflect.ValueOf(m))
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("map result is not map[string]any: %T", result)
	}
	if len(resultMap) != 2 {
		t.Errorf("map length = %d, want 2", len(resultMap))
	}

	// Test invalid reflect.Value
	var invalid reflect.Value
	result = convertToAny(invalid)
	if result != nil {
		t.Errorf("invalid: got %v, want nil", result)
	}
}

func TestMapToStruct(t *testing.T) {
	type Address struct {
		City    string `json:"city"`
		Country string `json:"country"`
	}

	type User struct {
		Name  string  `msgpack:"name"`
		Email string  `json:"email"`
		Age   int     // No tag
		Score float64 `msgpack:"score"`
		Addr  Address `msgpack:"address"`
	}

	m := map[string]any{
		"name":  "Bob",
		"email": "bob@example.com",
		"Age":   25,
		"score": 95.5,
		"address": map[string]any{
			"city":    "LA",
			"country": "USA",
		},
	}

	var user User
	if err := mapToStruct(m, &user); err != nil {
		t.Fatalf("mapToStruct failed: %v", err)
	}

	if user.Name != "Bob" {
		t.Errorf("Name = %v, want Bob", user.Name)
	}
	if user.Email != "bob@example.com" {
		t.Errorf("Email = %v, want bob@example.com", user.Email)
	}
	if user.Age != 25 {
		t.Errorf("Age = %v, want 25", user.Age)
	}
	if user.Score != 95.5 {
		t.Errorf("Score = %v, want 95.5", user.Score)
	}
	if user.Addr.City != "LA" {
		t.Errorf("Addr.City = %v, want LA", user.Addr.City)
	}

	// Test errors
	var notPtr User
	if mapToStruct(m, notPtr) == nil {
		t.Error("mapToStruct with non-pointer should fail")
	}

	var nilDest *User
	if mapToStruct(m, nilDest) == nil {
		t.Error("mapToStruct with nil pointer should fail")
	}

	var notStruct string
	if mapToStruct(m, &notStruct) == nil {
		t.Error("mapToStruct with non-struct should fail")
	}
}

func TestSetFieldValue(t *testing.T) {
	// Test nil value
	var i int
	rv := reflect.ValueOf(&i).Elem()
	if err := setFieldValue(rv, nil); err != nil {
		t.Errorf("setFieldValue with nil: %v", err)
	}

	// Test float64 to int conversion
	var intVal int
	rv = reflect.ValueOf(&intVal).Elem()
	if err := setFieldValue(rv, float64(42.0)); err != nil {
		t.Errorf("setFieldValue float64 to int: %v", err)
	}
	if intVal != 42 {
		t.Errorf("int value = %d, want 42", intVal)
	}

	// Test int to int64 conversion
	var int64Val int64
	rv = reflect.ValueOf(&int64Val).Elem()
	if err := setFieldValue(rv, int(100)); err != nil {
		t.Errorf("setFieldValue int to int64: %v", err)
	}
	if int64Val != 100 {
		t.Errorf("int64 value = %d, want 100", int64Val)
	}

	// Test int64 to float64 conversion
	var floatVal float64
	rv = reflect.ValueOf(&floatVal).Elem()
	if err := setFieldValue(rv, int64(50)); err != nil {
		t.Errorf("setFieldValue int64 to float64: %v", err)
	}
	if floatVal != 50.0 {
		t.Errorf("float64 value = %f, want 50.0", floatVal)
	}

	// Test int to float conversion
	rv = reflect.ValueOf(&floatVal).Elem()
	if err := setFieldValue(rv, int(75)); err != nil {
		t.Errorf("setFieldValue int to float64: %v", err)
	}
	if floatVal != 75.0 {
		t.Errorf("float64 value = %f, want 75.0", floatVal)
	}

	// Test bool
	var boolVal bool
	rv = reflect.ValueOf(&boolVal).Elem()
	if err := setFieldValue(rv, true); err != nil {
		t.Errorf("setFieldValue bool: %v", err)
	}
	if !boolVal {
		t.Error("bool value should be true")
	}

	// Test string
	var strVal string
	rv = reflect.ValueOf(&strVal).Elem()
	if err := setFieldValue(rv, "hello"); err != nil {
		t.Errorf("setFieldValue string: %v", err)
	}
	if strVal != "hello" {
		t.Errorf("string value = %v, want hello", strVal)
	}

	// Test slice
	var sliceVal []int
	rv = reflect.ValueOf(&sliceVal).Elem()
	if err := setFieldValue(rv, []any{int(1), int(2), int(3)}); err != nil {
		t.Errorf("setFieldValue slice: %v", err)
	}
	if len(sliceVal) != 3 {
		t.Errorf("slice length = %d, want 3", len(sliceVal))
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a, b     Value
		expected bool
	}{
		{"int64 equal", Int64Value(42), Int64Value(42), true},
		{"int64 not equal", Int64Value(42), Int64Value(43), false},
		{"float64 equal", Float64Value(3.14), Float64Value(3.14), true},
		{"float64 not equal", Float64Value(3.14), Float64Value(2.71), false},
		{"bool equal", BoolValue(true), BoolValue(true), true},
		{"bool not equal", BoolValue(true), BoolValue(false), false},
		{"string equal", StringValue("hello"), StringValue("hello"), true},
		{"string not equal", StringValue("hello"), StringValue("world"), false},
		{"bytes equal", BytesValue([]byte{1, 2, 3}), BytesValue([]byte{1, 2, 3}), true},
		{"bytes not equal", BytesValue([]byte{1, 2, 3}), BytesValue([]byte{1, 2, 4}), false},
		{"type mismatch", Int64Value(42), StringValue("42"), false},
		{"tombstone equal", Value{Type: ValueTypeTombstone}, Value{Type: ValueTypeTombstone}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valuesEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestSetNumericOrPrimitive(t *testing.T) {
	// Test int field with float64
	var intVal int
	rv := reflect.ValueOf(&intVal).Elem()
	setNumericOrPrimitive(rv, float64(42))
	if intVal != 42 {
		t.Errorf("int from float64: got %d, want 42", intVal)
	}

	// Test int field with int64
	setNumericOrPrimitive(rv, int64(100))
	if intVal != 100 {
		t.Errorf("int from int64: got %d, want 100", intVal)
	}

	// Test int field with int
	setNumericOrPrimitive(rv, int(200))
	if intVal != 200 {
		t.Errorf("int from int: got %d, want 200", intVal)
	}

	// Test float field with float64
	var floatVal float64
	rv = reflect.ValueOf(&floatVal).Elem()
	setNumericOrPrimitive(rv, float64(3.14))
	if floatVal != 3.14 {
		t.Errorf("float from float64: got %f, want 3.14", floatVal)
	}

	// Test float field with int64
	setNumericOrPrimitive(rv, int64(50))
	if floatVal != 50.0 {
		t.Errorf("float from int64: got %f, want 50.0", floatVal)
	}

	// Test float field with int
	setNumericOrPrimitive(rv, int(75))
	if floatVal != 75.0 {
		t.Errorf("float from int: got %f, want 75.0", floatVal)
	}

	// Test bool field
	var boolVal bool
	rv = reflect.ValueOf(&boolVal).Elem()
	setNumericOrPrimitive(rv, true)
	if !boolVal {
		t.Error("bool should be true")
	}

	// Test string field
	var strVal string
	rv = reflect.ValueOf(&strVal).Elem()
	setNumericOrPrimitive(rv, "hello")
	if strVal != "hello" {
		t.Errorf("string: got %q, want hello", strVal)
	}
}

func TestGetStruct(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type User struct {
		Name  string `msgpack:"name"`
		Email string `msgpack:"email"`
		Age   int    `msgpack:"age"`
	}

	// Store a struct
	user := User{Name: "Alice", Email: "alice@example.com", Age: 30}
	if err := PutStruct(store, []byte("user:1"), &user); err != nil {
		t.Fatalf("PutStruct failed: %v", err)
	}

	// Retrieve using GetStruct
	got, err := GetStruct[User](store, []byte("user:1"))
	if err != nil {
		t.Fatalf("GetStruct failed: %v", err)
	}
	if got.Name != "Alice" || got.Email != "alice@example.com" || got.Age != 30 {
		t.Errorf("got %+v, want {Alice alice@example.com 30}", got)
	}

	// Test with non-existent key
	_, err = GetStruct[User](store, []byte("nonexistent"))
	if err == nil {
		t.Error("GetStruct with nonexistent key should fail")
	}

	// Test with wrong type
	store.PutString([]byte("str"), "not a struct")
	_, err = GetStruct[User](store, []byte("str"))
	if err == nil {
		t.Error("GetStruct on string value should fail")
	}
}

func TestGetMapZeroCopy(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Store a map as msgpack
	data := map[string]any{"name": "Alice", "age": int64(30)}
	if err := store.PutMap([]byte("user:1"), data); err != nil {
		t.Fatalf("PutMap failed: %v", err)
	}

	// Retrieve with zero-copy
	var gotName string
	err = store.GetMapZeroCopy([]byte("user:1"), func(m map[string]any) error {
		gotName = m["name"].(string)
		return nil
	})
	if err != nil {
		t.Fatalf("GetMapZeroCopy failed: %v", err)
	}
	if gotName != "Alice" {
		t.Errorf("name = %q, want Alice", gotName)
	}

	// Test with wrong type
	store.PutString([]byte("str"), "not a map")
	err = store.GetMapZeroCopy([]byte("str"), func(m map[string]any) error {
		return nil
	})
	if err == nil {
		t.Error("GetMapZeroCopy on string should fail")
	}
}

func TestGetStructZeroCopy(t *testing.T) {
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

	user := User{Name: "Bob", Age: 25}
	if err := PutStruct(store, []byte("user:1"), &user); err != nil {
		t.Fatalf("PutStruct failed: %v", err)
	}

	var gotName string
	err = GetStructZeroCopy(store, []byte("user:1"), func(u *User) error {
		gotName = u.Name
		return nil
	})
	if err != nil {
		t.Fatalf("GetStructZeroCopy failed: %v", err)
	}
	if gotName != "Bob" {
		t.Errorf("name = %q, want Bob", gotName)
	}

	// Test with wrong type
	store.PutString([]byte("str"), "not a struct")
	err = GetStructZeroCopy(store, []byte("str"), func(u *User) error {
		return nil
	})
	if err == nil {
		t.Error("GetStructZeroCopy on string should fail")
	}
}

func TestScanPrefixMapsZeroCopy(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Store some maps
	for i := 0; i < 5; i++ {
		data := map[string]any{"id": int64(i), "name": "user"}
		if err := store.PutMap([]byte("user:"+string(rune('0'+i))), data); err != nil {
			t.Fatalf("PutMap failed: %v", err)
		}
	}

	// Also store a string to test skipping
	store.PutString([]byte("user:str"), "not a map")

	count := 0
	err = store.ScanPrefixMapsZeroCopy([]byte("user:"), func(key []byte, m map[string]any) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefixMapsZeroCopy failed: %v", err)
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

func TestScanPrefixStructsZeroCopy(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type Item struct {
		ID   int    `msgpack:"id"`
		Name string `msgpack:"name"`
	}

	// Store some structs
	for i := 0; i < 5; i++ {
		item := Item{ID: i, Name: "item"}
		if err := PutStruct(store, []byte("item:"+string(rune('0'+i))), &item); err != nil {
			t.Fatalf("PutStruct failed: %v", err)
		}
	}

	// Also store a string to test skipping
	store.PutString([]byte("item:str"), "not a struct")

	count := 0
	err = ScanPrefixStructsZeroCopy(store, []byte("item:"), func(key []byte, item *Item) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefixStructsZeroCopy failed: %v", err)
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

func TestScanRangeStructsZeroCopy(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type Item struct {
		ID   int    `msgpack:"id"`
		Name string `msgpack:"name"`
	}

	// Store some structs
	for i := 0; i < 10; i++ {
		item := Item{ID: i, Name: "item"}
		key := []byte("item:" + string(rune('0'+i)))
		if err := PutStruct(store, key, &item); err != nil {
			t.Fatalf("PutStruct failed: %v", err)
		}
	}

	// Also store a string in range to test skipping
	store.PutString([]byte("item:str"), "not a struct")

	count := 0
	err = ScanRangeStructsZeroCopy(store, []byte("item:3"), []byte("item:7"), func(key []byte, item *Item) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanRangeStructsZeroCopy failed: %v", err)
	}
	if count != 4 {
		t.Errorf("count = %d, want 4 (items 3,4,5,6)", count)
	}
}

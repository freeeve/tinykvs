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
	if err := mapToStruct(m, notPtr); err == nil {
		t.Error("mapToStruct with non-pointer should fail")
	}

	var nilDest *User
	if err := mapToStruct(m, nilDest); err == nil {
		t.Error("mapToStruct with nil pointer should fail")
	}

	var notStruct string
	if err := mapToStruct(m, &notStruct); err == nil {
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

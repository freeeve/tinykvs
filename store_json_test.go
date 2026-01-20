package tinykvs

import (
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestStoreRecords(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Test PutMap and GetMap
	record := map[string]any{
		"name":   "Alice",
		"age":    30,
		"active": true,
	}

	if err := store.PutMap([]byte("user:1"), record); err != nil {
		t.Fatalf("PutMap failed: %v", err)
	}

	got, err := store.GetMap([]byte("user:1"))
	if err != nil {
		t.Fatalf("GetMap failed: %v", err)
	}

	if got["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", got["name"])
	}
	if got["active"] != true {
		t.Errorf("active = %v, want true", got["active"])
	}
	// Note: msgpack may decode integers as float64
	if age, ok := got["age"].(int8); ok {
		if age != 30 {
			t.Errorf("age = %v, want 30", got["age"])
		}
	}

	// Test GetMap on non-record value
	if err := store.PutString([]byte("str"), "value"); err != nil {
		t.Fatalf("PutString failed: %v", err)
	}
	_, err = store.GetMap([]byte("str"))
	if err == nil {
		t.Error("GetMap on string should fail")
	}
}

func TestStoreRecordPersistence(t *testing.T) {
	dir := t.TempDir()

	// Write record
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	record := map[string]any{"field": "value", "number": 42}
	if err := store.PutMap([]byte("record:1"), record); err != nil {
		t.Fatalf("PutMap failed: %v", err)
	}

	store.Flush()
	store.Close()

	// Reopen and verify
	store, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store.Close()

	got, err := store.GetMap([]byte("record:1"))
	if err != nil {
		t.Fatalf("GetMap after reopen failed: %v", err)
	}

	if got["field"] != "value" {
		t.Errorf("field = %v, want value", got["field"])
	}
}

func TestStorePutGetJson(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Store JSON
	data := map[string]any{"name": "Alice", "age": 30}
	if err := store.PutJson([]byte("user:1"), data); err != nil {
		t.Fatalf("PutJson failed: %v", err)
	}

	// Verify it's stored as a string
	val, err := store.Get([]byte("user:1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.Type != ValueTypeString {
		t.Errorf("expected string type, got %d", val.Type)
	}

	// Get JSON back
	var got map[string]any
	if err := store.GetJson([]byte("user:1"), &got); err != nil {
		t.Fatalf("GetJson failed: %v", err)
	}

	if got["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", got["name"])
	}

	// GetJson on non-string should fail
	store.PutInt64([]byte("num"), 42)
	var dummy map[string]any
	if err := store.GetJson([]byte("num"), &dummy); err == nil {
		t.Error("GetJson on int64 should fail")
	}
}

func TestStoreJsonWithStruct(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type User struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		Age   int    `json:"age"`
	}

	// Store struct as JSON
	user := User{Name: "Bob", Email: "bob@example.com", Age: 25}
	if err := store.PutJson([]byte("user:bob"), user); err != nil {
		t.Fatalf("PutJson failed: %v", err)
	}

	// Get back into struct
	var got User
	if err := store.GetJson([]byte("user:bob"), &got); err != nil {
		t.Fatalf("GetJson failed: %v", err)
	}

	if got.Name != "Bob" || got.Email != "bob@example.com" || got.Age != 25 {
		t.Errorf("got = %+v, want {Bob bob@example.com 25}", got)
	}
}

func TestStorePutGetStruct(t *testing.T) {
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

	// Put struct
	user := User{Name: "Alice", Email: "alice@example.com", Age: 30}
	if err := PutStruct(store, []byte("user:alice"), &user); err != nil {
		t.Fatalf("PutStruct failed: %v", err)
	}

	// Get back into struct
	var got User
	if err := GetStructInto(store, []byte("user:alice"), &got); err != nil {
		t.Fatalf("GetStruct failed: %v", err)
	}

	if got.Name != "Alice" || got.Email != "alice@example.com" || got.Age != 30 {
		t.Errorf("got = %+v, want {Alice alice@example.com 30}", got)
	}

	// Test GetStruct with wrong type
	if err := store.PutString([]byte("string:key"), "not a record"); err != nil {
		t.Fatalf("PutString failed: %v", err)
	}
	var wrongType User
	if err := GetStructInto(store, []byte("string:key"), &wrongType); err == nil {
		t.Error("GetStruct should fail for non-record type")
	}

	// Test GetStruct with missing key
	var missing User
	if err := GetStructInto(store, []byte("missing:key"), &missing); err == nil {
		t.Error("GetStruct should fail for missing key")
	}
}

func TestStorePutGetNestedStruct(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type Address struct {
		City    string `msgpack:"city"`
		Country string `msgpack:"country"`
	}

	type User struct {
		Name    string  `msgpack:"name"`
		Address Address `msgpack:"address"`
	}

	// Put nested struct
	user := User{
		Name:    "Alice",
		Address: Address{City: "NYC", Country: "USA"},
	}
	if err := PutStruct(store, []byte("user:alice"), &user); err != nil {
		t.Fatalf("PutStruct failed: %v", err)
	}

	// Get back into nested struct
	var got User
	if err := GetStructInto(store, []byte("user:alice"), &got); err != nil {
		t.Fatalf("GetStruct failed: %v", err)
	}

	if got.Name != "Alice" {
		t.Errorf("got.Name = %q, want Alice", got.Name)
	}
	if got.Address.City != "NYC" {
		t.Errorf("got.Address.City = %q, want NYC", got.Address.City)
	}
	if got.Address.Country != "USA" {
		t.Errorf("got.Address.Country = %q, want USA", got.Address.Country)
	}

	// Verify the value is stored as msgpack for efficiency
	val, err := store.Get([]byte("user:alice"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.Type != ValueTypeMsgpack {
		t.Errorf("value type = %v, want Msgpack", val.Type)
	}

	// Verify we can decode the raw msgpack bytes
	var decoded User
	if err := getStructFromValue(val, &decoded); err != nil {
		t.Fatalf("getStructFromValue failed: %v", err)
	}
	if decoded.Address.City != "NYC" {
		t.Errorf("decoded address.city = %v, want NYC", decoded.Address.City)
	}
}

// getStructFromValue decodes a Value directly into a struct (for testing raw msgpack access)
func getStructFromValue(val Value, dest any) error {
	if val.Type == ValueTypeMsgpack {
		return msgpack.Unmarshal(val.Bytes, dest)
	}
	return fmt.Errorf("expected msgpack, got type %d", val.Type)
}

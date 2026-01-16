package tinykvs

import (
	"fmt"
	"testing"
)

type benchUser struct {
	Name    string  `msgpack:"name"`
	Email   string  `msgpack:"email"`
	Age     int     `msgpack:"age"`
	Active  bool    `msgpack:"active"`
	Balance float64 `msgpack:"balance"`
}

func BenchmarkStorePut(b *testing.B) {
	dir := b.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	value := StringValue("benchmark value that is reasonably sized")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.Put([]byte(key), value)
	}
}

func BenchmarkStoreGet(b *testing.B) {
	dir := b.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-populate
	n := 10000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}
	store.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%n)
		store.Get([]byte(key))
	}
}

func BenchmarkStoreMixed(b *testing.B) {
	dir := b.TempDir()

	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d", i)
		store.PutString([]byte(key), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% writes
			key := fmt.Sprintf("key%08d", i)
			store.PutString([]byte(key), "new value")
		} else {
			// 80% reads
			key := fmt.Sprintf("key%08d", i%1000)
			store.Get([]byte(key))
		}
	}
}

func BenchmarkCompaction(b *testing.B) {
	for _, numKeys := range []int{10000, 50000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				opts := DefaultOptions(dir)
				opts.MemtableSize = 64 * 1024 // 64KB to create multiple L0 tables

				store, err := Open(dir, opts)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}

				// Write keys to create multiple L0 tables
				for j := 0; j < numKeys; j++ {
					key := fmt.Sprintf("key%08d", j)
					store.PutString([]byte(key), "value that is long enough to fill blocks quickly")
				}
				store.Flush()

				b.StartTimer()
				store.Compact()
				b.StopTimer()

				store.Close()
			}
		})
	}
}

func BenchmarkPutStruct(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	user := benchUser{
		Name:    "Alice Smith",
		Email:   "alice@example.com",
		Age:     30,
		Active:  true,
		Balance: 1234.56,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user:%08d", i)
		store.PutStruct([]byte(key), user)
	}
}

func BenchmarkGetStruct(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-populate
	n := 10000
	user := benchUser{
		Name:    "Alice Smith",
		Email:   "alice@example.com",
		Age:     30,
		Active:  true,
		Balance: 1234.56,
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user:%08d", i)
		store.PutStruct([]byte(key), user)
	}
	store.Flush()

	var dest benchUser
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user:%08d", i%n)
		store.GetStruct([]byte(key), &dest)
	}
}

func BenchmarkPutMap(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	record := map[string]any{
		"name":    "Alice Smith",
		"email":   "alice@example.com",
		"age":     30,
		"active":  true,
		"balance": 1234.56,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user:%08d", i)
		store.PutMap([]byte(key), record)
	}
}

func BenchmarkGetMap(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Pre-populate
	n := 10000
	record := map[string]any{
		"name":    "Alice Smith",
		"email":   "alice@example.com",
		"age":     30,
		"active":  true,
		"balance": 1234.56,
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user:%08d", i)
		store.PutMap([]byte(key), record)
	}
	store.Flush()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user:%08d", i%n)
		store.GetMap([]byte(key))
	}
}

func BenchmarkPutJson(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type User struct {
		Name    string  `json:"name"`
		Email   string  `json:"email"`
		Age     int     `json:"age"`
		Active  bool    `json:"active"`
		Balance float64 `json:"balance"`
	}

	user := User{
		Name:    "Alice Smith",
		Email:   "alice@example.com",
		Age:     30,
		Active:  true,
		Balance: 1234.56,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user:%08d", i)
		store.PutJson([]byte(key), user)
	}
}

func BenchmarkGetJson(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	type User struct {
		Name    string  `json:"name"`
		Email   string  `json:"email"`
		Age     int     `json:"age"`
		Active  bool    `json:"active"`
		Balance float64 `json:"balance"`
	}

	// Pre-populate
	n := 10000
	user := User{
		Name:    "Alice Smith",
		Email:   "alice@example.com",
		Age:     30,
		Active:  true,
		Balance: 1234.56,
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user:%08d", i)
		store.PutJson([]byte(key), user)
	}
	store.Flush()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user:%08d", i%n)
		var got User
		store.GetJson([]byte(key), &got)
	}
}

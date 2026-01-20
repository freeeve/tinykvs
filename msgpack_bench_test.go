package tinykvs

import (
	"testing"

	"github.com/freeeve/msgpck"
)

type SmallStruct struct {
	Name string
	Age  int
}

type MediumStruct struct {
	ID       int64
	Name     string
	Email    string
	Age      int
	Active   bool
	Score    float64
	Tags     []string
	Metadata map[string]string
}

var (
	smallStruct  = SmallStruct{Name: "Alice", Age: 30}
	mediumStruct = MediumStruct{
		ID: 12345, Name: "Bob Smith", Email: "bob@example.com",
		Age: 42, Active: true, Score: 98.6,
		Tags:     []string{"admin", "user", "premium"},
		Metadata: map[string]string{"role": "manager", "dept": "engineering"},
	}
	smallMap  = map[string]any{"name": "Alice", "age": 30}
	mediumMap = map[string]any{
		"id": int64(12345), "name": "Bob Smith", "email": "bob@example.com",
		"age": 42, "active": true, "score": 98.6,
		"tags":     []any{"admin", "user", "premium"},
		"metadata": map[string]any{"role": "manager", "dept": "engineering"},
	}
)

// Map benchmarks
func BenchmarkMsgpck_SmallMap_Marshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msgpck.Marshal(smallMap)
	}
}
func BenchmarkMsgpck_SmallMap_Unmarshal(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.Unmarshal(data)
	}
}
func BenchmarkMsgpck_MediumMap_Marshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msgpck.Marshal(mediumMap)
	}
}
func BenchmarkMsgpck_MediumMap_Unmarshal(b *testing.B) {
	data, _ := msgpck.MarshalCopy(mediumMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.Unmarshal(data)
	}
}

// MarshalCopy (safe to retain) benchmarks
func BenchmarkMsgpck_SmallMap_MarshalCopy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msgpck.MarshalCopy(smallMap)
	}
}
func BenchmarkMsgpck_MediumMap_MarshalCopy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msgpck.MarshalCopy(mediumMap)
	}
}

// Struct serialization benchmarks
func BenchmarkMsgpck_SmallStruct_Marshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msgpck.Marshal(smallStruct)
	}
}
func BenchmarkMsgpck_SmallStruct_Unmarshal(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s SmallStruct
		msgpck.UnmarshalStruct(data, &s)
	}
}
func BenchmarkMsgpck_MediumStruct_Marshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msgpck.Marshal(mediumStruct)
	}
}
func BenchmarkMsgpck_MediumStruct_Unmarshal(b *testing.B) {
	data, _ := msgpck.MarshalCopy(mediumStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s MediumStruct
		msgpck.UnmarshalStruct(data, &s)
	}
}

// Pre-registered StructDecoder - fastest for repeated decoding (using cached versions)
var smallStructDec = msgpck.GetStructDecoder[SmallStruct](false)
var mediumStructDec = msgpck.GetStructDecoder[MediumStruct](false)

// Zero-copy decoders - no string allocations!
var smallStructDecZC = msgpck.GetStructDecoder[SmallStruct](true)
var mediumStructDecZC = msgpck.GetStructDecoder[MediumStruct](true)

func BenchmarkMsgpck_SmallStruct_PreReg(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s SmallStruct
		smallStructDec.Decode(data, &s)
	}
}

func BenchmarkMsgpck_MediumStruct_PreReg(b *testing.B) {
	data, _ := msgpck.MarshalCopy(mediumStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s MediumStruct
		mediumStructDec.Decode(data, &s)
	}
}

func BenchmarkMsgpck_SmallStruct_ZeroCopy(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s SmallStruct
		smallStructDecZC.Decode(data, &s)
	}
}

func BenchmarkMsgpck_MediumStruct_ZeroCopy(b *testing.B) {
	data, _ := msgpck.MarshalCopy(mediumStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s MediumStruct
		mediumStructDecZC.Decode(data, &s)
	}
}

// Pre-registered StructEncoder (using cached versions)
var smallStructEnc = msgpck.GetStructEncoder[SmallStruct]()
var mediumStructEnc = msgpck.GetStructEncoder[MediumStruct]()

func BenchmarkMsgpck_SmallStruct_EncPreReg(b *testing.B) {
	for i := 0; i < b.N; i++ {
		smallStructEnc.Encode(&smallStruct)
	}
}

func BenchmarkMsgpck_MediumStruct_EncPreReg(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mediumStructEnc.Encode(&mediumStruct)
	}
}

func BenchmarkMsgpck_SmallStruct_EncPreRegCopy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		smallStructEnc.EncodeCopy(&smallStruct)
	}
}

func BenchmarkMsgpck_MediumStruct_EncPreRegCopy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mediumStructEnc.EncodeCopy(&mediumStruct)
	}
}

// Typed map decoding benchmarks
func BenchmarkMsgpck_SmallMap_TypedUnmarshal(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.UnmarshalMapStringAny(data, false)
	}
}

func BenchmarkMsgpck_SmallMap_ZeroCopy(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.UnmarshalMapStringAny(data, true)
	}
}

func BenchmarkMsgpck_MediumMap_TypedUnmarshal(b *testing.B) {
	data, _ := msgpck.MarshalCopy(mediumMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.UnmarshalMapStringAny(data, false)
	}
}

func BenchmarkMsgpck_MediumMap_ZeroCopy(b *testing.B) {
	data, _ := msgpck.MarshalCopy(mediumMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.UnmarshalMapStringAny(data, true)
	}
}

// map[string]string benchmarks
var stringMap = map[string]string{
	"name":  "Alice",
	"email": "alice@example.com",
	"role":  "admin",
	"dept":  "engineering",
}

func BenchmarkMsgpck_StringMap_Unmarshal(b *testing.B) {
	data, _ := msgpck.MarshalCopy(stringMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.UnmarshalMapStringString(data, false)
	}
}

func BenchmarkMsgpck_StringMap_ZeroCopy(b *testing.B) {
	data, _ := msgpck.MarshalCopy(stringMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.UnmarshalMapStringString(data, true)
	}
}

// Callback-based API benchmarks (safe zero-copy)
func BenchmarkMsgpck_SmallStruct_Callback(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.DecodeStructFunc(data, func(s *SmallStruct) error {
			_ = s.Name
			return nil
		})
	}
}

func BenchmarkMsgpck_MediumStruct_Callback(b *testing.B) {
	data, _ := msgpck.MarshalCopy(mediumStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.DecodeStructFunc(data, func(s *MediumStruct) error {
			_ = s.Name
			return nil
		})
	}
}

func BenchmarkMsgpck_SmallMap_Callback(b *testing.B) {
	data, _ := msgpck.MarshalCopy(smallMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.DecodeMapFunc(data, func(m map[string]any) error {
			_ = m["name"]
			return nil
		})
	}
}

func BenchmarkMsgpck_StringMap_Callback(b *testing.B) {
	data, _ := msgpck.MarshalCopy(stringMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpck.DecodeStringMapFunc(data, func(m map[string]string) error {
			_ = m["name"]
			return nil
		})
	}
}

// TinyKVS integration benchmarks
func BenchmarkTinyKVS_PutMap(b *testing.B) {
	store := setupTempStore(b)
	defer store.Close()
	key := []byte("test-key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.PutMap(key, mediumMap)
	}
}

func BenchmarkTinyKVS_GetMap(b *testing.B) {
	store := setupTempStore(b)
	defer store.Close()
	key := []byte("test-key")
	store.PutMap(key, mediumMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetMap(key)
	}
}

func BenchmarkTinyKVS_GetMap_ZeroCopy(b *testing.B) {
	store := setupTempStore(b)
	defer store.Close()
	key := []byte("test-key")
	store.PutMap(key, mediumMap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetMapZeroCopy(key, func(m map[string]any) error {
			_ = m["name"]
			return nil
		})
	}
}

func BenchmarkTinyKVS_PutStruct(b *testing.B) {
	store := setupTempStore(b)
	defer store.Close()
	key := []byte("test-key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PutStruct(store, key, &mediumStruct)
	}
}

func BenchmarkTinyKVS_GetStruct(b *testing.B) {
	store := setupTempStore(b)
	defer store.Close()
	key := []byte("test-key")
	PutStruct(store, key, &mediumStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetStruct[MediumStruct](store, key)
	}
}

func BenchmarkTinyKVS_GetStructInto_Generic(b *testing.B) {
	store := setupTempStore(b)
	defer store.Close()
	key := []byte("test-key")
	PutStruct(store, key, &mediumStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s MediumStruct
		GetStructInto(store, key, &s)
	}
}

func BenchmarkTinyKVS_GetStruct_ZeroCopy(b *testing.B) {
	store := setupTempStore(b)
	defer store.Close()
	key := []byte("test-key")
	PutStruct(store, key, &mediumStruct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetStructZeroCopy[MediumStruct](store, key, func(v *MediumStruct) error {
			_ = v.Name
			return nil
		})
	}
}

func setupTempStore(b *testing.B) *Store {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open store: %v", err)
	}
	return store
}

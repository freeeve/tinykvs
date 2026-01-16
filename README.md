# TinyKVS

[![CI](https://github.com/freeeve/tinykvs/actions/workflows/ci.yml/badge.svg)](https://github.com/freeeve/tinykvs/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/freeeve/tinykvs/badge.svg?branch=main)](https://coveralls.io/github/freeeve/tinykvs?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/freeeve/tinykvs.svg)](https://pkg.go.dev/github.com/freeeve/tinykvs)
[![Go Report Card](https://goreportcard.com/badge/github.com/freeeve/tinykvs)](https://goreportcard.com/report/github.com/freeeve/tinykvs)

A low-memory, sorted key-value store for Go built on LSM-tree architecture with configurable compression (zstd, snappy, or none).

## Features

- **Sorted storage** - Lexicographic key ordering, efficient range scans
- **Ultra-low memory** - Runs 1B+ records on t4g.micro (1GB RAM) with swap
- **Configurable memory** - Block cache, memtable size, bloom filters all tunable
- **Concurrent access** - Concurrent reads and writes, optimized for read-heavy workloads
- **Durability** - Write-ahead log with configurable sync modes
- **Compression** - zstd (default), snappy, or none with configurable levels
- **Bloom filters** - Fast negative lookups (can be disabled to save memory)

## Installation

```bash
go get github.com/freeeve/tinykvs
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/freeeve/tinykvs"
)

func main() {
    // Open a store
    store, err := tinykvs.Open("/tmp/mydb", tinykvs.DefaultOptions("/tmp/mydb"))
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Write values
    store.PutString([]byte("name"), "Alice")
    store.PutInt64([]byte("age"), 30)
    store.PutFloat64([]byte("score"), 95.5)
    store.PutBool([]byte("active"), true)

    // Read values
    name, _ := store.GetString([]byte("name"))
    age, _ := store.GetInt64([]byte("age"))

    fmt.Printf("Name: %s, Age: %d\n", name, age)

    // Flush to disk
    store.Flush()
}
```

## API

### Store Operations

```go
// Open or create a store
func Open(path string, opts Options) (*Store, error)

// Close the store
func (s *Store) Close() error

// Flush all data to disk
func (s *Store) Flush() error
```

### Read/Write

```go
// Generic value operations
func (s *Store) Put(key []byte, value Value) error
func (s *Store) Get(key []byte) (Value, error)
func (s *Store) Delete(key []byte) error

// Typed convenience methods
func (s *Store) PutString(key []byte, value string) error
func (s *Store) PutInt64(key []byte, value int64) error
func (s *Store) PutFloat64(key []byte, value float64) error
func (s *Store) PutBool(key []byte, value bool) error
func (s *Store) PutBytes(key []byte, value []byte) error

func (s *Store) GetString(key []byte) (string, error)
func (s *Store) GetInt64(key []byte) (int64, error)
func (s *Store) GetFloat64(key []byte) (float64, error)
func (s *Store) GetBool(key []byte) (bool, error)
func (s *Store) GetBytes(key []byte) ([]byte, error)
```

### Range Scans

```go
// Iterate over all keys with a given prefix (sorted order)
// Return false from callback to stop iteration
func (s *Store) ScanPrefix(prefix []byte, fn func(key []byte, value Value) bool) error
```

### Value Types

```go
type Value struct {
    Type    ValueType
    Int64   int64
    Float64 float64
    Bool    bool
    Bytes   []byte
}

// Value constructors
func Int64Value(v int64) Value
func Float64Value(v float64) Value
func BoolValue(v bool) Value
func StringValue(v string) Value
func BytesValue(v []byte) Value
```

### Configuration

```go
type Options struct {
    Dir              string          // Data directory
    MemtableSize     int64           // Max memtable size before flush (default: 4MB)
    BlockCacheSize   int64           // LRU cache size (default: 64MB, 0 to disable)
    BlockSize        int             // Target block size (default: 16KB)
    CompressionType  CompressionType // zstd, snappy, or none (default: zstd)
    CompressionLevel int             // zstd level 1-4 (default: 1 = fastest)
    BloomFPRate      float64         // Bloom filter false positive rate (default: 0.01)
    WALSyncMode      WALSyncMode     // WAL sync behavior
    VerifyChecksums  bool            // Verify on read (default: true)
}

// Compression types
const (
    CompressionZstd   // Default, good compression and speed
    CompressionSnappy // Faster, less compression
    CompressionNone   // No compression
)

// Preset configurations
func DefaultOptions(dir string) Options          // Balanced defaults
func LowMemoryOptions(dir string) Options        // Minimal memory (4MB memtable, no cache, no bloom)
func HighPerformanceOptions(dir string) Options  // Max throughput
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Store                            │
├─────────────────────────────────────────────────────────┤
│  Write Path                    Read Path                │
│  ┌─────────┐                   ┌─────────────────────┐  │
│  │   WAL   │                   │ Memtable (newest)   │  │
│  └────┬────┘                   ├─────────────────────┤  │
│       │                        │ Immutable Memtables │  │
│       v                        ├─────────────────────┤  │
│  ┌─────────┐                   │ L0 SSTables         │  │
│  │Memtable │                   ├─────────────────────┤  │
│  └────┬────┘                   │ L1+ SSTables        │  │
│       │ flush                  └─────────┬───────────┘  │
│       v                                  │              │
│  ┌─────────┐    ┌───────────┐            │              │
│  │ SSTable │◄───│ LRU Cache │◄───────────┘              │
│  └─────────┘    └───────────┘                           │
│       │                                                 │
│       v compaction                                      │
│  ┌─────────────────────────────────────────────────┐    │
│  │ L0 → L1 → L2 → ... → L6 (leveled compaction)    │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Compression | zstd/snappy/none | Configurable speed vs size tradeoff |
| I/O | Explicit syscalls | Control over caching |
| Index | Sparse (per block) | Low memory footprint |
| Compaction | Leveled | Read-optimized |
| Concurrency | RWMutex | Simple, read-optimized |

### SSTable Format

```
┌────────────────────────────────────┐
│ Data Block 0 (compressed)          │
│ Data Block 1                       │
│ ...                                │
│ Data Block N                       │
├────────────────────────────────────┤
│ Bloom Filter                       │
├────────────────────────────────────┤
│ Index Block (sparse)               │
├────────────────────────────────────┤
│ Metadata Block                     │
├────────────────────────────────────┤
│ Footer (64 bytes)                  │
└────────────────────────────────────┘
```

### Upgrading from v0.3.x

v0.4.0 changed the block format to support configurable compression. Data files from v0.3.x are automatically readable and will be upgraded to the new format during compaction. No manual migration is required.

## Memory Usage

| Component | Memory |
|-----------|--------|
| Block cache | Configurable (default 64MB) |
| Memtable | Configurable (default 4MB) |
| Bloom filters | ~1.2MB per 1M keys |
| Sparse index | ~140KB per 1M keys (with 16KB blocks) |

For minimal memory (billions of records), use `LowMemoryOptions()`:
- 4MB memtable
- No block cache
- No bloom filters
- Index: ~140MB for 1B keys

## Performance

### Apple M3 Max

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Random read (64MB cache) | 1.3 µs | 800K ops/sec |
| Sequential read | 684 ns | 1.5M ops/sec |
| Sequential write | 5.7 µs | 175K ops/sec |
| Mixed (80% read) | 1.7 µs | 600K ops/sec |

Block cache impact (random reads, 100K keys):

| Cache | Latency | Hit Rate |
|-------|---------|----------|
| 0 MB  | 42 µs   | 0%       |
| 64 MB | 1.3 µs  | 99.9%    |

### AWS t4g.micro (1GB RAM, ARM64)

Ultra-low memory configuration with `GOMEMLIMIT=600MiB`:

| Metric | Value |
|--------|-------|
| Write throughput | ~150K ops/sec |
| Memory (heap) | 30-90 MB |
| Memory (sys) | ~130 MB |
| WAL size | ~3-4 MB (bounded) |

This configuration can sustain 1 billion sequential writes while staying within memory limits. The benchmark uses:
- 4MB memtable
- No block cache
- No bloom filters
- WAL sync disabled (for throughput)

See [BENCHMARKS.md](BENCHMARKS.md) for detailed results.

## Complexity

- **Writes**: O(log n) memtable insert, sequential I/O for WAL
- **Reads**: O(log n) per level, bloom filter avoids unnecessary reads
- **Space**: ~1.1x raw data size (compression + overhead)

## Examples

### Persistence and Recovery

```go
// Data persists across restarts
store, _ := tinykvs.Open("/tmp/mydb", tinykvs.DefaultOptions("/tmp/mydb"))
store.PutString([]byte("key"), "value")
store.Flush() // Ensure durability
store.Close()

// Reopen - data is still there
store, _ = tinykvs.Open("/tmp/mydb", tinykvs.DefaultOptions("/tmp/mydb"))
val, _ := store.GetString([]byte("key"))
fmt.Println(val) // "value"
```

### Low Memory Configuration

```go
opts := tinykvs.LowMemoryOptions("/tmp/mydb")
opts.MemtableSize = 512 * 1024 // 512KB
store, _ := tinykvs.Open("/tmp/mydb", opts)
```

### Low Memory (Billions of Records)

For running on memory-constrained systems like t4g.micro (1GB RAM) with billions of records:

```go
// Use LowMemoryOptions: 4MB memtable, no cache, no bloom filters
opts := tinykvs.LowMemoryOptions("/data/mydb")
store, _ := tinykvs.Open("/data/mydb", opts)

// Combined with GOMEMLIMIT for Go runtime memory control:
// GOMEMLIMIT=600MiB ./myapp
```

This configuration can handle 1B+ records while staying within tight memory limits.

### Prefix Scanning

```go
// Store user data with prefixed keys
store.PutString([]byte("user:001:name"), "Alice")
store.PutInt64([]byte("user:001:age"), 30)
store.PutString([]byte("user:002:name"), "Bob")
store.PutInt64([]byte("user:002:age"), 25)

// Scan all keys for user:001
store.ScanPrefix([]byte("user:001:"), func(key []byte, value tinykvs.Value) bool {
    fmt.Printf("%s = %v\n", key, value)
    return true // continue scanning
})

// Scan all users (returns keys in sorted order)
store.ScanPrefix([]byte("user:"), func(key []byte, value tinykvs.Value) bool {
    fmt.Printf("%s\n", key)
    return true
})
```

### Statistics

```go
stats := store.Stats()
fmt.Printf("Memtable: %d bytes, %d keys\n", stats.MemtableSize, stats.MemtableCount)
fmt.Printf("Cache hit rate: %.1f%%\n", stats.CacheStats.HitRate())
for _, level := range stats.Levels {
    fmt.Printf("L%d: %d tables, %d keys\n", level.Level, level.NumTables, level.NumKeys)
}
```

### Storing Structs

TinyKVS stores primitive types and byte slices natively. For structs, serialize to bytes using your preferred encoding. Here are common patterns:

#### JSON (simple, human-readable)

```go
import "encoding/json"

type User struct {
    Name  string `json:"name"`
    Email string `json:"email"`
    Age   int    `json:"age"`
}

// Write
user := User{Name: "Alice", Email: "alice@example.com", Age: 30}
data, _ := json.Marshal(user)
store.PutBytes([]byte("user:1"), data)

// Read
data, _ := store.GetBytes([]byte("user:1"))
var user User
json.Unmarshal(data, &user)
```

#### Gob (Go-native, efficient for Go-to-Go)

```go
import (
    "bytes"
    "encoding/gob"
)

// Write
var buf bytes.Buffer
gob.NewEncoder(&buf).Encode(user)
store.PutBytes([]byte("user:1"), buf.Bytes())

// Read
data, _ := store.GetBytes([]byte("user:1"))
var user User
gob.NewDecoder(bytes.NewReader(data)).Decode(&user)
```

#### MessagePack (compact, fast)

```go
import "github.com/vmihailenco/msgpack/v5"

// Write
data, _ := msgpack.Marshal(user)
store.PutBytes([]byte("user:1"), data)

// Read
data, _ := store.GetBytes([]byte("user:1"))
var user User
msgpack.Unmarshal(data, &user)
```

#### Protocol Buffers (schema-defined, cross-language)

```go
import "google.golang.org/protobuf/proto"

// Assuming User is a generated protobuf message
// Write
data, _ := proto.Marshal(user)
store.PutBytes([]byte("user:1"), data)

// Read
data, _ := store.GetBytes([]byte("user:1"))
user := &pb.User{}
proto.Unmarshal(data, user)
```

#### Comparison

| Format | Size | Speed | Cross-language | Schema |
|--------|------|-------|----------------|--------|
| JSON | Largest | Slow | Yes | No |
| Gob | Medium | Fast | Go only | No |
| MessagePack | Small | Fast | Yes | No |
| Protobuf | Smallest | Fastest | Yes | Required |

For most Go applications, **MessagePack** offers a good balance. Use **Protobuf** for cross-language systems or when schema evolution matters.

## License

MIT

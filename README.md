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

// Struct and map storage (uses msgpack internally)
func (s *Store) PutStruct(key []byte, v any) error
func (s *Store) GetStruct(key []byte, dest any) error
func (s *Store) PutMap(key []byte, fields map[string]any) error
func (s *Store) GetMap(key []byte) (map[string]any, error)

// JSON storage (stores as string, queryable in shell)
func (s *Store) PutJson(key []byte, v any) error
func (s *Store) GetJson(key []byte, dest any) error
```

### Batch Operations

```go
// Create a batch for atomic writes
batch := tinykvs.NewBatch()
batch.Put(key, value)
batch.PutString(key, "value")
batch.PutInt64(key, 42)
batch.PutStruct(key, myStruct)
batch.PutMap(key, map[string]any{"field": "value"})
batch.Delete(key)

// Apply atomically
store.WriteBatch(batch)
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
    Record  map[string]any  // For struct/map storage
}

// Value constructors
func Int64Value(v int64) Value
func Float64Value(v float64) Value
func BoolValue(v bool) Value
func StringValue(v string) Value
func BytesValue(v []byte) Value
func RecordValue(v map[string]any) Value
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
| L1+ Scans | Lazy loading | Only load tables when needed for LIMIT queries |

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

### Version Compatibility

Store files are not compatible between minor versions (e.g., v0.3.x stores cannot be read by v0.4.x). If upgrading, export your data first or recreate the store.

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
| Sequential read | 304 ns | 3.3M ops/sec |
| Sequential write | 465 ns | 2.2M ops/sec |
| Mixed (80% read) | 392 ns | 2.6M ops/sec |
| SSTable read (cached) | 300 ns | 3.3M ops/sec |

Block cache impact (random reads, 100K keys):

| Cache | Latency | Hit Rate |
|-------|---------|----------|
| 0 MB  | 42 µs   | 0%       |
| 64 MB | 300 ns  | 99.9%    |

### AWS t4g.micro (1GB RAM, ARM64)

1 billion record benchmark with `GOMEMLIMIT=700MiB`:

**zstd compression (default), 100M records**

| Operation | Throughput |
|-----------|------------|
| Sequential write | 579K ops/sec |
| Random read (no cache) | 16K ops/sec |
| Random read (64MB cache) | 16K ops/sec |
| Full scan | 1.4M keys/sec |
| Random prefix scan | 15K scans/sec |
| Prefix scan with LIMIT 100 | 7K scans/sec |

Prefix scans with LIMIT benefit from lazy loading: L1+ tables are sorted and non-overlapping, so only the tables actually needed are loaded.

Write time: ~3 min for 100M records, ~1.5h for 1B records

Memory usage during benchmark:
- Heap: 50-200 MB
- Sys: 450-700 MB
- Index: ~35 MB (for 1B records)

Configuration:
- 4MB memtable
- 16KB block size
- No block cache
- No bloom filters
- WAL sync disabled (for throughput)

## Complexity

- **Writes**: O(log n) memtable insert, sequential I/O for WAL
- **Reads**: O(L × log n) where L is number of levels (max 7), bloom filters skip levels without matches
- **Space**: Varies by data - sequential keys compress to ~0.1x with zstd, random data ~0.5-0.8x

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

### Storing Structs and Maps

TinyKVS has built-in support for storing Go structs and maps using msgpack serialization:

```go
type Address struct {
    City    string `msgpack:"city"`
    Country string `msgpack:"country"`
}

type User struct {
    Name    string  `msgpack:"name"`
    Email   string  `msgpack:"email"`
    Age     int     `msgpack:"age"`
    Address Address `msgpack:"address"`
}

// Store a struct
user := User{
    Name:    "Alice",
    Email:   "alice@example.com",
    Age:     30,
    Address: Address{City: "NYC", Country: "USA"},
}
store.PutStruct([]byte("user:1"), user)

// Retrieve into a struct
var retrieved User
store.GetStruct([]byte("user:1"), &retrieved)

// Store a map directly
store.PutMap([]byte("config:app"), map[string]any{
    "debug":   true,
    "timeout": 30,
})

// Retrieve as map
config, _ := store.GetMap([]byte("config:app"))
```

Nested structs are fully supported and can be queried in the interactive shell.

#### JSON Storage

For human-readable storage or shell querying:

```go
// Store as JSON string
store.PutJson([]byte("user:2"), User{Name: "Bob", Age: 25})

// Retrieve from JSON
var user User
store.GetJson([]byte("user:2"), &user)
```

#### Manual Serialization

For other formats (Gob, Protobuf, etc.), serialize to bytes:

```go
// Gob
var buf bytes.Buffer
gob.NewEncoder(&buf).Encode(user)
store.PutBytes([]byte("user:1"), buf.Bytes())

// Protobuf
data, _ := proto.Marshal(user)
store.PutBytes([]byte("user:1"), data)
```

## Interactive Shell

TinyKVS includes an interactive SQL-like shell for exploring and manipulating data:

```bash
go install github.com/freeeve/tinykvs/cmd/tinykvs@latest
tinykvs shell -dir /path/to/db

# Or use environment variable
export TINYKVS_STORE=/path/to/db
tinykvs shell
```

Results are displayed in a formatted table:
```
┌────────┬───────────────────────────┐
│ k      │ v                         │
├────────┼───────────────────────────┤
│ user:1 │ {"age":30,"name":"Alice"} │
│ user:2 │ {"age":25,"name":"Bob"}   │
└────────┴───────────────────────────┘
(2 rows) scanned 2 keys, 0 blocks, 0ms
```

### SQL Commands

```sql
-- Query data
SELECT * FROM kv WHERE k = 'user:1'
SELECT * FROM kv WHERE k LIKE 'user:%'
SELECT * FROM kv WHERE k BETWEEN 'a' AND 'z' LIMIT 10
SELECT * FROM kv LIMIT 100

-- Extract record fields
SELECT v.name, v.age FROM kv WHERE k = 'user:1'
SELECT v.address.city FROM kv WHERE k = 'user:1'

-- ORDER BY (buffers results for sorting)
SELECT * FROM kv ORDER BY k DESC LIMIT 10
SELECT v.name, v.age FROM kv ORDER BY v.age DESC, v.name
SELECT * FROM kv WHERE k LIKE 'user:%' ORDER BY v.score LIMIT 100

-- Insert data (JSON auto-detected as records)
INSERT INTO kv VALUES ('user:1', '{"name":"Alice","age":30}')
INSERT INTO kv VALUES ('key', 'simple string value')
INSERT INTO kv VALUES ('bin', x'deadbeef')  -- hex bytes

-- Update and delete
UPDATE kv SET v = 'newvalue' WHERE k = 'key'
DELETE FROM kv WHERE k = 'key'
DELETE FROM kv WHERE k LIKE 'temp:%'
```

### Shell Commands

```
\help, \h, \?      Show help
\stats             Show store statistics
\flush             Flush memtable to disk
\compact           Run compaction
\tables            Show table schema
\export <file>     Export to CSV
\import <file>     Import from CSV
\q, \quit          Exit shell
```

### Binary Key Functions

The shell supports functions for constructing binary keys:

```sql
-- uint64_be(n) - 8-byte big-endian encoding
SELECT * FROM kv WHERE k = x'14' || uint64_be(28708)

-- uint64_le(n) - 8-byte little-endian encoding
-- uint32_be(n) - 4-byte big-endian encoding
-- uint32_le(n) - 4-byte little-endian encoding

-- byte(n) - single byte (0-255)
SELECT * FROM kv WHERE k = byte(0x14) || uint64_be(12345)

-- fnv64(s) - FNV-1a 64-bit hash of string
SELECT * FROM kv WHERE k LIKE byte(0x10) || fnv64('user-123') || '%'

-- Hex concatenation
SELECT * FROM kv WHERE k = x'14' || uint64_be(28708) || fnv64('item-456')
```

These are useful for querying data with composite binary keys.

### CSV Import/Export

Export creates a simple `key,value` CSV:
```csv
key,value
user:1,{"name":"Alice","age":30}
counter,42
flag,true
```

Import auto-detects the format:

**2 columns (key,value)** - values auto-detect type:
```csv
key,value
user:1,hello
user:2,42
user:3,{"name":"Bob"}
```

**3+ columns** - first column is key, rest become record fields:
```csv
id,name,age,active
user:1,Alice,30,true
user:2,Bob,25,false
```
This creates records like `{"name":"Alice","age":30,"active":true}`

**Type hints** - prevent unwanted auto-detection (e.g., zip codes):
```csv
id,zip:string,count:int,price:float,active:bool,data:json
item:1,02134,100,19.99,true,{"x":1}
```
Supported hints: `string`, `int`, `float`, `bool`, `json`

### Nested Field Access

Records with nested structures support dot notation for field access:

```sql
-- Given: {"name":"Alice","address":{"city":"NYC","geo":{"lat":40.7}}}

SELECT v.name FROM kv WHERE k = 'user:1'           -- Alice
SELECT v.address.city FROM kv WHERE k = 'user:1'   -- NYC
SELECT v.`address.geo.lat` FROM kv WHERE k = 'user:1'  -- 40.7 (3+ levels need backticks)
```

### Streaming Aggregations

Aggregation functions compute results in a single pass with O(1) memory:

```sql
SELECT count() FROM kv                              -- count all rows
SELECT count() FROM kv WHERE k LIKE 'user:%'        -- count with filter
SELECT sum(v.age), avg(v.age) FROM kv               -- sum and average
SELECT min(v.score), max(v.score) FROM kv           -- min and max
SELECT count(), sum(v.price), avg(v.price) FROM kv  -- multiple aggregates
SELECT sum(v.stats.count) FROM kv                   -- nested fields work too
```

## License

MIT

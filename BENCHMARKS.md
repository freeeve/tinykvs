# TinyKVS Benchmarks

Benchmarks run on Apple M3 Max, Go 1.22+.

## Large Scale Test (100M Records)

| Phase | Metric | Value |
|-------|--------|-------|
| **Write** | Records | 100M |
| | Duration | 8m 23s |
| | Throughput | **199K ops/sec** |
| | Peak Heap | ~1GB |
| **Read (0MB cache)** | Random | 10K ops/sec |
| **Read (256MB cache)** | Random | 17K ops/sec |
| | Sequential | 337K ops/sec |

### Memory Usage During Write (100M records)

With default settings (4MB memtable):
- Heap: 300-700MB (spikes to 1GB during compaction)
- Sys: ~2GB

With GOMEMLIMIT=256MiB:
- Heap: 150-250MB
- Throughput: ~75K ops/sec (GC overhead)

### Ultra Low Memory Mode (10M records)

Using `LowMemoryOptions()`:
- 1MB memtable
- No block cache
- Bloom filters disabled
- GOMEMLIMIT=256MiB

| Metric | Value |
|--------|-------|
| Write throughput | 75K ops/sec |
| Peak heap | 394MB |
| Read (before compact) | 13K ops/sec |
| Read (after compact) | 14K ops/sec |

### Compaction Impact

| State | L0 Tables | L1 Tables | Read Throughput |
|-------|-----------|-----------|-----------------|
| Before | 118 | 14 | 13,182 ops/sec |
| After | 0 | 16 | 14,346 ops/sec |

Compaction provides ~9% improvement by converting overlapping L0 tables to disjoint L1 tables.

## Block Cache Impact

The block cache dramatically improves read performance by avoiding disk I/O:

| Cache Size | Read Latency | Hit Rate | Speedup |
|------------|--------------|----------|---------|
| 0 MB       | 42.3 µs      | 0%       | 1x      |
| 64 MB      | 1.3 µs       | 99.9%    | 33x     |
| 128 MB     | 1.4 µs       | 99.9%    | 30x     |
| 256 MB     | 0.9 µs       | 99.9%    | 46x     |

Even a 64MB cache provides near-optimal performance for typical workloads.

## Read Performance

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Sequential read | 684 ns | 1.46M ops/sec |
| Random read | 1.24 µs | 806K ops/sec |
| SSTable Get (cached) | 384 ns | 2.6M ops/sec |
| Concurrent reads | 567 ns | 1.76M ops/sec |

## Write Performance

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Sequential write | 5.7 µs | 175K ops/sec |
| Random write | 6.1 µs | 165K ops/sec |
| WAL append | 120 ns | 8.3M ops/sec |

## Value Size Impact on Writes

| Value Size | Write Latency | Memory/op |
|------------|---------------|-----------|
| 64 B       | 6.2 µs        | 102 KB    |
| 256 B      | 24.4 µs       | 334 KB    |
| 1 KB       | 171 µs        | 1.6 MB    |
| 4 KB       | 488 µs        | 4.7 MB    |
| 16 KB      | 432 µs        | 4.7 MB    |

## Mixed Workload (80% Read / 20% Write)

| Metric | Value |
|--------|-------|
| Latency | 1.66 µs |
| Throughput | 602K ops/sec |

## Component Benchmarks

| Component | Operation | Latency |
|-----------|-----------|---------|
| Memtable | Put | 503 ns |
| Memtable | Get | 388 ns |
| Bloom filter | MayContain | 41 ns |
| Index | Search | 52 ns |
| Cache | Get | 27 ns |
| Cache | Put | 359 ns |
| Block | Search | 21 ns |
| Key compare | CompareKeys | 15 ns |
| Value | Encode | 29 ns |
| Value | Decode | 33 ns |

## Store Load Time

Opening a store with 50K keys across multiple SSTables:

| Metric | Value |
|--------|-------|
| Load time | 4.9 ms |
| Memory | 619 KB |

## Flush Performance

Flushing a 4MB memtable with 10K entries:

| Metric | Value |
|--------|-------|
| Flush time | 18 ms |
| Memory | 95 MB (temporary) |

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem

# Run specific benchmark
go test -bench=BenchmarkStoreReadCacheSize -benchmem

# Run with longer duration for more accurate results
go test -bench=. -benchtime=5s -benchmem
```

## Memory Allocation Summary

| Operation | Allocs/op |
|-----------|-----------|
| Get (cached) | 2-3 |
| Put | 8-9 |
| Key compare | 0 |
| Bloom check | 0 |
| Index search | 0 |
| Cache get | 0 |

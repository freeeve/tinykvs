package tinykvs

import (
	"testing"
)

func TestCachePutGet(t *testing.T) {
	cache := newLRUCache(1024 * 1024) // 1MB

	key := cacheKey{FileID: 1, BlockOffset: 0}
	block := &Block{
		Type: blockTypeData,
		Entries: []BlockEntry{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}

	cache.Put(key, block)

	got, found := cache.Get(key)
	if !found {
		t.Fatal("block not found in cache")
	}

	if len(got.Entries) != 2 {
		t.Errorf("got %d entries, want 2", len(got.Entries))
	}
}

func TestCacheMiss(t *testing.T) {
	cache := newLRUCache(1024 * 1024)

	key := cacheKey{FileID: 1, BlockOffset: 0}
	_, found := cache.Get(key)
	if found {
		t.Error("expected cache miss")
	}
}

func TestCacheEviction(t *testing.T) {
	// Small cache that can only hold a few blocks
	cache := newLRUCache(100) // 100 bytes

	// Add blocks until eviction happens
	for i := 0; i < 10; i++ {
		key := cacheKey{FileID: 1, BlockOffset: uint64(i * 1000)}
		block := &Block{
			Type: blockTypeData,
			Entries: []BlockEntry{
				{Key: []byte("key"), Value: make([]byte, 20)}, // ~24 bytes per block
			},
		}
		cache.Put(key, block)
	}

	// First blocks should be evicted
	stats := cache.Stats()
	if stats.Size > stats.Capacity {
		t.Errorf("cache size %d exceeds capacity %d", stats.Size, stats.Capacity)
	}
}

func TestCacheRemove(t *testing.T) {
	cache := newLRUCache(1024 * 1024)

	key := cacheKey{FileID: 1, BlockOffset: 0}
	block := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("key"), Value: []byte("value")}},
	}

	cache.Put(key, block)

	// Verify it's there
	_, found := cache.Get(key)
	if !found {
		t.Fatal("block should be in cache")
	}

	// Remove it
	cache.Remove(key)

	// Verify it's gone
	_, found = cache.Get(key)
	if found {
		t.Error("block should be removed from cache")
	}
}

func TestCacheRemoveByFileID(t *testing.T) {
	cache := newLRUCache(1024 * 1024)

	// Add blocks from multiple files
	for fileID := uint32(1); fileID <= 3; fileID++ {
		for offset := uint64(0); offset < 3; offset++ {
			key := cacheKey{FileID: fileID, BlockOffset: offset * 1000}
			block := &Block{
				Type:    blockTypeData,
				Entries: []BlockEntry{{Key: []byte("key"), Value: []byte("value")}},
			}
			cache.Put(key, block)
		}
	}

	// Remove all blocks for file 2
	cache.RemoveByFileID(2)

	// Verify file 2 blocks are gone
	for offset := uint64(0); offset < 3; offset++ {
		key := cacheKey{FileID: 2, BlockOffset: offset * 1000}
		_, found := cache.Get(key)
		if found {
			t.Errorf("block from file 2 should be removed")
		}
	}

	// Verify file 1 and 3 blocks are still there
	for _, fileID := range []uint32{1, 3} {
		key := cacheKey{FileID: fileID, BlockOffset: 0}
		_, found := cache.Get(key)
		if !found {
			t.Errorf("block from file %d should still be in cache", fileID)
		}
	}
}

func TestCacheClear(t *testing.T) {
	cache := newLRUCache(1024 * 1024)

	// Add some blocks
	for i := 0; i < 5; i++ {
		key := cacheKey{FileID: 1, BlockOffset: uint64(i * 1000)}
		block := &Block{
			Type:    blockTypeData,
			Entries: []BlockEntry{{Key: []byte("key"), Value: []byte("value")}},
		}
		cache.Put(key, block)
	}

	// Clear
	cache.Clear()

	// Verify all are gone
	stats := cache.Stats()
	if stats.Entries != 0 {
		t.Errorf("cache should be empty, has %d entries", stats.Entries)
	}
	if stats.Size != 0 {
		t.Errorf("cache size should be 0, is %d", stats.Size)
	}
}

func TestCacheStats(t *testing.T) {
	cache := newLRUCache(1024 * 1024)

	key := cacheKey{FileID: 1, BlockOffset: 0}
	block := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("key"), Value: []byte("value")}},
	}

	// Miss
	cache.Get(key)

	// Put
	cache.Put(key, block)

	// Hit
	cache.Get(key)
	cache.Get(key)

	stats := cache.Stats()
	if stats.Hits != 2 {
		t.Errorf("hits = %d, want 2", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("misses = %d, want 1", stats.Misses)
	}
	if stats.Entries != 1 {
		t.Errorf("entries = %d, want 1", stats.Entries)
	}
}

func TestCacheHitRate(t *testing.T) {
	cache := newLRUCache(1024 * 1024)

	key := cacheKey{FileID: 1, BlockOffset: 0}
	block := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("key"), Value: []byte("value")}},
	}

	// 1 miss
	cache.Get(key)

	cache.Put(key, block)

	// 3 hits
	cache.Get(key)
	cache.Get(key)
	cache.Get(key)

	stats := cache.Stats()
	hitRate := stats.HitRate()

	// 3 hits / 4 total = 75%
	if hitRate != 75.0 {
		t.Errorf("hit rate = %.1f%%, want 75.0%%", hitRate)
	}
}

func TestCacheHitRateEmpty(t *testing.T) {
	cache := newLRUCache(1024 * 1024)
	stats := cache.Stats()
	if stats.HitRate() != 0 {
		t.Errorf("hit rate should be 0 for empty stats")
	}
}

func TestCacheZeroCapacity(t *testing.T) {
	cache := newLRUCache(0)

	key := cacheKey{FileID: 1, BlockOffset: 0}
	block := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("key"), Value: []byte("value")}},
	}

	// Put should be a no-op
	cache.Put(key, block)

	// Get should always miss
	_, found := cache.Get(key)
	if found {
		t.Error("zero-capacity cache should never have entries")
	}

	// Remove should be a no-op
	cache.Remove(key)
	cache.RemoveByFileID(1)
}

func TestCacheUpdate(t *testing.T) {
	cache := newLRUCache(1024 * 1024)

	key := cacheKey{FileID: 1, BlockOffset: 0}

	block1 := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("key1"), Value: []byte("value1")}},
	}
	block2 := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("key2"), Value: []byte("value2")}},
	}

	cache.Put(key, block1)
	cache.Put(key, block2)

	got, found := cache.Get(key)
	if !found {
		t.Fatal("block not found")
	}

	if string(got.Entries[0].Key) != "key2" {
		t.Error("block should be updated to block2")
	}

	// Should still only have 1 entry
	stats := cache.Stats()
	if stats.Entries != 1 {
		t.Errorf("entries = %d, want 1", stats.Entries)
	}
}

func TestCacheLRUOrder(t *testing.T) {
	// Cache that can hold ~2 blocks
	cache := newLRUCache(50)

	key1 := cacheKey{FileID: 1, BlockOffset: 0}
	key2 := cacheKey{FileID: 1, BlockOffset: 1000}
	key3 := cacheKey{FileID: 1, BlockOffset: 2000}

	block := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("k"), Value: make([]byte, 15)}},
	}

	// Add key1, key2
	cache.Put(key1, block)
	cache.Put(key2, block)

	// Access key1 to make it recently used
	cache.Get(key1)

	// Add key3 - should evict key2 (least recently used)
	cache.Put(key3, block)

	// key1 should still be there
	_, found := cache.Get(key1)
	if !found {
		t.Error("key1 should still be in cache (recently accessed)")
	}

	// key3 should be there
	_, found = cache.Get(key3)
	if !found {
		t.Error("key3 should be in cache (just added)")
	}
}

func BenchmarkCacheGet(b *testing.B) {
	cache := newLRUCache(64 * 1024 * 1024)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := cacheKey{FileID: 1, BlockOffset: uint64(i * 4096)}
		block := &Block{
			Type:    blockTypeData,
			Entries: []BlockEntry{{Key: []byte("key"), Value: make([]byte, 100)}},
		}
		cache.Put(key, block)
	}

	key := cacheKey{FileID: 1, BlockOffset: 500 * 4096}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(key)
	}
}

func BenchmarkCachePut(b *testing.B) {
	cache := newLRUCache(64 * 1024 * 1024)

	block := &Block{
		Type:    blockTypeData,
		Entries: []BlockEntry{{Key: []byte("key"), Value: make([]byte, 100)}},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := cacheKey{FileID: 1, BlockOffset: uint64(i * 4096)}
		cache.Put(key, block)
	}
}

// BenchmarkCacheEviction measures allocation behavior during cache eviction.
// With reference counting, evicted blocks should return buffers to the pool.
func BenchmarkCacheEviction(b *testing.B) {
	// Small cache to force frequent evictions
	cache := newLRUCache(16 * 1024) // 16KB cache

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a block with pooled buffer (simulates real block from DecodeBlock)
		buf := getDecompressBuffer(4096)
		block := &Block{
			Type:    blockTypeData,
			Entries: []BlockEntry{{Key: []byte("key"), Value: buf[:100]}},
			buffer:  buf,
			pooled:  true,
		}

		key := cacheKey{FileID: 1, BlockOffset: uint64(i * 4096)}
		cache.Put(key, block)
	}
}

// BenchmarkCacheChurn simulates realistic cache usage with gets and puts.
func BenchmarkCacheChurn(b *testing.B) {
	cache := newLRUCache(64 * 1024) // 64KB cache

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate read: check cache, miss, load, put, use, release
		key := cacheKey{FileID: 1, BlockOffset: uint64((i % 100) * 4096)}

		block, found := cache.Get(key)
		if found {
			// Cache hit - use block then release our reference
			_ = len(block.Entries)
			block.DecRef()
		} else {
			// Cache miss - create new block with pooled buffer
			buf := getDecompressBuffer(4096)
			block = &Block{
				Type:    blockTypeData,
				Entries: []BlockEntry{{Key: []byte("key"), Value: buf[:100]}},
				buffer:  buf,
				pooled:  true,
			}
			cache.Put(key, block)
			block.IncRef() // We also take a reference
			_ = len(block.Entries)
			block.DecRef() // Release our reference
		}
	}
}

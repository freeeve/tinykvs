package tinykvs

import (
	"container/list"
	"sync"
)

// CacheKey uniquely identifies a cached block.
type CacheKey struct {
	FileID      uint32
	BlockOffset uint64
}

// cacheEntry holds a cached block.
type cacheEntry struct {
	key   CacheKey
	block *Block
	size  int64
}

// LRUCache is a thread-safe LRU block cache.
type LRUCache struct {
	capacity  int64
	size      int64
	items     map[CacheKey]*list.Element
	evictList *list.List
	mu        sync.RWMutex

	// Statistics
	hits   uint64
	misses uint64
}

// NewLRUCache creates a new LRU cache with the given capacity in bytes.
// If capacity is 0, the cache is disabled.
func NewLRUCache(capacity int64) *LRUCache {
	return &LRUCache{
		capacity:  capacity,
		items:     make(map[CacheKey]*list.Element),
		evictList: list.New(),
	}
}

// Get retrieves a block from the cache.
// Returns the block and true if found, nil and false otherwise.
func (c *LRUCache) Get(key CacheKey) (*Block, bool) {
	if c.capacity == 0 {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		c.hits++
		return elem.Value.(*cacheEntry).block, true
	}

	c.misses++
	return nil, false
}

// Put adds a block to the cache.
func (c *LRUCache) Put(key CacheKey, block *Block) {
	if c.capacity == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Estimate block size (entries data)
	blockSize := int64(0)
	for _, e := range block.Entries {
		blockSize += int64(len(e.Key) + len(e.Value))
	}

	// Update existing entry
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		c.size -= entry.size
		entry.block = block
		entry.size = blockSize
		c.size += blockSize
		return
	}

	// Evict if necessary
	for c.size+blockSize > c.capacity && c.evictList.Len() > 0 {
		c.evict()
	}

	// Add new entry
	entry := &cacheEntry{
		key:   key,
		block: block,
		size:  blockSize,
	}
	elem := c.evictList.PushFront(entry)
	c.items[key] = elem
	c.size += blockSize
}

// evict removes the least recently used entry.
func (c *LRUCache) evict() {
	elem := c.evictList.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.evictList.Remove(elem)
	c.size -= entry.size

	// Return block's buffer to pool
	if entry.block != nil {
		entry.block.Release()
	}
}

// Remove removes a specific key from the cache.
func (c *LRUCache) Remove(key CacheKey) {
	if c.capacity == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*cacheEntry)
		delete(c.items, key)
		c.evictList.Remove(elem)
		c.size -= entry.size
		if entry.block != nil {
			entry.block.Release()
		}
	}
}

// RemoveByFileID removes all entries for a given file ID.
// Useful when an SSTable is deleted during compaction.
func (c *LRUCache) RemoveByFileID(fileID uint32) {
	if c.capacity == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, elem := range c.items {
		if key.FileID == fileID {
			entry := elem.Value.(*cacheEntry)
			delete(c.items, key)
			c.evictList.Remove(elem)
			c.size -= entry.size
			if entry.block != nil {
				entry.block.Release()
			}
		}
	}
}

// Clear removes all entries from the cache.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Release all block buffers
	for _, elem := range c.items {
		entry := elem.Value.(*cacheEntry)
		if entry.block != nil {
			entry.block.Release()
		}
	}

	c.items = make(map[CacheKey]*list.Element)
	c.evictList.Init()
	c.size = 0
}

// Stats returns cache statistics.
func (c *LRUCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CacheStats{
		Hits:     c.hits,
		Misses:   c.misses,
		Size:     c.size,
		Capacity: c.capacity,
		Entries:  c.evictList.Len(),
	}
}

// CacheStats contains cache statistics.
type CacheStats struct {
	Hits     uint64
	Misses   uint64
	Size     int64
	Capacity int64
	Entries  int
}

// HitRate returns the cache hit rate as a percentage.
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total) * 100
}

package tinykvs

import "sync"

// Reader coordinates lookups across memtable and SSTables.
type Reader struct {
	mu sync.RWMutex

	memtable   *Memtable
	immutables []*Memtable  // Recently flushed, waiting for SSTable write
	levels     [][]*SSTable // levels[0] = L0, levels[1] = L1, etc.
	cache      *LRUCache
	opts       Options
}

// NewReader creates a new reader.
func NewReader(memtable *Memtable, levels [][]*SSTable, cache *LRUCache, opts Options) *Reader {
	// Make a deep copy of levels to avoid sharing with Store
	// Store and Reader use different mutexes, so sharing would cause races
	levelsCopy := make([][]*SSTable, len(levels))
	for i, level := range levels {
		levelsCopy[i] = make([]*SSTable, len(level))
		copy(levelsCopy[i], level)
	}
	return &Reader{
		memtable: memtable,
		levels:   levelsCopy,
		cache:    cache,
		opts:     opts,
	}
}

// Get looks up a key, checking memtable first, then SSTables.
func (r *Reader) Get(key []byte) (Value, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Check active memtable
	if entry, found := r.memtable.Get(key); found {
		if entry.Value.IsTombstone() {
			return Value{}, ErrKeyNotFound
		}
		return entry.Value, nil
	}

	// 2. Check immutable memtables (newest first)
	for i := len(r.immutables) - 1; i >= 0; i-- {
		if entry, found := r.immutables[i].Get(key); found {
			if entry.Value.IsTombstone() {
				return Value{}, ErrKeyNotFound
			}
			return entry.Value, nil
		}
	}

	// 3. Check SSTables level by level (L0 to Lmax)
	for level := 0; level < len(r.levels); level++ {
		tables := r.levels[level]

		if level == 0 {
			// L0: Check all tables (newest first, may have overlapping keys)
			for i := len(tables) - 1; i >= 0; i-- {
				entry, found, err := tables[i].Get(key, r.cache, r.opts.VerifyChecksums)
				if err != nil {
					return Value{}, err
				}
				if found {
					if entry.Value.IsTombstone() {
						return Value{}, ErrKeyNotFound
					}
					return entry.Value, nil
				}
			}
		} else {
			// L1+: Tables are sorted and non-overlapping, binary search
			idx := r.findTableForKey(tables, key)
			if idx >= 0 {
				entry, found, err := tables[idx].Get(key, r.cache, r.opts.VerifyChecksums)
				if err != nil {
					return Value{}, err
				}
				if found {
					if entry.Value.IsTombstone() {
						return Value{}, ErrKeyNotFound
					}
					return entry.Value, nil
				}
			}
		}
	}

	return Value{}, ErrKeyNotFound
}

// findTableForKey finds the SSTable that may contain the key (for L1+).
// Returns the index of the table, or -1 if not found.
func (r *Reader) findTableForKey(tables []*SSTable, key []byte) int {
	lo, hi := 0, len(tables)-1

	for lo <= hi {
		mid := (lo + hi) / 2
		minKey := tables[mid].MinKey()
		maxKey := tables[mid].MaxKey()

		// Check if key is in range [MinKey, MaxKey]
		if CompareKeys(key, minKey) >= 0 && CompareKeys(key, maxKey) <= 0 {
			return mid
		}

		if CompareKeys(key, minKey) < 0 {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	return -1
}

// SetMemtable updates the active memtable.
func (r *Reader) SetMemtable(mt *Memtable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.memtable = mt
}

// AddImmutable adds an immutable memtable.
func (r *Reader) AddImmutable(mt *Memtable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.immutables = append(r.immutables, mt)
}

// RemoveImmutable removes an immutable memtable after it's been flushed.
func (r *Reader) RemoveImmutable(mt *Memtable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, imm := range r.immutables {
		if imm == mt {
			r.immutables = append(r.immutables[:i], r.immutables[i+1:]...)
			return
		}
	}
}

// SetLevels updates the SSTable levels.
func (r *Reader) SetLevels(levels [][]*SSTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.levels = levels
}

// AddSSTable adds an SSTable to a level.
func (r *Reader) AddSSTable(level int, sst *SSTable) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ensure the level exists
	for len(r.levels) <= level {
		r.levels = append(r.levels, nil)
	}

	r.levels[level] = append(r.levels[level], sst)
}

// GetLevels returns a copy of the current levels.
func (r *Reader) GetLevels() [][]*SSTable {
	r.mu.RLock()
	defer r.mu.RUnlock()

	levels := make([][]*SSTable, len(r.levels))
	for i, level := range r.levels {
		levels[i] = make([]*SSTable, len(level))
		copy(levels[i], level)
	}
	return levels
}

// ScanPrefix iterates over all keys with the given prefix in sorted order.
// Keys are deduplicated (newest version wins) and tombstones are skipped.
// Return false from the callback to stop iteration early.
func (r *Reader) ScanPrefix(prefix []byte, fn func(key []byte, value Value) bool) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Build a prefix scanner with all sources
	scanner := newPrefixScanner(prefix, r.cache, r.opts.VerifyChecksums)

	// Add memtable (highest priority - index 0)
	scanner.addMemtable(r.memtable, 0)

	// Add immutable memtables (newest first)
	for i := len(r.immutables) - 1; i >= 0; i-- {
		scanner.addMemtable(r.immutables[i], len(r.immutables)-i)
	}

	// Add SSTable levels
	baseIdx := len(r.immutables) + 1
	for level := 0; level < len(r.levels); level++ {
		tables := r.levels[level]
		if level == 0 {
			// L0: add all tables (newest first)
			for i := len(tables) - 1; i >= 0; i-- {
				scanner.addSSTable(tables[i], baseIdx)
				baseIdx++
			}
		} else {
			// L1+: add tables that may contain prefix
			for _, t := range tables {
				if hasKeyInRange(prefix, t.MinKey(), t.MaxKey()) {
					scanner.addSSTable(t, baseIdx)
					baseIdx++
				}
			}
		}
	}

	scanner.init()

	// Iterate and call callback
	var lastKey []byte
	for scanner.next() {
		entry := scanner.entry()

		// Skip duplicates (newer version already seen)
		if lastKey != nil && CompareKeys(entry.Key, lastKey) == 0 {
			continue
		}
		lastKey = entry.Key

		// Skip tombstones
		if entry.Value.IsTombstone() {
			continue
		}

		// Check prefix still matches
		if !hasPrefix(entry.Key, prefix) {
			break
		}

		if !fn(entry.Key, entry.Value) {
			break
		}
	}

	scanner.close()
	return nil
}

// hasPrefix returns true if key starts with prefix.
func hasPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}

// hasKeyInRange returns true if a key with the given prefix might exist in [minKey, maxKey].
func hasKeyInRange(prefix, minKey, maxKey []byte) bool {
	// Prefix is before or equal to maxKey AND prefix+\xff... is >= minKey
	// Simplified: prefix <= maxKey (prefix-wise) AND minKey prefix-matches or is < prefix
	if len(maxKey) >= len(prefix) {
		for i := 0; i < len(prefix); i++ {
			if prefix[i] < maxKey[i] {
				break // prefix < maxKey prefix, OK
			} else if prefix[i] > maxKey[i] {
				return false // prefix > maxKey, no match possible
			}
		}
	} else {
		// maxKey is shorter than prefix - check if maxKey is a prefix of prefix
		for i := 0; i < len(maxKey); i++ {
			if prefix[i] < maxKey[i] {
				break
			} else if prefix[i] > maxKey[i] {
				return false
			}
		}
	}

	// Check if minKey could have a prefix match
	if len(minKey) >= len(prefix) {
		for i := 0; i < len(prefix); i++ {
			if minKey[i] < prefix[i] {
				return true // minKey < prefix, might have prefix matches after minKey
			} else if minKey[i] > prefix[i] {
				return false // minKey > prefix+\xff..., no prefix matches
			}
		}
		return true // minKey starts with prefix
	}
	// minKey is shorter - if minKey < prefix, OK
	for i := 0; i < len(minKey); i++ {
		if minKey[i] < prefix[i] {
			return true
		} else if minKey[i] > prefix[i] {
			return false
		}
	}
	return true // minKey is prefix of prefix
}

// prefixScanner merges entries from memtables and SSTables for prefix scanning.
type prefixScanner struct {
	prefix  []byte
	cache   *LRUCache
	verify  bool
	heap    prefixHeap
	current Entry
}

type prefixHeapEntry struct {
	entry    Entry
	priority int // lower = newer/higher priority
	source   prefixSource
}

type prefixSource interface {
	next() bool
	entry() Entry
	close()
}

type prefixHeap []prefixHeapEntry

func (h prefixHeap) less(i, j int) bool {
	cmp := CompareKeys(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}

func (h *prefixHeap) push(x prefixHeapEntry) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *prefixHeap) pop() prefixHeapEntry {
	old := *h
	n := len(old) - 1
	old[0], old[n] = old[n], old[0]
	h.down(0, n)
	x := old[n]
	*h = old[:n]
	return x
}

func (h prefixHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.less(j, i) {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func (h prefixHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2
		}
		if !h.less(j, i) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

func (h *prefixHeap) init() {
	n := len(*h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func newPrefixScanner(prefix []byte, cache *LRUCache, verify bool) *prefixScanner {
	return &prefixScanner{
		prefix: prefix,
		cache:  cache,
		verify: verify,
		heap:   make(prefixHeap, 0, 8),
	}
}

func (s *prefixScanner) addMemtable(mt *Memtable, priority int) {
	src := &memtablePrefixSource{mt: mt, prefix: s.prefix}
	src.seekToPrefix()
	if src.valid {
		s.heap = append(s.heap, prefixHeapEntry{
			entry:    src.entry(),
			priority: priority,
			source:   src,
		})
	} else {
		// No matching entries, close the iterator to release lock
		src.close()
	}
}

func (s *prefixScanner) addSSTable(sst *SSTable, priority int) {
	src := &sstablePrefixSource{
		sst:    sst,
		prefix: s.prefix,
		cache:  s.cache,
		verify: s.verify,
	}
	src.seekToPrefix()
	if src.valid {
		s.heap = append(s.heap, prefixHeapEntry{
			entry:    src.entry(),
			priority: priority,
			source:   src,
		})
	} else {
		// No matching entries, close any resources
		src.close()
	}
}

func (s *prefixScanner) init() {
	s.heap.init()
}

func (s *prefixScanner) next() bool {
	if len(s.heap) == 0 {
		return false
	}

	he := s.heap.pop()
	s.current = he.entry

	if he.source.next() {
		s.heap.push(prefixHeapEntry{
			entry:    he.source.entry(),
			priority: he.priority,
			source:   he.source,
		})
	} else {
		// Source exhausted, close it to release any held locks
		he.source.close()
	}

	return true
}

func (s *prefixScanner) entry() Entry {
	return s.current
}

func (s *prefixScanner) close() {
	for _, he := range s.heap {
		he.source.close()
	}
}

// memtablePrefixSource wraps a memtable iterator for prefix scanning.
type memtablePrefixSource struct {
	mt     *Memtable
	prefix []byte
	iter   *MemtableIterator
	valid  bool
}

func (s *memtablePrefixSource) seekToPrefix() {
	s.iter = s.mt.Iterator()
	if s.iter.Seek(s.prefix) {
		s.valid = hasPrefix(s.iter.Key(), s.prefix)
	}
}

func (s *memtablePrefixSource) next() bool {
	if !s.iter.Next() {
		s.valid = false
		return false
	}
	s.valid = hasPrefix(s.iter.Key(), s.prefix)
	return s.valid
}

func (s *memtablePrefixSource) entry() Entry {
	return s.iter.Entry()
}

func (s *memtablePrefixSource) close() {
	if s.iter != nil {
		s.iter.Close()
	}
}

// sstablePrefixSource wraps an SSTable for prefix scanning.
type sstablePrefixSource struct {
	sst      *SSTable
	prefix   []byte
	cache    *LRUCache
	verify   bool
	blockIdx int
	entryIdx int
	block    *Block
	valid    bool
}

func (s *sstablePrefixSource) seekToPrefix() {
	// Check if SSTable might contain keys with this prefix
	if !hasKeyInRange(s.prefix, s.sst.Index.MinKey, s.sst.Index.MaxKey) {
		s.valid = false
		return
	}

	// Find starting block using index
	s.blockIdx = s.sst.Index.Search(s.prefix)
	if s.blockIdx < 0 {
		// Prefix is before all keys - start at block 0 if minKey has prefix
		if hasPrefix(s.sst.Index.MinKey, s.prefix) {
			s.blockIdx = 0
		} else {
			s.valid = false
			return
		}
	}

	// Load the block
	if err := s.loadBlock(); err != nil {
		s.valid = false
		return
	}

	// Find first entry >= prefix in block
	for s.entryIdx = 0; s.entryIdx < len(s.block.Entries); s.entryIdx++ {
		if CompareKeys(s.block.Entries[s.entryIdx].Key, s.prefix) >= 0 {
			s.valid = hasPrefix(s.block.Entries[s.entryIdx].Key, s.prefix)
			return
		}
	}

	// Not found in this block, try next
	s.blockIdx++
	s.entryIdx = -1
	s.valid = s.next()
}

func (s *sstablePrefixSource) loadBlock() error {
	if s.blockIdx >= len(s.sst.Index.Entries) {
		return ErrKeyNotFound
	}

	ie := s.sst.Index.Entries[s.blockIdx]
	cacheKey := CacheKey{FileID: s.sst.ID, BlockOffset: ie.BlockOffset}

	// Try cache first
	if s.cache != nil {
		if cached, found := s.cache.Get(cacheKey); found {
			s.block = cached
			return nil
		}
	}

	// Read from disk
	blockData := make([]byte, ie.BlockSize)
	if _, err := s.sst.file.ReadAt(blockData, int64(ie.BlockOffset)); err != nil {
		return err
	}

	block, err := DecodeBlock(blockData, s.verify)
	if err != nil {
		return err
	}

	if s.cache != nil {
		s.cache.Put(cacheKey, block)
	}
	s.block = block
	return nil
}

func (s *sstablePrefixSource) next() bool {
	s.entryIdx++

	// Try next entry in current block
	if s.block != nil && s.entryIdx < len(s.block.Entries) {
		s.valid = hasPrefix(s.block.Entries[s.entryIdx].Key, s.prefix)
		return s.valid
	}

	// Move to next block
	s.blockIdx++
	s.entryIdx = 0

	if s.blockIdx >= len(s.sst.Index.Entries) {
		s.valid = false
		return false
	}

	if err := s.loadBlock(); err != nil {
		s.valid = false
		return false
	}

	if len(s.block.Entries) == 0 {
		s.valid = false
		return false
	}

	s.valid = hasPrefix(s.block.Entries[0].Key, s.prefix)
	return s.valid
}

func (s *sstablePrefixSource) entry() Entry {
	if !s.valid || s.block == nil || s.entryIdx >= len(s.block.Entries) {
		return Entry{}
	}
	be := s.block.Entries[s.entryIdx]
	// Decode value from block entry
	val, _, _ := DecodeValue(be.Value)
	return Entry{
		Key:   be.Key,
		Value: val,
	}
}

func (s *sstablePrefixSource) close() {
	// Nothing to close
}

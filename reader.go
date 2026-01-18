package tinykvs

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// ScanStats tracks statistics during scan operations.
type ScanStats struct {
	BlocksLoaded int64 // Number of SSTable blocks loaded from disk or cache
	KeysExamined int64 // Total keys examined (including duplicates and tombstones)
}

// ScanProgress is called periodically during scan operations to report progress.
// Return false to stop the scan early.
type ScanProgress func(stats ScanStats) bool

// reader coordinates lookups across memtable and SSTables.
type reader struct {
	mu sync.RWMutex

	memtable   *memtable
	immutables []*memtable  // Recently flushed, waiting for SSTable write
	levels     [][]*SSTable // levels[0] = L0, levels[1] = L1, etc.
	cache      *lruCache
	opts       Options
}

// newReader creates a new reader.
func newReader(memtable *memtable, levels [][]*SSTable, cache *lruCache, opts Options) *reader {
	// Make a deep copy of levels to avoid sharing with Store
	// Store and reader use different mutexes, so sharing would cause races
	levelsCopy := make([][]*SSTable, len(levels))
	for i, level := range levels {
		levelsCopy[i] = make([]*SSTable, len(level))
		copy(levelsCopy[i], level)
	}
	return &reader{
		memtable: memtable,
		levels:   levelsCopy,
		cache:    cache,
		opts:     opts,
	}
}

// Get looks up a key, checking memtable first, then SSTables.
func (r *reader) Get(key []byte) (Value, error) {
	// Snapshot state under brief lock to minimize blocking.
	// Deep copy slices to avoid race with concurrent modifications.
	r.mu.RLock()
	memtable := r.memtable
	immutables := copyImmutables(r.immutables)
	levels := copyLevels(r.levels)
	cache := r.cache
	verify := r.opts.VerifyChecksums
	r.mu.RUnlock()

	// 1. Check active memtable
	if entry, found := memtable.Get(key); found {
		if entry.Value.IsTombstone() {
			return Value{}, ErrKeyNotFound
		}
		return entry.Value, nil
	}

	// 2. Check immutable memtables (newest first)
	for i := len(immutables) - 1; i >= 0; i-- {
		if entry, found := immutables[i].Get(key); found {
			if entry.Value.IsTombstone() {
				return Value{}, ErrKeyNotFound
			}
			return entry.Value, nil
		}
	}

	// 3. Check SSTables level by level (L0 to Lmax)
	for level := 0; level < len(levels); level++ {
		tables := levels[level]

		if level == 0 {
			// L0: Check all tables (newest first, may have overlapping keys)
			for i := len(tables) - 1; i >= 0; i-- {
				entry, found, err := tables[i].Get(key, cache, verify)
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
			idx := findTableForKey(tables, key)
			if idx >= 0 {
				entry, found, err := tables[idx].Get(key, cache, verify)
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
func findTableForKey(tables []*SSTable, key []byte) int {
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

// findTableForPrefix finds the first SSTable that may contain keys with the given prefix (for L1+).
// Returns the index of the first matching table, or -1 if no table could contain the prefix.
func findTableForPrefix(tables []*SSTable, prefix []byte) int {
	if len(tables) == 0 {
		return -1
	}

	// Binary search to find the first table where prefix might exist
	lo, hi := 0, len(tables)-1
	result := -1

	for lo <= hi {
		mid := (lo + hi) / 2
		maxKey := tables[mid].MaxKey()

		// If prefix <= maxKey (prefix-wise), this table or an earlier one might contain matches
		prefixBeforeOrAtMax := true
		if len(maxKey) >= len(prefix) {
			for i := 0; i < len(prefix); i++ {
				if prefix[i] < maxKey[i] {
					break
				} else if prefix[i] > maxKey[i] {
					prefixBeforeOrAtMax = false
					break
				}
			}
		}

		if prefixBeforeOrAtMax {
			// This table might contain matches, but check if there's an earlier one
			if hasKeyInRange(prefix, tables[mid].MinKey(), maxKey) {
				result = mid
			}
			hi = mid - 1
		} else {
			// Prefix is after this table's maxKey, look right
			lo = mid + 1
		}
	}

	return result
}

// Setmemtable updates the active memtable.
func (r *reader) Setmemtable(mt *memtable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.memtable = mt
}

// AddImmutable adds an immutable memtable.
func (r *reader) AddImmutable(mt *memtable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.immutables = append(r.immutables, mt)
}

// RemoveImmutable removes an immutable memtable after it's been flushed.
func (r *reader) RemoveImmutable(mt *memtable) {
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
func (r *reader) SetLevels(levels [][]*SSTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.levels = levels
}

// AddSSTable adds an SSTable to a level.
func (r *reader) AddSSTable(level int, sst *SSTable) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ensure the level exists
	for len(r.levels) <= level {
		r.levels = append(r.levels, nil)
	}

	r.levels[level] = append(r.levels[level], sst)
}

// GetLevels returns a copy of the current levels.
func (r *reader) GetLevels() [][]*SSTable {
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
// Keys and values passed to the callback are copies owned by the caller.
// Note: Streams entries directly without buffering for constant memory usage.
func (r *reader) ScanPrefix(prefix []byte, fn func(key []byte, value Value) bool) error {
	// Snapshot state under brief lock to minimize blocking.
	// Deep copy slices to avoid race with concurrent modifications.
	r.mu.RLock()
	memtable := r.memtable
	immutables := copyImmutables(r.immutables)
	levels := copyLevels(r.levels)
	cache := r.cache
	verify := r.opts.VerifyChecksums
	r.mu.RUnlock()

	scanner := newPrefixScanner(prefix, cache, verify)

	scanner.addmemtable(memtable, 0)

	for i := len(immutables) - 1; i >= 0; i-- {
		scanner.addmemtable(immutables[i], len(immutables)-i)
	}

	baseIdx := len(immutables) + 1
	for level := 0; level < len(levels); level++ {
		tables := levels[level]
		if level == 0 {
			// L0: tables may overlap, check all
			for i := len(tables) - 1; i >= 0; i-- {
				scanner.addSSTable(tables[i], baseIdx)
				baseIdx++
			}
		} else {
			// L1+: tables are sorted and non-overlapping
			// Binary search to find starting table, then check subsequent tables
			startIdx := findTableForPrefix(tables, prefix)
			if startIdx >= 0 {
				for i := startIdx; i < len(tables); i++ {
					if hasKeyInRange(prefix, tables[i].MinKey(), tables[i].MaxKey()) {
						scanner.addSSTable(tables[i], baseIdx)
						baseIdx++
					} else {
						// Tables are sorted, no more matches possible
						break
					}
				}
			}
		}
	}

	scanner.init()

	var lastKey []byte

	for scanner.next() {
		entry := scanner.entry()

		// Skip duplicates (newer version already seen)
		if lastKey != nil && CompareKeys(entry.Key, lastKey) == 0 {
			continue
		}

		lastKey = make([]byte, len(entry.Key))
		copy(lastKey, entry.Key)

		// Skip tombstones
		if entry.Value.IsTombstone() {
			continue
		}

		// Check prefix still matches
		if !hasPrefix(entry.Key, prefix) {
			break
		}

		// Stream directly to callback
		keyCopy := make([]byte, len(entry.Key))
		copy(keyCopy, entry.Key)

		if !fn(keyCopy, copyValue(entry.Value)) {
			break
		}
	}

	scanner.close()
	return nil
}

// ScanPrefixWithStats is like ScanPrefix but also returns scan statistics.
// The progress callback (if non-nil) is called periodically during the scan
// with current stats. Return false from progress to stop the scan early.
// Note: This streams entries directly to the callback without buffering,
// so it uses constant memory regardless of result size.
func (r *reader) ScanPrefixWithStats(prefix []byte, fn func(key []byte, value Value) bool, progress ScanProgress) (ScanStats, error) {
	// Snapshot state under brief lock to minimize blocking.
	// Deep copy slices to avoid race with concurrent modifications.
	r.mu.RLock()
	memtable := r.memtable
	immutables := copyImmutables(r.immutables)
	levels := copyLevels(r.levels)
	cache := r.cache
	verify := r.opts.VerifyChecksums
	r.mu.RUnlock()

	scanner := newPrefixScanner(prefix, cache, verify)

	scanner.addmemtable(memtable, 0)

	for i := len(immutables) - 1; i >= 0; i-- {
		scanner.addmemtable(immutables[i], len(immutables)-i)
	}

	baseIdx := len(immutables) + 1
	for level := 0; level < len(levels); level++ {
		tables := levels[level]
		if level == 0 {
			// L0: tables may overlap, check all
			for i := len(tables) - 1; i >= 0; i-- {
				scanner.addSSTable(tables[i], baseIdx)
				baseIdx++
			}
		} else {
			// L1+: tables are sorted and non-overlapping
			// Binary search to find starting table, then check subsequent tables
			startIdx := findTableForPrefix(tables, prefix)
			if startIdx >= 0 {
				for i := startIdx; i < len(tables); i++ {
					if hasKeyInRange(prefix, tables[i].MinKey(), tables[i].MaxKey()) {
						scanner.addSSTable(tables[i], baseIdx)
						baseIdx++
					} else {
						// Tables are sorted, no more matches possible
						break
					}
				}
			}
		}
	}

	scanner.init()

	var lastKey []byte
	var progressCount int64

	for scanner.next() {
		entry := scanner.entry()

		// Call progress every 10000 keys
		progressCount++
		if progress != nil && progressCount%10000 == 0 {
			if !progress(scanner.stats) {
				break
			}
		}

		// Skip duplicates (newer version already seen)
		if lastKey != nil && CompareKeys(entry.Key, lastKey) == 0 {
			continue
		}

		lastKey = make([]byte, len(entry.Key))
		copy(lastKey, entry.Key)

		// Skip tombstones
		if entry.Value.IsTombstone() {
			continue
		}

		// Check prefix still matches
		if !hasPrefix(entry.Key, prefix) {
			break
		}

		// Stream directly to callback (no buffering)
		keyCopy := make([]byte, len(entry.Key))
		copy(keyCopy, entry.Key)

		if !fn(keyCopy, copyValue(entry.Value)) {
			break
		}
	}

	stats := scanner.stats
	scanner.close()

	return stats, nil
}

// copyValue creates a deep copy of a Value.
func copyValue(v Value) Value {
	result := Value{
		Type:    v.Type,
		Int64:   v.Int64,
		Float64: v.Float64,
		Bool:    v.Bool,
	}
	if v.Bytes != nil {
		result.Bytes = make([]byte, len(v.Bytes))
		copy(result.Bytes, v.Bytes)
	}
	if v.Pointer != nil {
		result.Pointer = &dataPointer{
			FileID:      v.Pointer.FileID,
			BlockOffset: v.Pointer.BlockOffset,
			DataOffset:  v.Pointer.DataOffset,
			Length:      v.Pointer.Length,
		}
	}
	if v.Record != nil {
		result.Record = make(map[string]any, len(v.Record))
		for k, val := range v.Record {
			result.Record[k] = val
		}
	}
	return result
}

// copyLevels creates a deep copy of the levels slice.
// This is needed to prevent race conditions when readers snapshot state
// while writers are modifying the levels.
func copyLevels(levels [][]*SSTable) [][]*SSTable {
	result := make([][]*SSTable, len(levels))
	for i, level := range levels {
		result[i] = make([]*SSTable, len(level))
		copy(result[i], level)
	}
	return result
}

// copyImmutables creates a copy of the immutables slice.
func copyImmutables(immutables []*memtable) []*memtable {
	result := make([]*memtable, len(immutables))
	copy(result, immutables)
	return result
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
	cache   *lruCache
	verify  bool
	heap    prefixHeap
	current Entry
	stats   ScanStats // Tracks blocks loaded and keys examined
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

func newPrefixScanner(prefix []byte, cache *lruCache, verify bool) *prefixScanner {
	return &prefixScanner{
		prefix: prefix,
		cache:  cache,
		verify: verify,
		heap:   make(prefixHeap, 0, 8),
	}
}

func (s *prefixScanner) addmemtable(mt *memtable, priority int) {
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
		stats:  &s.stats,
	}
	src.seekToPrefix()
	if src.valid {
		entry := src.entry()
		// Debug: ALWAYS log initial entry for debugging
		if len(s.prefix) > 0 && !hasPrefix(entry.Key, s.prefix) {
			fmt.Fprintf(os.Stderr, "[DEBUG] SSTable %d (priority %d) adding NON-MATCHING initial key %x (prefix %x)\n",
				sst.ID, priority, entry.Key, s.prefix)
			fmt.Fprintf(os.Stderr, "[DEBUG]   minKey=%x maxKey=%x blockIdx=%d entryIdx=%d\n",
				sst.MinKey(), sst.MaxKey(), src.blockIdx, src.entryIdx)
		}
		s.heap = append(s.heap, prefixHeapEntry{
			entry:    entry,
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

var debugPushCount int

func (s *prefixScanner) next() bool {
	if len(s.heap) == 0 {
		return false
	}

	he := s.heap.pop()
	s.current = he.entry
	s.stats.KeysExamined++

	// Debug: verify the popped entry matches our prefix
	if len(s.prefix) > 0 && !hasPrefix(he.entry.Key, s.prefix) {
		fmt.Fprintf(os.Stderr, "[DEBUG] POPPED non-matching key %x (prefix %x) at priority %d\n",
			he.entry.Key, s.prefix, he.priority)
	}

	if he.source.next() {
		newEntry := he.source.entry()
		debugPushCount++
		// Debug: check if source is pushing non-matching key
		if len(s.prefix) > 0 && !hasPrefix(newEntry.Key, s.prefix) {
			fmt.Fprintf(os.Stderr, "[DEBUG] Push %d: non-matching key %x after %x (prefix %x)\n",
				debugPushCount, newEntry.Key, he.entry.Key, s.prefix)
		}
		// Debug: check if key ordering is violated (new key < old key is bad for same source)
		if len(s.prefix) > 0 && CompareKeys(newEntry.Key, he.entry.Key) < 0 {
			fmt.Fprintf(os.Stderr, "[DEBUG] Push %d: KEY ORDER VIOLATION! new=%x < old=%x\n",
				debugPushCount, newEntry.Key, he.entry.Key)
		}
		s.heap.push(prefixHeapEntry{
			entry:    newEntry,
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
	mt     *memtable
	prefix []byte
	iter   *memtableIterator
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
	sst       *SSTable
	prefix    []byte
	cache     *lruCache
	verify    bool
	blockIdx  int
	entryIdx  int
	block     *Block
	valid     bool
	fromCache bool       // True if current block came from cache (don't release it)
	stats     *ScanStats // Shared stats counter (may be nil)
}

func (s *sstablePrefixSource) seekToPrefix() {
	// Ensure index is loaded for lazy-loaded SSTables
	if err := s.sst.ensureIndex(); err != nil {
		s.valid = false
		return
	}

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

	// Not found in this block - keep scanning subsequent blocks
	for {
		// Release old block and clear it
		if s.block != nil && !s.fromCache {
			s.block.Release()
		}
		s.block = nil
		s.blockIdx++

		if s.blockIdx >= len(s.sst.Index.Entries) {
			s.valid = false
			return
		}

		if err := s.loadBlock(); err != nil {
			s.valid = false
			return
		}

		if len(s.block.Entries) == 0 {
			continue
		}

		// Find first entry >= prefix in this block
		for s.entryIdx = 0; s.entryIdx < len(s.block.Entries); s.entryIdx++ {
			key := s.block.Entries[s.entryIdx].Key
			cmp := CompareKeys(key, s.prefix)
			if cmp >= 0 {
				// Found entry >= prefix, check if it matches
				s.valid = hasPrefix(key, s.prefix)
				return
			}
		}
		// All entries in this block are < prefix, continue to next block
	}
}

func (s *sstablePrefixSource) loadBlock() error {
	if s.blockIdx >= len(s.sst.Index.Entries) {
		return ErrKeyNotFound
	}

	// Release previous block if we owned it
	if s.block != nil && !s.fromCache {
		s.block.Release()
		s.block = nil
	}

	ie := s.sst.Index.Entries[s.blockIdx]
	cacheKey := cacheKey{FileID: s.sst.ID, BlockOffset: ie.BlockOffset}

	// Try cache first
	if s.cache != nil {
		if cached, found := s.cache.Get(cacheKey); found {
			s.block = cached
			s.fromCache = true
			if s.stats != nil {
				atomic.AddInt64(&s.stats.BlocksLoaded, 1)
			}
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
		s.fromCache = true // Now in cache, don't release
	} else {
		s.fromCache = false // Not cached, we own it
	}
	s.block = block
	if s.stats != nil {
		atomic.AddInt64(&s.stats.BlocksLoaded, 1)
	}
	return nil
}

func (s *sstablePrefixSource) next() bool {
	s.entryIdx++

	// Try next entry in current block
	if s.block != nil && s.entryIdx < len(s.block.Entries) {
		key := s.block.Entries[s.entryIdx].Key
		s.valid = hasPrefix(key, s.prefix)
		// Debug: if returning true but key doesn't match, that's a bug
		if s.valid && len(s.prefix) > 0 && len(key) > 0 && key[0] != s.prefix[0] {
			fmt.Fprintf(os.Stderr, "[BUG] next() in-block: returning true, key=%x prefix=%x sst=%d block=%d entry=%d\n",
				key, s.prefix, s.sst.ID, s.blockIdx, s.entryIdx)
		}
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

	key := s.block.Entries[0].Key
	s.valid = hasPrefix(key, s.prefix)
	// Debug: if returning true but key doesn't match, that's a bug
	if s.valid && len(s.prefix) > 0 && len(key) > 0 && key[0] != s.prefix[0] {
		fmt.Fprintf(os.Stderr, "[BUG] next() new-block: returning true, key=%x prefix=%x sst=%d block=%d\n",
			key, s.prefix, s.sst.ID, s.blockIdx)
	}
	return s.valid
}

func (s *sstablePrefixSource) entry() Entry {
	if !s.valid || s.block == nil || s.entryIdx >= len(s.block.Entries) {
		return Entry{}
	}
	be := s.block.Entries[s.entryIdx]
	// Decode value from block entry
	val, _, _ := DecodeValue(be.Value)
	// IMPORTANT: Copy the key since the block may be released later
	keyCopy := make([]byte, len(be.Key))
	copy(keyCopy, be.Key)
	return Entry{
		Key:   keyCopy,
		Value: val,
	}
}

func (s *sstablePrefixSource) close() {
	// Release block if we own it (not from cache)
	if s.block != nil && !s.fromCache {
		s.block.Release()
		s.block = nil
	}
}

// ScanRange iterates over all keys in [start, end) in sorted order.
// Keys are deduplicated (newest version wins) and tombstones are skipped.
// Return false from the callback to stop iteration early.
func (r *reader) ScanRange(start, end []byte, fn func(key []byte, value Value) bool) error {
	// Snapshot state under brief lock to minimize blocking.
	// Deep copy slices to avoid race with concurrent modifications.
	r.mu.RLock()
	memtable := r.memtable
	immutables := copyImmutables(r.immutables)
	levels := copyLevels(r.levels)
	cache := r.cache
	verify := r.opts.VerifyChecksums
	r.mu.RUnlock()

	// Build a range scanner with all sources
	scanner := newRangeScanner(start, end, cache, verify)

	// Add memtable (highest priority - index 0)
	scanner.addMemtable(memtable, 0)

	// Add immutable memtables (newest first)
	for i := len(immutables) - 1; i >= 0; i-- {
		scanner.addMemtable(immutables[i], len(immutables)-i)
	}

	// Add SSTable levels
	baseIdx := len(immutables) + 1
	for level := 0; level < len(levels); level++ {
		tables := levels[level]
		if level == 0 {
			// L0: add all tables that may overlap with range (newest first)
			for i := len(tables) - 1; i >= 0; i-- {
				if rangeOverlaps(start, end, tables[i].MinKey(), tables[i].MaxKey()) {
					scanner.addSSTable(tables[i], baseIdx)
					baseIdx++
				}
			}
		} else {
			// L1+: add tables that overlap with range
			for _, t := range tables {
				if rangeOverlaps(start, end, t.MinKey(), t.MaxKey()) {
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

		// Check if we've passed the end
		if CompareKeys(entry.Key, end) >= 0 {
			break
		}

		if !fn(entry.Key, entry.Value) {
			break
		}
	}

	scanner.close()
	return nil
}

// rangeOverlaps returns true if [start, end) overlaps with [minKey, maxKey].
func rangeOverlaps(start, end, minKey, maxKey []byte) bool {
	// Range overlaps if start < maxKey AND end > minKey
	// i.e., NOT (start >= maxKey OR end <= minKey)
	if CompareKeys(start, maxKey) > 0 {
		return false
	}
	if CompareKeys(end, minKey) <= 0 {
		return false
	}
	return true
}

// rangeScanner merges entries from memtables and SSTables for range scanning.
type rangeScanner struct {
	start   []byte
	end     []byte
	cache   *lruCache
	verify  bool
	heap    rangeHeap
	current Entry
	stats   ScanStats
}

type rangeHeapEntry struct {
	entry    Entry
	priority int // lower = newer/higher priority
	source   rangeSource
}

type rangeSource interface {
	next() bool
	entry() Entry
	close()
}

type rangeHeap []rangeHeapEntry

func (h rangeHeap) less(i, j int) bool {
	cmp := CompareKeys(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}

func (h *rangeHeap) push(x rangeHeapEntry) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *rangeHeap) pop() rangeHeapEntry {
	old := *h
	n := len(old) - 1
	old[0], old[n] = old[n], old[0]
	h.down(0, n)
	x := old[n]
	*h = old[:n]
	return x
}

func (h rangeHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.less(j, i) {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func (h rangeHeap) down(i, n int) {
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

func (h *rangeHeap) init() {
	n := len(*h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func newRangeScanner(start, end []byte, cache *lruCache, verify bool) *rangeScanner {
	return &rangeScanner{
		start:  start,
		end:    end,
		cache:  cache,
		verify: verify,
		heap:   make(rangeHeap, 0, 8),
	}
}

func (s *rangeScanner) addMemtable(mt *memtable, priority int) {
	src := &memtableRangeSource{mt: mt, start: s.start, end: s.end}
	src.seekToStart()
	if src.valid {
		s.heap = append(s.heap, rangeHeapEntry{
			entry:    src.entry(),
			priority: priority,
			source:   src,
		})
	} else {
		src.close()
	}
}

func (s *rangeScanner) addSSTable(sst *SSTable, priority int) {
	src := &sstableRangeSource{
		sst:    sst,
		start:  s.start,
		end:    s.end,
		cache:  s.cache,
		verify: s.verify,
		stats:  &s.stats,
	}
	src.seekToStart()
	if src.valid {
		s.heap = append(s.heap, rangeHeapEntry{
			entry:    src.entry(),
			priority: priority,
			source:   src,
		})
	} else {
		src.close()
	}
}

func (s *rangeScanner) init() {
	s.heap.init()
}

func (s *rangeScanner) next() bool {
	if len(s.heap) == 0 {
		return false
	}

	he := s.heap.pop()
	s.current = he.entry
	s.stats.KeysExamined++

	if he.source.next() {
		s.heap.push(rangeHeapEntry{
			entry:    he.source.entry(),
			priority: he.priority,
			source:   he.source,
		})
	} else {
		he.source.close()
	}

	return true
}

func (s *rangeScanner) entry() Entry {
	return s.current
}

func (s *rangeScanner) close() {
	for _, he := range s.heap {
		he.source.close()
	}
}

// memtableRangeSource wraps a memtable iterator for range scanning.
type memtableRangeSource struct {
	mt    *memtable
	start []byte
	end   []byte
	iter  *memtableIterator
	valid bool
}

func (s *memtableRangeSource) seekToStart() {
	s.iter = s.mt.Iterator()
	if s.iter.Seek(s.start) {
		key := s.iter.Key()
		s.valid = CompareKeys(key, s.end) < 0
	}
}

func (s *memtableRangeSource) next() bool {
	if !s.iter.Next() {
		s.valid = false
		return false
	}
	key := s.iter.Key()
	s.valid = CompareKeys(key, s.end) < 0
	return s.valid
}

func (s *memtableRangeSource) entry() Entry {
	return s.iter.Entry()
}

func (s *memtableRangeSource) close() {
	if s.iter != nil {
		s.iter.Close()
	}
}

// sstableRangeSource wraps an SSTable for range scanning.
type sstableRangeSource struct {
	sst       *SSTable
	start     []byte
	end       []byte
	cache     *lruCache
	verify    bool
	blockIdx  int
	entryIdx  int
	block     *Block
	valid     bool
	fromCache bool
	stats     *ScanStats
}

func (s *sstableRangeSource) seekToStart() {
	// Ensure index is loaded for lazy-loaded SSTables
	if err := s.sst.ensureIndex(); err != nil {
		s.valid = false
		return
	}

	// Check if SSTable might contain keys in range
	if !rangeOverlaps(s.start, s.end, s.sst.Index.MinKey, s.sst.Index.MaxKey) {
		s.valid = false
		return
	}

	// Find starting block using index
	s.blockIdx = s.sst.Index.Search(s.start)
	if s.blockIdx < 0 {
		s.blockIdx = 0
	}

	// Load the block
	if err := s.loadBlock(); err != nil {
		s.valid = false
		return
	}

	// Find first entry >= start in block
	for s.entryIdx = 0; s.entryIdx < len(s.block.Entries); s.entryIdx++ {
		key := s.block.Entries[s.entryIdx].Key
		if CompareKeys(key, s.start) >= 0 {
			s.valid = CompareKeys(key, s.end) < 0
			return
		}
	}

	// Not found in this block, try next block
	// Release old block and clear it so we load the new block
	if s.block != nil && !s.fromCache {
		s.block.Release()
	}
	s.block = nil
	s.blockIdx++
	s.entryIdx = 0

	// Load and check the next block directly (don't call next() which would increment blockIdx again)
	if s.blockIdx >= len(s.sst.Index.Entries) {
		s.valid = false
		return
	}

	if err := s.loadBlock(); err != nil {
		s.valid = false
		return
	}

	if len(s.block.Entries) == 0 {
		s.valid = false
		return
	}

	// Check if first entry is in range
	key := s.block.Entries[0].Key
	s.valid = CompareKeys(key, s.end) < 0
}

func (s *sstableRangeSource) loadBlock() error {
	if s.blockIdx >= len(s.sst.Index.Entries) {
		return ErrKeyNotFound
	}

	// Release previous block if we owned it
	if s.block != nil && !s.fromCache {
		s.block.Release()
		s.block = nil
	}

	ie := s.sst.Index.Entries[s.blockIdx]
	cacheKey := cacheKey{FileID: s.sst.ID, BlockOffset: ie.BlockOffset}

	// Try cache first
	if s.cache != nil {
		if cached, found := s.cache.Get(cacheKey); found {
			s.block = cached
			s.fromCache = true
			if s.stats != nil {
				atomic.AddInt64(&s.stats.BlocksLoaded, 1)
			}
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
		s.fromCache = true
	} else {
		s.fromCache = false
	}
	s.block = block
	if s.stats != nil {
		atomic.AddInt64(&s.stats.BlocksLoaded, 1)
	}
	return nil
}

func (s *sstableRangeSource) next() bool {
	s.entryIdx++

	// Try next entry in current block
	if s.block != nil && s.entryIdx < len(s.block.Entries) {
		key := s.block.Entries[s.entryIdx].Key
		s.valid = CompareKeys(key, s.end) < 0
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

	key := s.block.Entries[0].Key
	s.valid = CompareKeys(key, s.end) < 0
	return s.valid
}

func (s *sstableRangeSource) entry() Entry {
	if !s.valid || s.block == nil || s.entryIdx >= len(s.block.Entries) {
		return Entry{}
	}
	be := s.block.Entries[s.entryIdx]
	val, _, _ := DecodeValue(be.Value)
	// IMPORTANT: Copy the key since the block may be released later
	keyCopy := make([]byte, len(be.Key))
	copy(keyCopy, be.Key)
	return Entry{
		Key:   keyCopy,
		Value: val,
	}
}

func (s *sstableRangeSource) close() {
	if s.block != nil && !s.fromCache {
		s.block.Release()
		s.block = nil
	}
}

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

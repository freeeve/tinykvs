package tinykvs

import "sort"

// mergeIterator performs k-way merge over multiple SSTables.
type mergeIterator struct {
	tables    []*SSTable
	iterators []*sstableIterator
	heap      *entryHeap
	cache     *lruCache
	verify    bool
	current   Entry
	lastKey   []byte
}

type sstableIterator struct {
	sst        *SSTable
	blockIdx   int
	entryIdx   int
	block      *Block
	verify     bool
	tableIndex int // For stable sorting (lower = newer)
}

type heapEntry struct {
	entry      Entry
	tableIndex int
	iterator   *sstableIterator
}

type entryHeap []heapEntry

func (h entryHeap) less(i, j int) bool {
	cmp := CompareKeys(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	// Same key: prefer lower tableIndex (newer)
	return h[i].tableIndex < h[j].tableIndex
}

// Inline heap operations to avoid interface{} boxing allocations

func (h *entryHeap) push(x heapEntry) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *entryHeap) pop() heapEntry {
	old := *h
	n := len(old) - 1
	old[0], old[n] = old[n], old[0]
	h.down(0, n)
	x := old[n]
	*h = old[:n]
	return x
}

func (h entryHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func (h entryHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.less(j, i) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

func (h *entryHeap) init() {
	n := len(*h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func newMergeIterator(tables []*SSTable, cache *lruCache, verify bool) *mergeIterator {
	m := &mergeIterator{
		tables: tables,
		cache:  cache,
		verify: verify,
		heap:   &entryHeap{},
	}

	// Initialize iterators and push first entry from each
	// Note: Iterators don't use the cache to avoid use-after-free issues
	for i, t := range tables {
		// Ensure index is loaded for lazy-loaded SSTables
		if err := t.ensureIndex(); err != nil {
			continue // Skip tables that can't be loaded
		}

		iter := &sstableIterator{
			sst:        t,
			blockIdx:   0,
			entryIdx:   -1,
			verify:     verify,
			tableIndex: i,
		}
		m.iterators = append(m.iterators, iter)

		if iter.Next() {
			*m.heap = append(*m.heap, heapEntry{
				entry:      iter.Entry(),
				tableIndex: i,
				iterator:   iter,
			})
		}
	}
	m.heap.init()

	return m
}

func (m *mergeIterator) Next() bool {
	for len(*m.heap) > 0 {
		// Pop minimum (no interface{} boxing)
		he := m.heap.pop()
		m.current = he.entry

		// Advance that iterator
		if he.iterator.Next() {
			m.heap.push(heapEntry{
				entry:      he.iterator.Entry(),
				tableIndex: he.tableIndex,
				iterator:   he.iterator,
			})
		}

		// Skip duplicates (keep first, which is newest)
		if m.lastKey != nil && CompareKeys(m.current.Key, m.lastKey) == 0 {
			continue
		}

		m.lastKey = m.current.Key
		return true
	}
	return false
}

func (m *mergeIterator) Entry() Entry {
	return m.current
}

func (m *mergeIterator) Close() {
	// Close all underlying iterators to release their blocks
	for _, iter := range m.iterators {
		iter.Close()
	}
}

func (it *sstableIterator) Next() bool {
	// Advance within current block
	if it.block != nil {
		it.entryIdx++
		if it.entryIdx < len(it.block.Entries) {
			return true
		}
		// Done with current block - always release since we own all blocks
		it.block.Release()
		it.block = nil
	}

	// Move to next block
	for {
		if it.blockIdx >= len(it.sst.Index.Entries) {
			return false
		}

		indexEntry := it.sst.Index.Entries[it.blockIdx]
		it.blockIdx++

		// Note: We intentionally don't use the cache during iteration.
		// This avoids use-after-free issues if cached blocks get evicted
		// while we're still iterating over them. We own all blocks we create.

		// Read from disk
		blockData := make([]byte, indexEntry.BlockSize)
		if _, err := it.sst.file.ReadAt(blockData, int64(indexEntry.BlockOffset)); err != nil {
			return false
		}

		block, err := DecodeBlock(blockData, it.verify)
		if err != nil {
			return false
		}

		// Skip empty blocks
		if len(block.Entries) == 0 {
			block.Release()
			continue
		}

		it.block = block
		it.entryIdx = 0
		return true
	}
}

// Close releases resources held by the iterator.
func (it *sstableIterator) Close() {
	if it.block != nil {
		it.block.Release()
		it.block = nil
	}
}

func (it *sstableIterator) Entry() Entry {
	if it.block == nil || it.entryIdx >= len(it.block.Entries) {
		return Entry{}
	}

	be := it.block.Entries[it.entryIdx]
	// Make copies of key and value since the block may be released
	// while this entry is still on the merge heap
	keyCopy := make([]byte, len(be.Key))
	copy(keyCopy, be.Key)

	// Decode value (this already makes a copy for non-bytes values)
	value, _, _ := DecodeValue(be.Value)

	return Entry{
		Key:   keyCopy,
		Value: value,
	}
}

// sortTablesByMinKey sorts tables by their minimum key.
func sortTablesByMinKey(tables []*SSTable) {
	sort.Slice(tables, func(i, j int) bool {
		return CompareKeys(tables[i].MinKey(), tables[j].MinKey()) < 0
	})
}

// sortTablesByID sorts tables by their ID (oldest first = lowest ID first).
// This is critical for L0 tables to ensure correct compaction ordering.
func sortTablesByID(tables []*SSTable) {
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].ID < tables[j].ID
	})
}

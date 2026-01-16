package tinykvs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Writer handles all write operations including flushes and compaction.
type Writer struct {
	store    *Store
	memtable *Memtable
	wal      *WAL
	reader   *Reader
	sequence uint64 // Atomic counter for sequence numbers

	// Flush management
	flushMu    sync.Mutex
	flushCond  *sync.Cond // For backpressure when too many immutables
	immutables []*Memtable
	flushCh    chan struct{}

	// Compaction management
	compactMu sync.Mutex
	compactCh chan compactionTask

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type compactionTask struct {
	level int
}

// MaxImmutableMemtables is the max number of memtables waiting to be flushed.
// When this limit is reached, writes will block until a flush completes.
const MaxImmutableMemtables = 2

// NewWriter creates a new writer.
func NewWriter(store *Store, memtable *Memtable, wal *WAL, reader *Reader) *Writer {
	ctx, cancel := context.WithCancel(context.Background())

	w := &Writer{
		store:     store,
		memtable:  memtable,
		wal:       wal,
		reader:    reader,
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan compactionTask, 10),
		ctx:       ctx,
		cancel:    cancel,
	}
	w.flushCond = sync.NewCond(&w.flushMu)

	return w
}

// Start starts background goroutines for flush and compaction.
func (w *Writer) Start() {
	w.wg.Add(2)
	go w.flushLoop()
	go w.compactionLoop()
}

// Stop stops background goroutines.
func (w *Writer) Stop() {
	w.cancel()
	w.wg.Wait()
}

// Put writes a key-value pair.
func (w *Writer) Put(key []byte, value Value) error {
	seq := atomic.AddUint64(&w.sequence, 1)

	// Write to WAL first
	entry := WALEntry{
		Operation: OpPut,
		Key:       key,
		Value:     value,
		Sequence:  seq,
	}
	if err := w.wal.Append(entry); err != nil {
		return err
	}

	// Write to memtable
	w.memtable.Put(key, value, seq)

	// Check if flush is needed
	if w.memtable.Size() >= w.store.opts.MemtableSize {
		w.triggerFlush()
	}

	return nil
}

// Delete marks a key as deleted.
func (w *Writer) Delete(key []byte) error {
	return w.Put(key, TombstoneValue())
}

// triggerFlush initiates an async flush.
// Blocks if too many immutable memtables are pending (backpressure).
func (w *Writer) triggerFlush() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	// Backpressure: wait if too many immutables pending
	for len(w.immutables) >= MaxImmutableMemtables {
		w.flushCond.Wait()
	}

	// Move current memtable to immutables
	w.immutables = append(w.immutables, w.memtable)
	w.reader.AddImmutable(w.memtable)

	// Create new memtable
	w.memtable = NewMemtable()
	w.reader.SetMemtable(w.memtable)

	// Signal flush goroutine
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
}

// Flush forces a synchronous flush of all data to disk.
func (w *Writer) Flush() error {
	w.flushMu.Lock()
	if w.memtable.Count() > 0 {
		w.immutables = append(w.immutables, w.memtable)
		w.reader.AddImmutable(w.memtable)
		w.memtable = NewMemtable()
		w.reader.SetMemtable(w.memtable)
	}
	imm := make([]*Memtable, len(w.immutables))
	copy(imm, w.immutables)
	w.flushMu.Unlock()

	// Flush all immutables
	for _, mt := range imm {
		if err := w.flushMemtable(mt); err != nil {
			return err
		}
	}

	// Sync WAL
	return w.wal.Sync()
}

// flushMemtable writes a memtable to an SSTable.
func (w *Writer) flushMemtable(mt *Memtable) error {
	if mt.Count() == 0 {
		return nil
	}

	// Generate SSTable filename
	id := w.store.nextSSTableID()
	path := filepath.Join(w.store.opts.Dir, fmt.Sprintf("%06d.sst", id))

	// Create writer
	writer, err := NewSSTableWriter(id, path, uint(mt.Count()), w.store.opts)
	if err != nil {
		return err
	}

	// Iterate memtable and write entries
	iter := mt.Iterator()
	for iter.Next() {
		if err := writer.Add(iter.Entry()); err != nil {
			iter.Close()
			writer.Abort()
			return err
		}
	}
	iter.Close()

	// Finish SSTable
	if err := writer.Finish(0); err != nil { // Level 0
		writer.Abort()
		return err
	}
	writer.Close()

	// Open the new SSTable and add to L0
	sst, err := OpenSSTable(id, path)
	if err != nil {
		return err
	}

	w.store.addSSTable(0, sst)
	w.reader.AddSSTable(0, sst)

	// Add to manifest
	if w.store.manifest != nil {
		meta := &TableMeta{
			ID:          sst.ID,
			Level:       0,
			MinKey:      sst.MinKey(),
			MaxKey:      sst.MaxKey(),
			NumKeys:     sst.Footer.NumKeys,
			FileSize:    int64(sst.Footer.FileSize),
			IndexOffset: sst.Footer.IndexOffset,
			IndexSize:   sst.Footer.IndexSize,
			BloomOffset: sst.Footer.BloomOffset,
			BloomSize:   sst.Footer.BloomSize,
		}
		if err := w.store.manifest.AddTable(meta); err != nil {
			return fmt.Errorf("failed to add table to manifest: %w", err)
		}
	}

	// Remove from immutables and signal waiting writers
	w.flushMu.Lock()
	for i, imm := range w.immutables {
		if imm == mt {
			w.immutables = append(w.immutables[:i], w.immutables[i+1:]...)
			break
		}
	}
	w.flushCond.Signal() // Wake up any blocked writers
	w.flushMu.Unlock()
	w.reader.RemoveImmutable(mt)

	// Truncate WAL entries that have been flushed
	w.flushMu.Lock()
	if len(w.immutables) == 0 {
		// All flushed, fully truncate
		w.wal.Truncate()
	} else {
		// Find minimum sequence of remaining immutables
		minSeq := w.immutables[0].MinSequence()
		for _, imm := range w.immutables[1:] {
			if seq := imm.MinSequence(); seq < minSeq {
				minSeq = seq
			}
		}
		// Truncate entries before the oldest unflushed memtable
		w.wal.TruncateBefore(minSeq)
	}
	w.flushMu.Unlock()

	// Aggressive memory cleanup after flush
	runtime.GC()
	debug.FreeOSMemory()

	// Check if compaction needed
	w.maybeScheduleCompaction()

	return nil
}

// Background flush goroutine
func (w *Writer) flushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.store.opts.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.flushCh:
			w.flushImmutables()
		case <-ticker.C:
			w.flushImmutables()
		}
	}
}

func (w *Writer) flushImmutables() {
	w.flushMu.Lock()
	imm := make([]*Memtable, len(w.immutables))
	copy(imm, w.immutables)
	w.flushMu.Unlock()

	for _, mt := range imm {
		if err := w.flushMemtable(mt); err != nil {
			// Log error, will retry on next tick
			continue
		}
	}
}

// Compaction

func (w *Writer) maybeScheduleCompaction() {
	levels := w.reader.GetLevels()

	// Check L0 trigger
	if len(levels) > 0 && len(levels[0]) >= w.store.opts.L0CompactionTrigger {
		select {
		case w.compactCh <- compactionTask{level: 0}:
		default:
		}
	}

	// Check other levels
	for level := 1; level < len(levels); level++ {
		levelSize := w.levelSize(levels[level])
		maxSize := w.maxLevelSize(level)
		if levelSize > maxSize {
			select {
			case w.compactCh <- compactionTask{level: level}:
			default:
			}
		}
	}
}

// compactionLoop runs background compaction.
func (w *Writer) compactionLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.store.opts.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-w.compactCh:
			w.runCompaction(task)
		case <-ticker.C:
			w.maybeScheduleCompaction()
		}
	}
}

// runCompaction performs leveled compaction.
func (w *Writer) runCompaction(task compactionTask) {
	if task.level == 0 {
		w.compactL0ToL1()
	} else {
		w.compactLevelToNext(task.level)
	}
}

func (w *Writer) compactL0ToL1() {
	w.compactMu.Lock()
	defer w.compactMu.Unlock()

	levels := w.reader.GetLevels()
	if len(levels) == 0 || len(levels[0]) == 0 {
		return
	}

	l0Tables := levels[0]

	// Limit batch size if configured
	if w.store.opts.L0CompactionBatchSize > 0 && len(l0Tables) > w.store.opts.L0CompactionBatchSize {
		// Take oldest tables (at the beginning of the slice)
		l0Tables = l0Tables[:w.store.opts.L0CompactionBatchSize]
	}

	// Find key range of L0
	var minKey, maxKey []byte
	for _, t := range l0Tables {
		if minKey == nil || CompareKeys(t.MinKey(), minKey) < 0 {
			minKey = t.MinKey()
		}
		if maxKey == nil || CompareKeys(t.MaxKey(), maxKey) > 0 {
			maxKey = t.MaxKey()
		}
	}

	// Find overlapping L1 tables
	var l1Tables []*SSTable
	if len(levels) > 1 {
		for _, t := range levels[1] {
			if CompareKeys(t.MaxKey(), minKey) >= 0 && CompareKeys(t.MinKey(), maxKey) <= 0 {
				l1Tables = append(l1Tables, t)
			}
		}
	}

	// Reverse L0 tables so newer tables (appended last) come first
	// The merge iterator uses index order to determine which entry is newer
	reversedL0 := make([]*SSTable, len(l0Tables))
	for i, t := range l0Tables {
		reversedL0[len(l0Tables)-1-i] = t
	}

	// Merge all tables (reversed L0 first, then L1)
	allTables := append(reversedL0, l1Tables...)
	newTables, err := w.mergeTables(allTables, 1)
	if err != nil {
		return
	}

	// Update store with new tables
	w.store.replaceTablesAfterCompaction(0, l0Tables, l1Tables, newTables)

	// Update reader
	w.reader.SetLevels(w.store.getLevels())

	// Remove old files and close old tables
	for _, t := range l0Tables {
		w.store.cache.RemoveByFileID(t.ID)
		t.Close()
		os.Remove(t.Path)
	}
	for _, t := range l1Tables {
		w.store.cache.RemoveByFileID(t.ID)
		t.Close()
		os.Remove(t.Path)
	}
}

func (w *Writer) compactLevelToNext(level int) {
	w.compactMu.Lock()
	defer w.compactMu.Unlock()

	levels := w.reader.GetLevels()
	if level >= len(levels) || len(levels[level]) == 0 {
		return
	}

	// Pick the oldest table from the level
	table := levels[level][0]

	// Find overlapping tables in next level
	var nextLevelTables []*SSTable
	if level+1 < len(levels) {
		for _, t := range levels[level+1] {
			if CompareKeys(t.MaxKey(), table.MinKey()) >= 0 &&
				CompareKeys(t.MinKey(), table.MaxKey()) <= 0 {
				nextLevelTables = append(nextLevelTables, t)
			}
		}
	}

	// Merge
	allTables := append([]*SSTable{table}, nextLevelTables...)
	newTables, err := w.mergeTables(allTables, level+1)
	if err != nil {
		return
	}

	// Update store
	w.store.replaceTablesAfterCompaction(level, []*SSTable{table}, nextLevelTables, newTables)

	// Update reader
	w.reader.SetLevels(w.store.getLevels())

	// Remove old files
	w.store.cache.RemoveByFileID(table.ID)
	table.Close()
	os.Remove(table.Path)
	for _, t := range nextLevelTables {
		w.store.cache.RemoveByFileID(t.ID)
		t.Close()
		os.Remove(t.Path)
	}
}

// mergeTables merges multiple SSTables into new ones at target level.
func (w *Writer) mergeTables(tables []*SSTable, targetLevel int) ([]*SSTable, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	// Create merge iterator
	mergeIter := newMergeIterator(tables, w.store.cache, w.store.opts.VerifyChecksums)
	defer mergeIter.Close()

	// Count total keys for bloom filter sizing
	var totalKeys uint
	for _, t := range tables {
		totalKeys += uint(t.Footer.NumKeys)
	}

	// Create output SSTable
	var newTables []*SSTable
	var writer *SSTableWriter
	var currentKeys uint

	maxTableSize := w.maxTableSize()
	isLastLevel := targetLevel >= w.store.opts.MaxLevels-1

	for mergeIter.Next() {
		entry := mergeIter.Entry()

		// Drop tombstones at last level
		if isLastLevel && entry.Value.IsTombstone() {
			continue
		}

		// Start new SSTable if needed
		if writer == nil {
			id := w.store.nextSSTableID()
			path := filepath.Join(w.store.opts.Dir, fmt.Sprintf("%06d.sst", id))
			var err error
			writer, err = NewSSTableWriter(id, path, totalKeys, w.store.opts)
			if err != nil {
				return nil, err
			}
			currentKeys = 0
		}

		if err := writer.Add(entry); err != nil {
			writer.Abort()
			return nil, err
		}
		currentKeys++

		// Check if we should finish this SSTable
		// Rough estimate: check every 1000 keys
		if currentKeys%1000 == 0 {
			stat, _ := writer.file.Stat()
			if stat != nil && stat.Size() >= maxTableSize {
				if err := writer.Finish(targetLevel); err != nil {
					writer.Abort()
					return nil, err
				}
				writer.Close()

				sst, err := OpenSSTable(writer.ID(), writer.Path())
				if err != nil {
					return nil, err
				}
				newTables = append(newTables, sst)
				writer = nil
			}
		}
	}

	// Finish last SSTable
	if writer != nil && currentKeys > 0 {
		if err := writer.Finish(targetLevel); err != nil {
			writer.Abort()
			return nil, err
		}
		writer.Close()

		sst, err := OpenSSTable(writer.ID(), writer.Path())
		if err != nil {
			return nil, err
		}
		newTables = append(newTables, sst)
	} else if writer != nil {
		writer.Abort()
	}

	return newTables, nil
}

func (w *Writer) levelSize(tables []*SSTable) int64 {
	var size int64
	for _, t := range tables {
		size += t.Size()
	}
	return size
}

func (w *Writer) maxLevelSize(level int) int64 {
	// L1: 10MB, each subsequent level 10x larger
	base := int64(10 * 1024 * 1024) // 10MB
	for i := 1; i < level; i++ {
		base *= int64(w.store.opts.LevelSizeMultiplier)
	}
	return base
}

func (w *Writer) maxTableSize() int64 {
	return 64 * 1024 * 1024 // 64MB per SSTable
}

// Memtable accessor
func (w *Writer) Memtable() *Memtable {
	return w.memtable
}

// SetSequence sets the sequence number (for recovery).
func (w *Writer) SetSequence(seq uint64) {
	atomic.StoreUint64(&w.sequence, seq)
}

// ForceCompact forces compaction of all L0 tables to L1.
func (w *Writer) ForceCompact() error {
	for {
		levels := w.reader.GetLevels()
		if len(levels) == 0 || len(levels[0]) == 0 {
			return nil // No more L0 tables
		}
		w.compactL0ToL1()
	}
}

// mergeIterator performs k-way merge over multiple SSTables.
type mergeIterator struct {
	tables    []*SSTable
	iterators []*sstableIterator
	heap      *entryHeap
	cache     *LRUCache
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

func newMergeIterator(tables []*SSTable, cache *LRUCache, verify bool) *mergeIterator {
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

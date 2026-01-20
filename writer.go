package tinykvs

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// writer handles all write operations including flushes and compaction.
type writer struct {
	store    *Store
	memtable *memtable
	wal      *wal
	reader   *reader
	sequence uint64 // Atomic counter for sequence numbers

	// Flush management
	flushMu    sync.Mutex
	flushCond  *sync.Cond // For backpressure when too many immutables
	immutables []*memtable
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

// maxImmutableMemtables is the max number of memtables waiting to be flushed.
// When this limit is reached, writes will block until a flush completes.
const maxImmutableMemtables = 2

// newWriter creates a new writer.
func newWriter(store *Store, memtable *memtable, wal *wal, reader *reader) *writer {
	ctx, cancel := context.WithCancel(context.Background())

	w := &writer{
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
func (w *writer) Start() {
	w.wg.Add(2)
	go w.flushLoop()
	go w.compactionLoop()
}

// Stop stops background goroutines.
func (w *writer) Stop() {
	w.cancel()
	w.wg.Wait()
}

// Put writes a key-value pair.
func (w *writer) Put(key []byte, value Value) error {
	seq := atomic.AddUint64(&w.sequence, 1)

	// Write to wal first
	entry := walEntry{
		Operation: opPut,
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
func (w *writer) Delete(key []byte) error {
	return w.Put(key, TombstoneValue())
}

// WriteBatch atomically writes all operations in the batch.
func (w *writer) WriteBatch(ops []batchOp) error {
	if len(ops) == 0 {
		return nil
	}

	// Allocate sequence numbers for all ops
	startSeq := atomic.AddUint64(&w.sequence, uint64(len(ops))) - uint64(len(ops)) + 1

	// Write all entries to wal
	for i, op := range ops {
		seq := startSeq + uint64(i)
		var entry walEntry
		if op.delete {
			entry = walEntry{
				Operation: opDelete,
				Key:       op.key,
				Value:     TombstoneValue(),
				Sequence:  seq,
			}
		} else {
			entry = walEntry{
				Operation: opPut,
				Key:       op.key,
				Value:     op.value,
				Sequence:  seq,
			}
		}
		if err := w.wal.Append(entry); err != nil {
			return err
		}
	}

	// Apply all entries to memtable
	for i, op := range ops {
		seq := startSeq + uint64(i)
		if op.delete {
			w.memtable.Put(op.key, TombstoneValue(), seq)
		} else {
			w.memtable.Put(op.key, op.value, seq)
		}
	}

	// Check if flush is needed
	if w.memtable.Size() >= w.store.opts.MemtableSize {
		w.triggerFlush()
	}

	return nil
}

// triggerFlush initiates an async flush.
// Blocks if too many immutable memtables are pending (backpressure).
func (w *writer) triggerFlush() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	// Backpressure: wait if too many immutables pending
	for len(w.immutables) >= maxImmutableMemtables {
		w.flushCond.Wait()
	}

	// Move current memtable to immutables
	w.immutables = append(w.immutables, w.memtable)
	w.reader.AddImmutable(w.memtable)

	// Create new memtable
	w.memtable = newMemtable()
	w.reader.Setmemtable(w.memtable)

	// Signal flush goroutine
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
}

// Flush forces a synchronous flush of all data to disk.
func (w *writer) Flush() error {
	w.flushMu.Lock()
	if w.memtable.Count() > 0 {
		w.immutables = append(w.immutables, w.memtable)
		w.reader.AddImmutable(w.memtable)
		w.memtable = newMemtable()
		w.reader.Setmemtable(w.memtable)
	}
	imm := make([]*memtable, len(w.immutables))
	copy(imm, w.immutables)
	w.flushMu.Unlock()

	// Flush all immutables
	for _, mt := range imm {
		if err := w.flushmemtable(mt); err != nil {
			return err
		}
	}

	// Sync wal
	return w.wal.Sync()
}

// flushmemtable writes a memtable to an SSTable.
func (w *writer) flushmemtable(mt *memtable) error {
	if mt.Count() == 0 {
		return nil
	}

	sst, err := w.writeMemtableToSSTable(mt)
	if err != nil {
		return err
	}

	w.store.addSSTable(0, sst)
	w.reader.AddSSTable(0, sst)

	if err := w.addToManifest(sst); err != nil {
		return err
	}

	w.removeFromImmutables(mt)
	w.truncateWALAfterFlush()
	w.maybeScheduleCompaction()

	return nil
}

// writeMemtableToSSTable writes a memtable to a new SSTable file.
func (w *writer) writeMemtableToSSTable(mt *memtable) (*SSTable, error) {
	id := w.store.nextSSTableID()
	path := filepath.Join(w.store.opts.Dir, fmt.Sprintf("%06d.sst", id))

	writer, err := newSSTableWriter(id, path, uint(mt.Count()), w.store.opts, true)
	if err != nil {
		return nil, err
	}

	iter := mt.Iterator()
	for iter.Next() {
		if err := writer.Add(iter.Entry()); err != nil {
			iter.Close()
			writer.Abort()
			return nil, err
		}
	}
	iter.Close()

	if err := writer.Finish(0); err != nil {
		writer.Abort()
		return nil, err
	}
	writer.Close()

	return OpenSSTable(id, path)
}

// addToManifest adds the SSTable to the manifest if enabled.
func (w *writer) addToManifest(sst *SSTable) error {
	if w.store.manifest == nil {
		return nil
	}
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
	return nil
}

// removeFromImmutables removes a memtable from the immutables list.
func (w *writer) removeFromImmutables(mt *memtable) {
	w.flushMu.Lock()
	for i, imm := range w.immutables {
		if imm == mt {
			w.immutables = append(w.immutables[:i], w.immutables[i+1:]...)
			break
		}
	}
	w.flushCond.Signal()
	w.flushMu.Unlock()
	w.reader.RemoveImmutable(mt)
}

// truncateWALAfterFlush truncates the WAL based on remaining immutables.
func (w *writer) truncateWALAfterFlush() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if len(w.immutables) == 0 {
		w.wal.Truncate()
		return
	}

	minSeq := w.findMinImmutableSequence()
	w.wal.TruncateBefore(minSeq)
}

// findMinImmutableSequence finds the minimum sequence number among immutables.
func (w *writer) findMinImmutableSequence() uint64 {
	minSeq := w.immutables[0].MinSequence()
	for _, imm := range w.immutables[1:] {
		if seq := imm.MinSequence(); seq < minSeq {
			minSeq = seq
		}
	}
	return minSeq
}

// Background flush goroutine
func (w *writer) flushLoop() {
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

func (w *writer) flushImmutables() {
	w.flushMu.Lock()
	imm := make([]*memtable, len(w.immutables))
	copy(imm, w.immutables)
	w.flushMu.Unlock()

	for _, mt := range imm {
		if err := w.flushmemtable(mt); err != nil {
			// Log error, will retry on next tick
			continue
		}
	}
}

// memtable accessor
func (w *writer) getMemtable() *memtable {
	return w.memtable
}

// SetSequence sets the sequence number (for recovery).
func (w *writer) SetSequence(seq uint64) {
	atomic.StoreUint64(&w.sequence, seq)
}

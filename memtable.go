package tinykvs

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

const (
	maxHeight   = 12
	probability = 0.25
)

// skiplistNode represents a node in the skiplist.
type skiplistNode struct {
	entry   Entry
	forward []*skiplistNode
}

// Memtable is an in-memory sorted buffer using a skiplist.
// It is safe for concurrent reads but requires external synchronization for writes.
type Memtable struct {
	head   *skiplistNode
	height int
	size   int64  // Approximate size in bytes (atomic)
	count  int64  // Number of entries (atomic)
	minSeq uint64 // Minimum sequence number in this memtable (atomic)

	mu  sync.RWMutex
	rng *rand.Rand
}

// NewMemtable creates a new empty memtable.
func NewMemtable() *Memtable {
	return &Memtable{
		head:   &skiplistNode{forward: make([]*skiplistNode, maxHeight)},
		height: 1,
		rng:    rand.New(rand.NewSource(rand.Int63())),
	}
}

// Put inserts or updates a key-value pair.
func (m *Memtable) Put(key []byte, value Value, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Use stack-allocated array to avoid heap allocation
	var update [maxHeight]*skiplistNode
	x := m.head

	// Find insert position from top level down
	for i := m.height - 1; i >= 0; i-- {
		for x.forward[i] != nil && CompareKeys(x.forward[i].entry.Key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]

	entry := Entry{
		Key:      key,
		Value:    value,
		Sequence: seq,
	}

	entrySize := int64(len(key) + value.EncodedSize() + 8) // key + value + sequence

	// Update existing key
	if x != nil && CompareKeys(x.entry.Key, key) == 0 {
		oldSize := int64(len(x.entry.Key) + x.entry.Value.EncodedSize() + 8)
		x.entry = entry
		atomic.AddInt64(&m.size, entrySize-oldSize)
		return
	}

	// Insert new node
	level := m.randomHeight()
	if level > m.height {
		for i := m.height; i < level; i++ {
			update[i] = m.head
		}
		m.height = level
	}

	newNode := &skiplistNode{
		entry:   entry,
		forward: make([]*skiplistNode, level),
	}

	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	atomic.AddInt64(&m.size, entrySize)
	atomic.AddInt64(&m.count, 1)

	// Track minimum sequence (set only on first insert)
	atomic.CompareAndSwapUint64(&m.minSeq, 0, seq)
}

// Get retrieves an entry by key.
// Returns the entry and true if found, or an empty entry and false if not found.
func (m *Memtable) Get(key []byte) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	x := m.head
	for i := m.height - 1; i >= 0; i-- {
		for x.forward[i] != nil && CompareKeys(x.forward[i].entry.Key, key) < 0 {
			x = x.forward[i]
		}
	}

	x = x.forward[0]
	if x != nil && CompareKeys(x.entry.Key, key) == 0 {
		return x.entry, true
	}
	return Entry{}, false
}

// Iterator returns an iterator over all entries in sorted order.
// The caller must call Close() when done.
func (m *Memtable) Iterator() *MemtableIterator {
	m.mu.RLock()
	return &MemtableIterator{
		mt:      m,
		current: m.head,
	}
}

// Size returns approximate memory usage in bytes.
func (m *Memtable) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

// Count returns the number of entries.
func (m *Memtable) Count() int64 {
	return atomic.LoadInt64(&m.count)
}

// MinSequence returns the minimum sequence number in this memtable.
func (m *Memtable) MinSequence() uint64 {
	return atomic.LoadUint64(&m.minSeq)
}

// randomHeight generates a random height for a new node.
func (m *Memtable) randomHeight() int {
	h := 1
	for h < maxHeight && m.rng.Float64() < probability {
		h++
	}
	return h
}

// MemtableIterator iterates over memtable entries in sorted order.
type MemtableIterator struct {
	mt      *Memtable
	current *skiplistNode
}

// Next advances to the next entry.
// Returns true if there is a next entry, false if iteration is complete.
func (it *MemtableIterator) Next() bool {
	if it.current == nil {
		return false
	}
	it.current = it.current.forward[0]
	return it.current != nil
}

// Entry returns the current entry.
// Only valid after a successful call to Next().
func (it *MemtableIterator) Entry() Entry {
	if it.current == nil {
		return Entry{}
	}
	return it.current.entry
}

// Key returns the current key.
func (it *MemtableIterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.entry.Key
}

// Value returns the current value.
func (it *MemtableIterator) Value() Value {
	if it.current == nil {
		return Value{}
	}
	return it.current.entry.Value
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *MemtableIterator) Valid() bool {
	return it.current != nil && it.current != it.mt.head
}

// Seek positions the iterator at the first entry with key >= target.
func (it *MemtableIterator) Seek(target []byte) bool {
	it.mt.mu.RLock()
	defer it.mt.mu.RUnlock()

	x := it.mt.head
	for i := it.mt.height - 1; i >= 0; i-- {
		for x.forward[i] != nil && CompareKeys(x.forward[i].entry.Key, target) < 0 {
			x = x.forward[i]
		}
	}

	it.current = x.forward[0]
	return it.current != nil
}

// Close releases resources held by the iterator.
func (it *MemtableIterator) Close() {
	it.mt.mu.RUnlock()
}

package tinykvs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Store is the main key-value store.
type Store struct {
	opts Options
	dir  string

	// Data structures
	memtable *Memtable
	wal      *WAL
	levels   [][]*SSTable // levels[0] = L0, levels[1] = L1, etc.
	cache    *LRUCache

	// Components
	reader *Reader
	writer *Writer

	// Synchronization
	mu      sync.RWMutex
	writeMu sync.Mutex // Single writer

	// State
	nextID uint32 // Atomic SSTable ID counter
	closed bool
}

// Open opens or creates a store at the given path.
func Open(path string, opts Options) (*Store, error) {
	// Create directory if needed
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	opts.Dir = path

	// Initialize store
	s := &Store{
		opts:     opts,
		dir:      path,
		memtable: NewMemtable(),
		levels:   make([][]*SSTable, opts.MaxLevels),
		cache:    NewLRUCache(opts.BlockCacheSize),
	}

	// Open WAL
	walPath := filepath.Join(path, "wal.log")
	wal, err := OpenWAL(walPath, opts.WALSyncMode)
	if err != nil {
		return nil, err
	}
	s.wal = wal

	// Load existing SSTables
	if err := s.loadSSTables(); err != nil {
		wal.Close()
		return nil, err
	}

	// Initialize reader
	s.reader = NewReader(s.memtable, s.levels, s.cache, opts)

	// Initialize writer
	s.writer = NewWriter(s, s.memtable, wal, s.reader)

	// Recover from WAL
	if err := s.recover(); err != nil {
		wal.Close()
		return nil, err
	}

	// Start background tasks
	s.writer.Start()

	return s, nil
}

// Get retrieves a value by key.
func (s *Store) Get(key []byte) (Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return Value{}, ErrStoreClosed
	}

	return s.reader.Get(key)
}

// Put stores a key-value pair.
func (s *Store) Put(key []byte, value Value) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	return s.writer.Put(key, value)
}

// PutInt64 stores an int64 value.
func (s *Store) PutInt64(key []byte, value int64) error {
	return s.Put(key, Int64Value(value))
}

// PutFloat64 stores a float64 value.
func (s *Store) PutFloat64(key []byte, value float64) error {
	return s.Put(key, Float64Value(value))
}

// PutBool stores a bool value.
func (s *Store) PutBool(key []byte, value bool) error {
	return s.Put(key, BoolValue(value))
}

// PutString stores a string value.
func (s *Store) PutString(key []byte, value string) error {
	return s.Put(key, StringValue(value))
}

// PutBytes stores a byte slice value.
func (s *Store) PutBytes(key []byte, value []byte) error {
	return s.Put(key, BytesValue(value))
}

// Delete removes a key.
func (s *Store) Delete(key []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	return s.writer.Delete(key)
}

// Flush forces all data to disk.
func (s *Store) Flush() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	return s.writer.Flush()
}

// Compact forces compaction of all L0 tables to L1.
// This makes reads faster by converting overlapping L0 tables to disjoint L1 tables.
func (s *Store) Compact() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	return s.writer.ForceCompact()
}

// Close closes the store.
func (s *Store) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return nil
	}

	// Stop background goroutines
	s.writer.Stop()

	// Flush remaining data
	s.writer.Flush()

	// Close WAL
	s.wal.Close()

	// Close all SSTables
	s.mu.Lock()
	for _, level := range s.levels {
		for _, sst := range level {
			sst.Close()
		}
	}
	s.mu.Unlock()

	s.closed = true
	return nil
}

// Stats returns store statistics.
func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := StoreStats{
		MemtableSize:  s.writer.Memtable().Size(),
		MemtableCount: s.writer.Memtable().Count(),
		CacheStats:    s.cache.Stats(),
	}

	for level, tables := range s.levels {
		var levelSize int64
		var levelKeys uint64
		for _, t := range tables {
			levelSize += t.Size()
			levelKeys += t.Footer.NumKeys
		}
		stats.Levels = append(stats.Levels, LevelStats{
			Level:     level,
			NumTables: len(tables),
			Size:      levelSize,
			NumKeys:   levelKeys,
		})
	}

	return stats
}

// StoreStats contains store statistics.
type StoreStats struct {
	MemtableSize  int64
	MemtableCount int64
	CacheStats    CacheStats
	Levels        []LevelStats
}

// LevelStats contains statistics for a single level.
type LevelStats struct {
	Level     int
	NumTables int
	Size      int64
	NumKeys   uint64
}

// Internal methods

func (s *Store) recover() error {
	entries, err := s.wal.Recover()
	if err != nil {
		return err
	}

	var maxSeq uint64
	for _, entry := range entries {
		switch entry.Operation {
		case OpPut:
			s.memtable.Put(entry.Key, entry.Value, entry.Sequence)
		case OpDelete:
			s.memtable.Put(entry.Key, TombstoneValue(), entry.Sequence)
		}
		if entry.Sequence > maxSeq {
			maxSeq = entry.Sequence
		}
	}

	// Set writer sequence to max recovered
	s.writer.SetSequence(maxSeq)

	return nil
}

func (s *Store) loadSSTables() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	var maxID uint32
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}

		// Parse ID from filename (e.g., "000001.sst")
		name := strings.TrimSuffix(entry.Name(), ".sst")
		id64, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			continue
		}
		id := uint32(id64)

		if id > maxID {
			maxID = id
		}

		path := filepath.Join(s.dir, entry.Name())
		sst, err := OpenSSTable(id, path)
		if err != nil {
			// Log error but continue loading other files
			continue
		}

		// Add to appropriate level
		level := sst.Level
		for len(s.levels) <= level {
			s.levels = append(s.levels, nil)
		}
		s.levels[level] = append(s.levels[level], sst)
	}

	// Sort L1+ tables by min key
	for level := 1; level < len(s.levels); level++ {
		sortTablesByMinKey(s.levels[level])
	}

	// Set next ID
	atomic.StoreUint32(&s.nextID, maxID)

	return nil
}

func (s *Store) nextSSTableID() uint32 {
	return atomic.AddUint32(&s.nextID, 1)
}

func (s *Store) addSSTable(level int, sst *SSTable) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.levels) <= level {
		s.levels = append(s.levels, nil)
	}
	s.levels[level] = append(s.levels[level], sst)
}

func (s *Store) getLevels() [][]*SSTable {
	s.mu.RLock()
	defer s.mu.RUnlock()

	levels := make([][]*SSTable, len(s.levels))
	for i, level := range s.levels {
		levels[i] = make([]*SSTable, len(level))
		copy(levels[i], level)
	}
	return levels
}

func (s *Store) replaceTablesAfterCompaction(
	compactedLevel int,
	oldL0Tables []*SSTable,
	oldL1Tables []*SSTable,
	newTables []*SSTable,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove old L0 tables
	if compactedLevel == 0 {
		newL0 := make([]*SSTable, 0, len(s.levels[0]))
		for _, t := range s.levels[0] {
			found := false
			for _, old := range oldL0Tables {
				if t.ID == old.ID {
					found = true
					break
				}
			}
			if !found {
				newL0 = append(newL0, t)
			}
		}
		s.levels[0] = newL0
	} else {
		// Remove the compacted table from its level
		newLevel := make([]*SSTable, 0, len(s.levels[compactedLevel]))
		for _, t := range s.levels[compactedLevel] {
			found := false
			for _, old := range oldL0Tables { // oldL0Tables contains the single table for non-L0 compaction
				if t.ID == old.ID {
					found = true
					break
				}
			}
			if !found {
				newLevel = append(newLevel, t)
			}
		}
		s.levels[compactedLevel] = newLevel
	}

	// Target level is always compactedLevel+1 for L0, or compactedLevel+1 for others
	targetLevel := 1
	if compactedLevel > 0 {
		targetLevel = compactedLevel + 1
	}

	// Ensure target level exists
	for len(s.levels) <= targetLevel {
		s.levels = append(s.levels, nil)
	}

	// Remove old target level tables
	newTargetLevel := make([]*SSTable, 0, len(s.levels[targetLevel]))
	for _, t := range s.levels[targetLevel] {
		found := false
		for _, old := range oldL1Tables {
			if t.ID == old.ID {
				found = true
				break
			}
		}
		if !found {
			newTargetLevel = append(newTargetLevel, t)
		}
	}

	// Add new tables
	newTargetLevel = append(newTargetLevel, newTables...)

	// Sort by min key
	sort.Slice(newTargetLevel, func(i, j int) bool {
		return CompareKeys(newTargetLevel[i].MinKey(), newTargetLevel[j].MinKey()) < 0
	})

	s.levels[targetLevel] = newTargetLevel
}

// Errors
var (
	ErrStoreClosed = errors.New("store is closed")
)

// Convenience functions for common operations

// GetString retrieves a string value by key.
func (s *Store) GetString(key []byte) (string, error) {
	val, err := s.Get(key)
	if err != nil {
		return "", err
	}
	return val.String(), nil
}

// GetInt64 retrieves an int64 value by key.
func (s *Store) GetInt64(key []byte) (int64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if val.Type != ValueTypeInt64 {
		return 0, fmt.Errorf("expected int64, got %d", val.Type)
	}
	return val.Int64, nil
}

// GetFloat64 retrieves a float64 value by key.
func (s *Store) GetFloat64(key []byte) (float64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if val.Type != ValueTypeFloat64 {
		return 0, fmt.Errorf("expected float64, got %d", val.Type)
	}
	return val.Float64, nil
}

// GetBool retrieves a bool value by key.
func (s *Store) GetBool(key []byte) (bool, error) {
	val, err := s.Get(key)
	if err != nil {
		return false, err
	}
	if val.Type != ValueTypeBool {
		return false, fmt.Errorf("expected bool, got %d", val.Type)
	}
	return val.Bool, nil
}

// GetBytes retrieves a byte slice value by key.
func (s *Store) GetBytes(key []byte) ([]byte, error) {
	val, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	return val.GetBytes(), nil
}

// ScanPrefix iterates over all keys with the given prefix in sorted order.
// The callback receives the key and value bytes directly (zero-copy).
// Return false from the callback to stop iteration.
// Keys are deduplicated (newest version wins) and tombstones are skipped.
func (s *Store) ScanPrefix(prefix []byte, fn func(key []byte, value Value) bool) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrStoreClosed
	}
	reader := s.reader
	s.mu.RUnlock()

	return reader.ScanPrefix(prefix, fn)
}

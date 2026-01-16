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
	"syscall"
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
	manifest *Manifest // Tracks all SSTables

	// Components
	reader *Reader
	writer *Writer

	// Synchronization
	mu       sync.RWMutex
	writeMu  sync.Mutex // Single writer
	lockFile *os.File   // Exclusive lock file

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

	// Acquire exclusive lock
	lockPath := filepath.Join(path, "LOCK")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, ErrStoreLocked
	}

	opts.Dir = path

	// Initialize store
	s := &Store{
		lockFile: lockFile,
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
		s.releaseLock()
		return nil, err
	}
	s.wal = wal

	// Try to open manifest for fast loading
	manifestPath := filepath.Join(path, "MANIFEST")
	manifest, err := OpenManifest(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		wal.Close()
		s.releaseLock()
		return nil, err
	}

	if manifest != nil && len(manifest.Tables()) > 0 {
		// Load SSTables from manifest (fast path - lazy loading)
		s.manifest = manifest
		if err := s.loadSSTablesFromManifest(); err != nil {
			manifest.Close()
			wal.Close()
			s.releaseLock()
			return nil, err
		}
	} else {
		// No manifest or empty - scan directory (migration/fallback path)
		if manifest != nil {
			manifest.Close()
		}
		if err := s.loadSSTables(); err != nil {
			wal.Close()
			s.releaseLock()
			return nil, err
		}
		// Create manifest from loaded SSTables
		manifest, err = s.createManifestFromSSTables(manifestPath)
		if err != nil {
			wal.Close()
			s.releaseLock()
			return nil, err
		}
		s.manifest = manifest
	}

	// Initialize reader
	s.reader = NewReader(s.memtable, s.levels, s.cache, opts)

	// Initialize writer
	s.writer = NewWriter(s, s.memtable, wal, s.reader)

	// Recover from WAL
	if err := s.recover(); err != nil {
		s.manifest.Close()
		wal.Close()
		s.releaseLock()
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

	// Close manifest
	if s.manifest != nil {
		s.manifest.Close()
	}

	// Close all SSTables
	s.mu.Lock()
	for _, level := range s.levels {
		for _, sst := range level {
			sst.Close()
		}
	}
	s.mu.Unlock()

	// Release lock file
	s.releaseLock()

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

	var totalIndexMemory int64
	for level, tables := range s.levels {
		var levelSize int64
		var levelKeys uint64
		for _, t := range tables {
			levelSize += t.Size()
			levelKeys += t.Footer.NumKeys
			totalIndexMemory += t.MemorySize()
		}
		stats.Levels = append(stats.Levels, LevelStats{
			Level:     level,
			NumTables: len(tables),
			Size:      levelSize,
			NumKeys:   levelKeys,
		})
	}
	stats.IndexMemory = totalIndexMemory

	return stats
}

// StoreStats contains store statistics.
type StoreStats struct {
	MemtableSize  int64
	MemtableCount int64
	IndexMemory   int64 // Total in-memory size of indexes and bloom filters
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

	// Collect SSTable file info
	type sstFile struct {
		id   uint32
		path string
	}
	var files []sstFile
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

		files = append(files, sstFile{
			id:   id,
			path: filepath.Join(s.dir, entry.Name()),
		})
	}

	// Load SSTables in parallel
	numWorkers := 8
	if len(files) < numWorkers {
		numWorkers = len(files)
	}
	if numWorkers == 0 {
		atomic.StoreUint32(&s.nextID, maxID)
		return nil
	}

	type result struct {
		sst *SSTable
		err error
	}

	jobs := make(chan sstFile, len(files))
	results := make(chan result, len(files))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range jobs {
				sst, err := OpenSSTable(f.id, f.path)
				results <- result{sst: sst, err: err}
			}
		}()
	}

	// Send jobs
	for _, f := range files {
		jobs <- f
	}
	close(jobs)

	// Wait for workers and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for r := range results {
		if r.err != nil {
			// Log error but continue loading other files
			continue
		}

		// Add to appropriate level
		level := r.sst.Level
		for len(s.levels) <= level {
			s.levels = append(s.levels, nil)
		}
		s.levels[level] = append(s.levels[level], r.sst)
	}

	// Sort L1+ tables by min key
	for level := 1; level < len(s.levels); level++ {
		sortTablesByMinKey(s.levels[level])
	}

	// Set next ID
	atomic.StoreUint32(&s.nextID, maxID)

	return nil
}

// loadSSTablesFromManifest loads SSTables using metadata from the manifest.
// This is much faster than loadSSTables() because it uses lazy loading.
func (s *Store) loadSSTablesFromManifest() error {
	tables := s.manifest.Tables()

	// Open SSTables in parallel with lazy loading
	numWorkers := 8
	if len(tables) < numWorkers {
		numWorkers = len(tables)
	}
	if numWorkers == 0 {
		atomic.StoreUint32(&s.nextID, s.manifest.MaxID())
		return nil
	}

	type result struct {
		sst *SSTable
		err error
	}

	jobs := make(chan *TableMeta, len(tables))
	results := make(chan result, len(tables))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for meta := range jobs {
				sst, err := OpenSSTableFromManifest(meta, s.dir)
				results <- result{sst: sst, err: err}
			}
		}()
	}

	// Send jobs
	for _, meta := range tables {
		jobs <- meta
	}
	close(jobs)

	// Wait for workers and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for r := range results {
		if r.err != nil {
			// Log error but continue loading other files
			continue
		}

		// Add to appropriate level
		level := r.sst.Level
		for len(s.levels) <= level {
			s.levels = append(s.levels, nil)
		}
		s.levels[level] = append(s.levels[level], r.sst)
	}

	// Sort L1+ tables by min key
	for level := 1; level < len(s.levels); level++ {
		sortTablesByMinKey(s.levels[level])
	}

	// Set next ID
	atomic.StoreUint32(&s.nextID, s.manifest.MaxID())

	return nil
}

// createManifestFromSSTables creates a manifest from already-loaded SSTables.
// This is used when migrating from a store without a manifest.
func (s *Store) createManifestFromSSTables(manifestPath string) (*Manifest, error) {
	manifest, err := OpenManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	// Add all loaded SSTables to manifest
	for _, level := range s.levels {
		for _, sst := range level {
			meta := &TableMeta{
				ID:          sst.ID,
				Level:       sst.Level,
				MinKey:      sst.MinKey(),
				MaxKey:      sst.MaxKey(),
				NumKeys:     sst.Footer.NumKeys,
				FileSize:    sst.fileSize,
				IndexOffset: sst.Footer.IndexOffset,
				IndexSize:   sst.Footer.IndexSize,
				BloomOffset: sst.Footer.BloomOffset,
				BloomSize:   sst.Footer.BloomSize,
			}
			if err := manifest.AddTable(meta); err != nil {
				manifest.Close()
				return nil, err
			}
		}
	}

	return manifest, nil
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

	// Update manifest
	if s.manifest != nil {
		// Add new tables
		for _, sst := range newTables {
			meta := &TableMeta{
				ID:          sst.ID,
				Level:       targetLevel,
				MinKey:      sst.MinKey(),
				MaxKey:      sst.MaxKey(),
				NumKeys:     sst.Footer.NumKeys,
				FileSize:    int64(sst.Footer.FileSize),
				IndexOffset: sst.Footer.IndexOffset,
				IndexSize:   sst.Footer.IndexSize,
				BloomOffset: sst.Footer.BloomOffset,
				BloomSize:   sst.Footer.BloomSize,
			}
			s.manifest.AddTable(meta)
		}

		// Delete old tables
		var deleteIDs []uint32
		for _, t := range oldL0Tables {
			deleteIDs = append(deleteIDs, t.ID)
		}
		for _, t := range oldL1Tables {
			deleteIDs = append(deleteIDs, t.ID)
		}
		if len(deleteIDs) > 0 {
			s.manifest.DeleteTables(deleteIDs)
		}
	}
}

// releaseLock releases the exclusive lock file.
func (s *Store) releaseLock() {
	if s.lockFile != nil {
		syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		s.lockFile.Close()
		s.lockFile = nil
	}
}

// Errors
var (
	ErrStoreClosed = errors.New("store is closed")
	ErrStoreLocked = errors.New("store is locked by another process")
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

// KeyLocation describes where a key is stored.
type KeyLocation struct {
	Level   int
	TableID uint32
}

// FindKey returns the location of a key, or nil if in memtable or not found.
func (s *Store) FindKey(key []byte) *KeyLocation {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check each level
	for level, tables := range s.levels {
		for _, sst := range tables {
			// Quick range check
			if CompareKeys(key, sst.MinKey()) < 0 || CompareKeys(key, sst.MaxKey()) > 0 {
				continue
			}
			// Check bloom filter
			if sst.BloomFilter != nil && !sst.BloomFilter.MayContain(key) {
				continue
			}
			// Try to get the key
			_, found, err := sst.Get(key, s.cache, false)
			if err == nil && found {
				return &KeyLocation{Level: level, TableID: sst.ID}
			}
		}
	}
	return nil
}

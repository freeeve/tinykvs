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
	memtable *memtable
	wal      *wal
	levels   [][]*SSTable // levels[0] = L0, levels[1] = L1, etc.
	cache    *lruCache
	manifest *Manifest // Tracks all SSTables

	// Components
	reader *reader
	writer *writer

	// Synchronization
	mu       sync.RWMutex
	writeMu  sync.Mutex // Single writer
	lockFile *os.File   // Exclusive lock file

	// State
	nextID uint32 // Atomic SSTable ID counter
	closed atomic.Bool
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

	if err := acquireLock(lockFile); err != nil {
		lockFile.Close()
		return nil, ErrStoreLocked
	}

	opts.Dir = path

	// Initialize store
	s := &Store{
		lockFile: lockFile,
		opts:     opts,
		dir:      path,
		memtable: newMemtable(),
		levels:   make([][]*SSTable, opts.MaxLevels),
		cache:    newLRUCache(opts.BlockCacheSize),
	}

	// Open wal
	walPath := filepath.Join(path, "wal.log")
	wal, err := openWAL(walPath, opts.WALSyncMode)
	if err != nil {
		s.releaseLock()
		return nil, err
	}
	s.wal = wal

	// Load SSTables using manifest (fast) or directory scan (fallback)
	manifestPath := filepath.Join(path, "MANIFEST")
	if err := s.initializeSSTables(manifestPath); err != nil {
		wal.Close()
		s.releaseLock()
		return nil, err
	}

	// Initialize reader
	s.reader = newReader(s.memtable, s.levels, s.cache, opts)

	// Initialize writer
	s.writer = newWriter(s, s.memtable, wal, s.reader)

	// Recover from wal
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
	if s.closed.Load() {
		return Value{}, ErrStoreClosed
	}

	// Note: We don't take s.mu.RLock() here. The reader has its own internal
	// locking (r.mu) and s.reader is set once during Open() and never changes.
	// This allows reads to proceed without blocking on flush/compaction.
	return s.reader.Get(key)
}

// Put stores a key-value pair.
func (s *Store) Put(key []byte, value Value) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return s.writer.Put(key, value)
}

// Delete removes a key.
func (s *Store) Delete(key []byte) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return s.writer.Delete(key)
}

// Flush forces all data to disk.
func (s *Store) Flush() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return s.writer.Flush()
}

// Sync ensures all written data is durable by syncing the WAL.
// This is faster than Flush() because it doesn't create an SSTable.
// Data remains in the memtable and will be recovered from WAL on restart.
// Use this for frequent durability checkpoints; use Flush() less frequently
// to convert memtable data to SSTables.
//
// Sync does not block on pending writes - it syncs whatever is currently
// in the WAL buffer. This allows Sync to proceed even during backpressure.
func (s *Store) Sync() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	// Note: We don't take writeMu here. The WAL has its own internal lock
	// and Sync should not block on write backpressure. Each Put() writes
	// to the WAL before returning, so calling Sync() after Puts ensures
	// those writes are durable.
	return s.wal.Sync()
}

// Compact forces compaction of all L0 tables to L1.
// This makes reads faster by converting overlapping L0 tables to disjoint L1 tables.
func (s *Store) Compact() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return s.writer.ForceCompact()
}

// Close closes the store.
func (s *Store) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed.Load() {
		return nil
	}

	// Mark as closed early to reject new operations
	s.closed.Store(true)

	// Stop background goroutines
	s.writer.Stop()

	// Flush remaining data
	s.writer.Flush()

	// Close wal
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

	return nil
}

// ScanPrefix iterates over all keys with the given prefix in sorted order.
// The callback receives the key and value bytes directly (zero-copy).
// Return false from the callback to stop iteration.
// Keys are deduplicated (newest version wins) and tombstones are skipped.
func (s *Store) ScanPrefix(prefix []byte, fn func(key []byte, value Value) bool) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	return s.reader.ScanPrefix(prefix, fn)
}

// ScanPrefixWithStats is like ScanPrefix but also returns scan statistics.
// The progress callback (if non-nil) is called periodically during the scan.
func (s *Store) ScanPrefixWithStats(prefix []byte, fn func(key []byte, value Value) bool, progress ScanProgress) (ScanStats, error) {
	if s.closed.Load() {
		return ScanStats{}, ErrStoreClosed
	}

	return s.reader.ScanPrefixWithStats(prefix, fn, progress)
}

// ScanRange iterates over all keys in the range [start, end) in sorted order.
// The callback receives the key and value bytes directly (zero-copy).
// Return false from the callback to stop iteration.
func (s *Store) ScanRange(start, end []byte, fn func(key []byte, value Value) bool) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	return s.reader.ScanRange(start, end, fn)
}

// Stats returns store statistics.
func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := StoreStats{
		MemtableSize:  s.writer.getMemtable().Size(),
		MemtableCount: s.writer.getMemtable().Count(),
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

// PrefixTableInfo describes an SSTable's relationship to a prefix.
type PrefixTableInfo struct {
	Level      int
	TableID    uint32
	MinKey     []byte
	MaxKey     []byte
	NumKeys    uint64
	InRange    bool   // Prefix is within [MinKey, MaxKey]
	HasMatch   bool   // Table actually contains keys with this prefix
	FirstMatch []byte // First matching key (if HasMatch)
}

// ExplainPrefix returns information about which tables contain a given prefix.
func (s *Store) ExplainPrefix(prefix []byte) []PrefixTableInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []PrefixTableInfo

	for level, tables := range s.levels {
		for _, sst := range tables {
			info := PrefixTableInfo{
				Level:   level,
				TableID: sst.ID,
				MinKey:  sst.MinKey(),
				MaxKey:  sst.MaxKey(),
				NumKeys: sst.Footer.NumKeys,
			}

			info.InRange = hasKeyInRange(prefix, info.MinKey, info.MaxKey)

			if info.InRange {
				info.HasMatch, info.FirstMatch = findPrefixInTable(sst, prefix)
				results = append(results, info)
			}
		}
	}

	return results
}

// findPrefixInTable searches an SSTable for keys matching the given prefix.
// Returns whether a match was found and the first matching key.
func findPrefixInTable(sst *SSTable, prefix []byte) (bool, []byte) {
	sst.ensureIndex()
	blockIdx := sst.Index.Search(prefix)
	if blockIdx < 0 {
		if hasPrefix(sst.Index.MinKey, prefix) {
			blockIdx = 0
		} else {
			return false, nil
		}
	}

	for blockIdx >= 0 && blockIdx < len(sst.Index.Entries) {
		ie := sst.Index.Entries[blockIdx]

		if CompareKeys(ie.Key, prefix) > 0 && !hasPrefix(ie.Key, prefix) {
			break
		}

		hasMatch, firstMatch, continueSearch := findPrefixInBlock(sst, ie, prefix)
		if hasMatch {
			return true, firstMatch
		}
		if !continueSearch {
			break
		}
		blockIdx++
	}

	return false, nil
}

// findPrefixInBlock searches a single block for a prefix match.
// Returns: hasMatch, firstMatch key, whether to continue searching.
func findPrefixInBlock(sst *SSTable, ie IndexEntry, prefix []byte) (bool, []byte, bool) {
	blockData := make([]byte, ie.BlockSize)
	if _, err := sst.file.ReadAt(blockData, int64(ie.BlockOffset)); err != nil {
		return false, nil, true
	}

	block, err := DecodeBlock(blockData, false)
	if err != nil {
		return false, nil, true
	}
	defer block.Release()

	for _, entry := range block.Entries {
		if CompareKeys(entry.Key, prefix) >= 0 {
			if hasPrefix(entry.Key, prefix) {
				firstMatch := make([]byte, len(entry.Key))
				copy(firstMatch, entry.Key)
				return true, firstMatch, false
			}
			return false, nil, false
		}
	}

	return false, nil, true
}

// Errors
var (
	ErrStoreClosed     = errors.New("store is closed")
	ErrStoreLocked     = errors.New("store is locked by another process")
	ErrKeyExists       = errors.New("key already exists")
	ErrConditionFailed = errors.New("condition failed")
	ErrTypeMismatch    = errors.New("value type mismatch")
)

// Internal methods

func (s *Store) recover() error {
	entries, err := s.wal.Recover()
	if err != nil {
		return err
	}

	var maxSeq uint64
	for _, entry := range entries {
		switch entry.Operation {
		case opPut:
			s.memtable.Put(entry.Key, entry.Value, entry.Sequence)
		case opDelete:
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

// initializeSSTables loads SSTables using manifest (fast) or directory scan (fallback).
func (s *Store) initializeSSTables(manifestPath string) error {
	manifest, err := OpenManifest(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if manifest != nil && len(manifest.Tables()) > 0 {
		s.manifest = manifest
		return s.loadSSTablesFromManifest()
	}

	// No manifest or empty - scan directory (migration/fallback path)
	if manifest != nil {
		manifest.Close()
	}
	if err := s.loadSSTables(); err != nil {
		return err
	}

	// Create manifest from loaded SSTables
	manifest, err = s.createManifestFromSSTables(manifestPath)
	if err != nil {
		return err
	}
	s.manifest = manifest
	return nil
}

// sstFile holds SSTable file info for loading.
type sstFile struct {
	id   uint32
	path string
}

func (s *Store) loadSSTables() error {
	files, maxID, err := s.collectSSTableFiles()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		atomic.StoreUint32(&s.nextID, maxID)
		return nil
	}

	tables := s.loadSSTablesParallel(files)
	s.organizeTables(tables)

	atomic.StoreUint32(&s.nextID, maxID)
	return nil
}

// collectSSTableFiles scans the directory for .sst files.
func (s *Store) collectSSTableFiles() ([]sstFile, uint32, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, 0, err
	}

	var files []sstFile
	var maxID uint32

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}

		id, ok := parseSSTableID(entry.Name())
		if !ok {
			continue
		}

		if id > maxID {
			maxID = id
		}

		files = append(files, sstFile{
			id:   id,
			path: filepath.Join(s.dir, entry.Name()),
		})
	}

	return files, maxID, nil
}

// parseSSTableID extracts the ID from an SSTable filename.
func parseSSTableID(filename string) (uint32, bool) {
	name := strings.TrimSuffix(filename, ".sst")
	id64, err := strconv.ParseUint(name, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id64), true
}

// loadSSTablesParallel loads SSTables concurrently.
func (s *Store) loadSSTablesParallel(files []sstFile) []*SSTable {
	numWorkers := 8
	if len(files) < numWorkers {
		numWorkers = len(files)
	}

	jobs := make(chan sstFile, len(files))
	results := make(chan *SSTable, len(files))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range jobs {
				sst, err := OpenSSTable(f.id, f.path)
				if err == nil {
					results <- sst
				}
			}
		}()
	}

	// Send jobs
	for _, f := range files {
		jobs <- f
	}
	close(jobs)

	// Wait and collect
	go func() {
		wg.Wait()
		close(results)
	}()

	var tables []*SSTable
	for sst := range results {
		tables = append(tables, sst)
	}
	return tables
}

// organizeTables adds tables to levels and sorts them.
func (s *Store) organizeTables(tables []*SSTable) {
	for _, sst := range tables {
		level := sst.Level
		for len(s.levels) <= level {
			s.levels = append(s.levels, nil)
		}
		s.levels[level] = append(s.levels[level], sst)
	}

	// Sort L0 tables by ID (oldest first = lowest ID first)
	if len(s.levels) > 0 {
		sortTablesByID(s.levels[0])
	}

	// Sort L1+ tables by min key
	for level := 1; level < len(s.levels); level++ {
		sortTablesByMinKey(s.levels[level])
	}
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

	// Sort L0 tables by ID (oldest first = lowest ID first)
	// This is critical for correct compaction ordering
	if len(s.levels) > 0 {
		sortTablesByID(s.levels[0])
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

	// Remove old tables from compacted level
	s.levels[compactedLevel] = filterOutTables(s.levels[compactedLevel], oldL0Tables)

	// Determine and ensure target level exists
	targetLevel := s.getTargetLevel(compactedLevel)

	// Update target level: remove old tables, add new ones, sort
	s.levels[targetLevel] = s.buildNewTargetLevel(targetLevel, oldL1Tables, newTables)

	// Update manifest
	s.updateManifestAfterCompaction(targetLevel, newTables, oldL0Tables, oldL1Tables)
}

// filterOutTables returns tables from source excluding any with IDs matching exclusions.
func filterOutTables(source, exclusions []*SSTable) []*SSTable {
	result := make([]*SSTable, 0, len(source))
	for _, t := range source {
		if !tableInSlice(t.ID, exclusions) {
			result = append(result, t)
		}
	}
	return result
}

// tableInSlice returns true if a table with the given ID exists in the slice.
func tableInSlice(id uint32, tables []*SSTable) bool {
	for _, t := range tables {
		if t.ID == id {
			return true
		}
	}
	return false
}

// getTargetLevel returns the target level for compaction and ensures it exists.
func (s *Store) getTargetLevel(compactedLevel int) int {
	targetLevel := 1
	if compactedLevel > 0 {
		targetLevel = compactedLevel + 1
	}
	for len(s.levels) <= targetLevel {
		s.levels = append(s.levels, nil)
	}
	return targetLevel
}

// buildNewTargetLevel creates the new target level by filtering, adding, and sorting tables.
func (s *Store) buildNewTargetLevel(targetLevel int, oldTables, newTables []*SSTable) []*SSTable {
	result := filterOutTables(s.levels[targetLevel], oldTables)
	result = append(result, newTables...)
	sort.Slice(result, func(i, j int) bool {
		return CompareKeys(result[i].MinKey(), result[j].MinKey()) < 0
	})
	return result
}

// updateManifestAfterCompaction updates the manifest with new and deleted tables.
func (s *Store) updateManifestAfterCompaction(targetLevel int, newTables, oldL0Tables, oldL1Tables []*SSTable) {
	if s.manifest == nil {
		return
	}

	for _, sst := range newTables {
		s.manifest.AddTable(sstableToMeta(sst, targetLevel))
	}

	deleteIDs := collectTableIDs(oldL0Tables, oldL1Tables)
	if len(deleteIDs) > 0 {
		s.manifest.DeleteTables(deleteIDs)
	}
}

// sstableToMeta creates a TableMeta from an SSTable.
func sstableToMeta(sst *SSTable, level int) *TableMeta {
	return &TableMeta{
		ID:          sst.ID,
		Level:       level,
		MinKey:      sst.MinKey(),
		MaxKey:      sst.MaxKey(),
		NumKeys:     sst.Footer.NumKeys,
		FileSize:    int64(sst.Footer.FileSize),
		IndexOffset: sst.Footer.IndexOffset,
		IndexSize:   sst.Footer.IndexSize,
		BloomOffset: sst.Footer.BloomOffset,
		BloomSize:   sst.Footer.BloomSize,
	}
}

// collectTableIDs returns all table IDs from the given slices.
func collectTableIDs(tableSets ...[]*SSTable) []uint32 {
	var ids []uint32
	for _, tables := range tableSets {
		for _, t := range tables {
			ids = append(ids, t.ID)
		}
	}
	return ids
}

// releaseLock releases the exclusive lock file.
func (s *Store) releaseLock() {
	if s.lockFile != nil {
		releaseLockFile(s.lockFile)
		s.lockFile.Close()
		s.lockFile = nil
	}
}

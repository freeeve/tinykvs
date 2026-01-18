package tinykvs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// Compaction

func (w *writer) maybeScheduleCompaction() {
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
func (w *writer) compactionLoop() {
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
func (w *writer) runCompaction(task compactionTask) {
	if task.level == 0 {
		w.compactL0ToL1()
	} else {
		w.compactLevelToNext(task.level)
	}

	// Re-check if more compaction is needed (e.g., remaining L0 tables after batch)
	w.maybeScheduleCompaction()
}

func (w *writer) compactL0ToL1() {
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
	mergeRes, err := w.mergeTables(allTables, 1)
	if err != nil {
		return
	}
	newTables := mergeRes.tables

	// Update store with new tables
	w.store.replaceTablesAfterCompaction(0, l0Tables, l1Tables, newTables)

	// Update reader
	w.reader.SetLevels(w.store.getLevels())

	// Remove old files and close old tables
	for _, t := range l0Tables {
		w.store.cache.RemoveByFileID(t.ID)
		t.Close()
		if err := os.Remove(t.Path); err != nil {
			log.Printf("[compaction] Warning: failed to remove L0 file %s: %v", t.Path, err)
		}
	}
	for _, t := range l1Tables {
		w.store.cache.RemoveByFileID(t.ID)
		t.Close()
		if err := os.Remove(t.Path); err != nil {
			log.Printf("[compaction] Warning: failed to remove L1 file %s: %v", t.Path, err)
		}
	}
}

func (w *writer) compactLevelToNext(level int) {
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
	mergeRes, err := w.mergeTables(allTables, level+1)
	if err != nil {
		return
	}
	newTables := mergeRes.tables

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

// mergeResult contains the output of a merge operation.
type mergeResult struct {
	tables            []*SSTable
	uncompressedBytes uint64
}

// mergeTables merges multiple SSTables into new ones at target level.
func (w *writer) mergeTables(tables []*SSTable, targetLevel int) (*mergeResult, error) {
	if len(tables) == 0 {
		return &mergeResult{}, nil
	}

	// Create merge iterator
	mergeIter := newMergeIterator(tables, w.store.cache, w.store.opts.VerifyChecksums)
	defer mergeIter.Close()

	// Estimate keys per output table for bloom filter sizing
	// This is approximate since we don't know deduplication ratio upfront
	var totalKeys uint
	var totalBytes int64
	for _, t := range tables {
		totalKeys += uint(t.Footer.NumKeys)
		totalBytes += t.Size() // Use actual file size, not Footer.FileSize
	}
	// Estimate output tables based on total bytes, not key count
	maxTableSize := w.maxTableSize()
	estimatedOutputTables := (totalBytes + maxTableSize - 1) / maxTableSize
	if estimatedOutputTables < 1 {
		estimatedOutputTables = 1
	}
	keysPerTable := totalKeys / uint(estimatedOutputTables)
	if keysPerTable < 1000 {
		keysPerTable = totalKeys // small dataset, use total
	}

	// Create output SSTable
	var newTables []*SSTable
	var writer *sstableWriter
	var currentKeys uint
	var totalUncompressed uint64
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
			// L1+ tables don't need bloom filters (non-overlapping key ranges)
			writer, err = newSSTableWriter(id, path, keysPerTable, w.store.opts, false)
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
		// Check every 100 keys using internal size tracker (no syscall)
		if currentKeys%100 == 0 && writer.Size() >= maxTableSize {
			if err := writer.Finish(targetLevel); err != nil {
				writer.Abort()
				return nil, err
			}
			totalUncompressed += writer.UncompressedBytes()
			writer.Close()

			sst, err := OpenSSTable(writer.ID(), writer.Path())
			if err != nil {
				return nil, err
			}
			newTables = append(newTables, sst)
			writer = nil
		}
	}

	// Finish last SSTable
	if writer != nil {
		if err := writer.Finish(targetLevel); err != nil {
			writer.Abort()
			return nil, err
		}
		totalUncompressed += writer.UncompressedBytes()
		writer.Close()

		sst, err := OpenSSTable(writer.ID(), writer.Path())
		if err != nil {
			return nil, err
		}
		newTables = append(newTables, sst)
	}

	return &mergeResult{
		tables:            newTables,
		uncompressedBytes: totalUncompressed,
	}, nil
}

func (w *writer) levelSize(tables []*SSTable) int64 {
	var size int64
	for _, t := range tables {
		size += t.Size()
	}
	return size
}

func (w *writer) maxLevelSize(level int) int64 {
	// L1: configurable (default 10MB), each subsequent level LevelSizeMultiplier larger
	base := w.store.opts.L1MaxSize
	if base <= 0 {
		base = 10 * 1024 * 1024 // 10MB default
	}
	for i := 1; i < level; i++ {
		base *= int64(w.store.opts.LevelSizeMultiplier)
	}
	return base
}

func (w *writer) maxTableSize() int64 {
	return 64 * 1024 * 1024 // 64MB per SSTable
}

// ForceCompact forces compaction of all levels that need it.
// First compacts all L0 tables to L1, then compacts any levels exceeding their size limit.
func (w *writer) ForceCompact() error {
	// First: compact all L0 tables
	for {
		levels := w.reader.GetLevels()
		if len(levels) == 0 || len(levels[0]) == 0 {
			break // No more L0 tables
		}
		w.compactL0ToL1()
	}

	// Second: compact any levels that exceed their size limit
	for {
		levels := w.reader.GetLevels()
		compacted := false

		for level := 1; level < len(levels); level++ {
			levelSize := w.levelSize(levels[level])
			maxSize := w.maxLevelSize(level)
			if levelSize > maxSize {
				w.compactLevelToNext(level)
				compacted = true
				break // Re-check from the beginning after compaction
			}
		}

		if !compacted {
			break // No more levels to compact
		}
	}

	return nil
}

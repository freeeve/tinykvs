package tinykvs

import (
	"fmt"
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

	l0Tables := limitL0BatchSize(levels[0], w.store.opts.L0CompactionBatchSize)
	minKey, maxKey := getTableKeyRange(l0Tables)
	l1Tables := findOverlappingTables(levels, 1, minKey, maxKey)

	reversedL0 := reverseTables(l0Tables)
	allTables := append(reversedL0, l1Tables...)

	mergeRes, err := w.mergeTables(allTables, 1)
	if err != nil {
		return
	}

	w.store.replaceTablesAfterCompaction(0, l0Tables, l1Tables, mergeRes.tables)
	w.reader.SetLevels(w.store.getLevels())
	w.removeCompactedTables(l0Tables, l1Tables)
}

// limitL0BatchSize returns at most batchSize tables from the beginning.
func limitL0BatchSize(tables []*SSTable, batchSize int) []*SSTable {
	if batchSize > 0 && len(tables) > batchSize {
		return tables[:batchSize]
	}
	return tables
}

// getTableKeyRange finds the min and max keys across all tables.
func getTableKeyRange(tables []*SSTable) (minKey, maxKey []byte) {
	for _, t := range tables {
		if minKey == nil || CompareKeys(t.MinKey(), minKey) < 0 {
			minKey = t.MinKey()
		}
		if maxKey == nil || CompareKeys(t.MaxKey(), maxKey) > 0 {
			maxKey = t.MaxKey()
		}
	}
	return minKey, maxKey
}

// findOverlappingTables returns tables from the specified level that overlap with the key range.
func findOverlappingTables(levels [][]*SSTable, level int, minKey, maxKey []byte) []*SSTable {
	if level >= len(levels) {
		return nil
	}
	var result []*SSTable
	for _, t := range levels[level] {
		if CompareKeys(t.MaxKey(), minKey) >= 0 && CompareKeys(t.MinKey(), maxKey) <= 0 {
			result = append(result, t)
		}
	}
	return result
}

// reverseTables returns a new slice with tables in reverse order.
func reverseTables(tables []*SSTable) []*SSTable {
	reversed := make([]*SSTable, len(tables))
	for i, t := range tables {
		reversed[len(tables)-1-i] = t
	}
	return reversed
}

// removeCompactedTables marks compacted tables for removal.
// The files will be closed and deleted when all readers release their references.
func (w *writer) removeCompactedTables(tableSets ...[]*SSTable) {
	for _, tables := range tableSets {
		for _, t := range tables {
			w.store.cache.RemoveByFileID(t.ID)
			t.MarkForRemoval()
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

	// Mark old tables for removal (will be deleted when all readers release refs)
	w.store.cache.RemoveByFileID(table.ID)
	table.MarkForRemoval()
	for _, t := range nextLevelTables {
		w.store.cache.RemoveByFileID(t.ID)
		t.MarkForRemoval()
	}
}

// mergeResult contains the output of a merge operation.
type mergeResult struct {
	tables            []*SSTable
	uncompressedBytes uint64
}

// mergeState holds state during table merging.
type mergeState struct {
	writer            *sstableWriter
	keysPerTable      uint
	maxTableSize      uint64
	currentKeys       uint
	tables            []*SSTable
	totalUncompressed uint64
}

// mergeTables merges multiple SSTables into new ones at target level.
func (w *writer) mergeTables(tables []*SSTable, targetLevel int) (*mergeResult, error) {
	if len(tables) == 0 {
		return &mergeResult{}, nil
	}

	mergeIter := newMergeIterator(tables, w.store.cache, w.store.opts.VerifyChecksums)
	defer mergeIter.Close()

	state := &mergeState{
		keysPerTable: w.estimateKeysPerTable(tables),
		maxTableSize: w.maxTableSize(),
	}
	isLastLevel := targetLevel >= w.store.opts.MaxLevels-1

	for mergeIter.Next() {
		entry := mergeIter.Entry()

		if isLastLevel && entry.Value.IsTombstone() {
			continue
		}

		if err := w.processMergeEntry(entry, state, targetLevel); err != nil {
			return nil, err
		}
	}

	if err := w.finalizeMerge(state, targetLevel); err != nil {
		return nil, err
	}

	return &mergeResult{
		tables:            state.tables,
		uncompressedBytes: state.totalUncompressed,
	}, nil
}

// processMergeEntry handles writing a single entry during merge.
func (w *writer) processMergeEntry(entry Entry, state *mergeState, targetLevel int) error {
	if state.writer == nil {
		var err error
		state.writer, err = w.createSSTableWriter(state.keysPerTable)
		if err != nil {
			return err
		}
		state.currentKeys = 0
	}

	if err := state.writer.Add(entry); err != nil {
		state.writer.Abort()
		return err
	}
	state.currentKeys++

	if state.currentKeys%100 == 0 && uint64(state.writer.Size()) >= state.maxTableSize {
		return w.rotateTable(state, targetLevel)
	}
	return nil
}

// rotateTable finishes the current table and prepares for the next.
func (w *writer) rotateTable(state *mergeState, targetLevel int) error {
	sst, uncompressed, err := w.finishSSTable(state.writer, targetLevel)
	if err != nil {
		return err
	}
	state.totalUncompressed += uncompressed
	state.tables = append(state.tables, sst)
	state.writer = nil
	return nil
}

// finalizeMerge finishes any remaining table data.
func (w *writer) finalizeMerge(state *mergeState, targetLevel int) error {
	if state.writer != nil {
		return w.rotateTable(state, targetLevel)
	}
	return nil
}

// estimateKeysPerTable calculates expected keys per output table for bloom filter sizing.
func (w *writer) estimateKeysPerTable(tables []*SSTable) uint {
	var totalKeys uint
	var totalBytes uint64
	for _, t := range tables {
		totalKeys += uint(t.Footer.NumKeys)
		totalBytes += uint64(t.Size())
	}

	maxTableSize := w.maxTableSize()
	estimatedOutputTables := (totalBytes + maxTableSize - 1) / maxTableSize
	if estimatedOutputTables < 1 {
		estimatedOutputTables = 1
	}

	keysPerTable := totalKeys / uint(estimatedOutputTables)
	if keysPerTable < 1000 {
		keysPerTable = totalKeys
	}
	return keysPerTable
}

// createSSTableWriter creates a new SSTable writer for compaction output.
func (w *writer) createSSTableWriter(keysPerTable uint) (*sstableWriter, error) {
	id := w.store.nextSSTableID()
	path := filepath.Join(w.store.opts.Dir, fmt.Sprintf("%06d.sst", id))
	return newSSTableWriter(id, path, keysPerTable, w.store.opts, false)
}

// finishSSTable finishes writing, closes, and opens an SSTable.
func (w *writer) finishSSTable(sstWriter *sstableWriter, targetLevel int) (*SSTable, uint64, error) {
	if err := sstWriter.Finish(targetLevel); err != nil {
		sstWriter.Abort()
		return nil, 0, err
	}
	uncompressed := sstWriter.UncompressedBytes()
	sstWriter.Close()

	sst, err := OpenSSTable(sstWriter.ID(), sstWriter.Path())
	if err != nil {
		return nil, 0, err
	}
	return sst, uncompressed, nil
}

func (w *writer) levelSize(tables []*SSTable) int64 {
	var size int64
	for _, t := range tables {
		size += t.Size()
	}
	return size
}

func (w *writer) maxLevelSize(level int) int64 {
	// L1: configurable (default 1GB), each subsequent level LevelSizeMultiplier larger
	base := w.store.opts.L1MaxSize
	if base <= 0 {
		base = 1024 * 1024 * 1024 // 1GB fallback
	}
	for i := 1; i < level; i++ {
		base *= int64(w.store.opts.LevelSizeMultiplier)
	}
	return base
}

func (w *writer) maxTableSize() uint64 {
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

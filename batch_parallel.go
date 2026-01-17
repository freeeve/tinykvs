package tinykvs

import (
	"runtime"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// KeyStruct pairs a key with a struct value for bulk operations.
type KeyStruct struct {
	Key   []byte
	Value any
}

// putStructsParallel encodes and adds multiple structs in parallel.
// Uses all available CPU cores for encoding, then appends results atomically.
func (b *Batch) putStructsParallel(items []KeyStruct) error {
	return b.putStructsParallelN(items, runtime.NumCPU())
}

// putStructsParallelN encodes and adds multiple structs using n workers.
func (b *Batch) putStructsParallelN(items []KeyStruct, numWorkers int) error {
	if len(items) == 0 {
		return nil
	}

	if numWorkers <= 1 || len(items) < numWorkers {
		// Fall back to sequential for small batches
		for _, item := range items {
			if err := b.PutStruct(item.Key, item.Value); err != nil {
				return err
			}
		}
		return nil
	}

	// Pre-allocate results slice
	results := make([]batchOp, len(items))
	var firstErr error
	var errOnce sync.Once

	// Create work chunks
	chunkSize := (len(items) + numWorkers - 1) / numWorkers
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > len(items) {
			end = len(items)
		}
		if start >= end {
			break
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				item := items[i]
				data, err := msgpack.Marshal(item.Value)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				keyCopy := make([]byte, len(item.Key))
				copy(keyCopy, item.Key)
				results[i] = batchOp{key: keyCopy, value: MsgpackValue(data), delete: false}
			}
		}(start, end)
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	// Append all results
	b.ops = append(b.ops, results...)
	return nil
}

// PutStructs writes multiple structs in parallel.
// This is the fastest way to ingest many structs - it parallelizes encoding
// across all CPU cores, then writes atomically.
func (s *Store) PutStructs(items []KeyStruct) error {
	if len(items) == 0 {
		return nil
	}

	// Parallel encode
	batch := NewBatch()
	if err := batch.putStructsParallel(items); err != nil {
		return err
	}

	// Write atomically
	return s.WriteBatch(batch)
}

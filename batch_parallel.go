package tinykvs

import (
	"runtime"
	"sync"

	"github.com/freeeve/msgpck"
)

// KeyValue pairs a key with a struct value for bulk operations.
type KeyValue[T any] struct {
	Key   []byte
	Value *T
}

// PutStructs writes multiple structs in parallel using cached encoder.
// Parallelizes encoding across CPU cores, then writes atomically.
func PutStructs[T any](s *Store, items []KeyValue[T]) error {
	if len(items) == 0 {
		return nil
	}

	batch := NewBatch()
	if err := batchPutStructsParallel(batch, items, runtime.NumCPU()); err != nil {
		return err
	}

	return s.WriteBatch(batch)
}

// batchPutStructsParallel encodes structs in parallel using cached encoder.
func batchPutStructsParallel[T any](b *Batch, items []KeyValue[T], numWorkers int) error {
	if len(items) == 0 {
		return nil
	}

	enc := msgpck.GetStructEncoder[T]()

	if numWorkers <= 1 || len(items) < numWorkers {
		// Sequential for small batches
		for _, item := range items {
			data, err := enc.EncodeCopy(item.Value)
			if err != nil {
				return err
			}
			b.Put(item.Key, MsgpackValue(data))
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
				data, err := enc.EncodeCopy(item.Value)
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

	b.ops = append(b.ops, results...)
	return nil
}

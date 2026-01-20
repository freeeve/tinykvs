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
		return encodeItemsSequential(b, enc, items)
	}

	ops, err := encodeItemsParallel(enc, items, numWorkers)
	if err != nil {
		return err
	}

	b.ops = append(b.ops, ops...)
	return nil
}

// encodeItemsSequential encodes items one at a time for small batches.
func encodeItemsSequential[T any](b *Batch, enc *msgpck.StructEncoder[T], items []KeyValue[T]) error {
	for _, item := range items {
		data, err := enc.EncodeCopy(item.Value)
		if err != nil {
			return err
		}
		b.Put(item.Key, MsgpackValue(data))
	}
	return nil
}

// encodeItemsParallel encodes items across multiple workers.
func encodeItemsParallel[T any](enc *msgpck.StructEncoder[T], items []KeyValue[T], numWorkers int) ([]batchOp, error) {
	results := make([]batchOp, len(items))
	var firstErr error
	var errOnce sync.Once

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
			encodeChunk(enc, items, results, start, end, &firstErr, &errOnce)
		}(start, end)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return results, nil
}

// encodeChunk encodes a range of items into the results slice.
func encodeChunk[T any](enc *msgpck.StructEncoder[T], items []KeyValue[T], results []batchOp, start, end int, firstErr *error, errOnce *sync.Once) {
	for i := start; i < end; i++ {
		item := items[i]
		data, err := enc.EncodeCopy(item.Value)
		if err != nil {
			errOnce.Do(func() { *firstErr = err })
			return
		}
		keyCopy := make([]byte, len(item.Key))
		copy(keyCopy, item.Key)
		results[i] = batchOp{key: keyCopy, value: MsgpackValue(data), delete: false}
	}
}

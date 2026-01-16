package tinykvs

import (
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// Batch accumulates multiple operations to be applied atomically.
type Batch struct {
	ops []batchOp
}

type batchOp struct {
	key    []byte
	value  Value
	delete bool
}

// NewBatch creates a new batch for atomic writes.
func NewBatch() *Batch {
	return &Batch{}
}

// Put adds a put operation to the batch.
func (b *Batch) Put(key []byte, value Value) {
	// Copy key to avoid issues if caller reuses buffer
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	b.ops = append(b.ops, batchOp{key: keyCopy, value: value, delete: false})
}

// Delete adds a delete operation to the batch.
func (b *Batch) Delete(key []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	b.ops = append(b.ops, batchOp{key: keyCopy, delete: true})
}

// PutString adds a string put operation.
func (b *Batch) PutString(key []byte, value string) {
	b.Put(key, StringValue(value))
}

// PutInt64 adds an int64 put operation.
func (b *Batch) PutInt64(key []byte, value int64) {
	b.Put(key, Int64Value(value))
}

// PutBytes adds a bytes put operation.
func (b *Batch) PutBytes(key []byte, value []byte) {
	b.Put(key, BytesValue(value))
}

// PutMap adds a record put operation.
func (b *Batch) PutMap(key []byte, fields map[string]any) error {
	data, err := msgpack.Marshal(fields)
	if err != nil {
		return err
	}
	b.Put(key, MsgpackValue(data))
	return nil
}

// PutJson adds a JSON string put operation.
func (b *Batch) PutJson(key []byte, data any) error {
	jsonBytes, err := EncodeJson(data)
	if err != nil {
		return err
	}
	b.Put(key, StringValue(string(jsonBytes)))
	return nil
}

// PutStruct adds a struct put operation using msgpack serialization.
func (b *Batch) PutStruct(key []byte, v any) error {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return err
	}
	b.Put(key, MsgpackValue(data))
	return nil
}

// Len returns the number of operations in the batch.
func (b *Batch) Len() int {
	return len(b.ops)
}

// Reset clears the batch for reuse.
func (b *Batch) Reset() {
	b.ops = b.ops[:0]
}

// WriteBatch atomically applies all operations in the batch.
// All operations are written to wal together before applying to memtable.
func (s *Store) WriteBatch(batch *Batch) error {
	if batch == nil || len(batch.ops) == 0 {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	return s.writer.WriteBatch(batch.ops)
}

// PutIfNotExists stores a value only if the key doesn't exist.
// Returns ErrKeyExists if the key already exists.
func (s *Store) PutIfNotExists(key []byte, value Value) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	// Check if key exists
	_, err := s.reader.Get(key)
	if err == nil {
		return ErrKeyExists
	}
	if err != ErrKeyNotFound {
		return err
	}

	return s.writer.Put(key, value)
}

// PutIfEquals stores a value only if the current value equals expected.
// Returns ErrConditionFailed if values don't match, ErrKeyNotFound if key doesn't exist.
func (s *Store) PutIfEquals(key []byte, value Value, expected Value) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	// Get current value
	current, err := s.reader.Get(key)
	if err != nil {
		return err
	}

	// Compare values
	if !valuesEqual(current, expected) {
		return ErrConditionFailed
	}

	return s.writer.Put(key, value)
}

// Increment atomically adds delta to an int64 value and returns the new value.
// If key doesn't exist, it's treated as 0.
// Returns error if existing value is not an int64.
func (s *Store) Increment(key []byte, delta int64) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return 0, ErrStoreClosed
	}

	// Get current value
	var current int64
	val, err := s.reader.Get(key)
	if err == nil {
		if val.Type != ValueTypeInt64 {
			return 0, ErrTypeMismatch
		}
		current = val.Int64
	} else if err != ErrKeyNotFound {
		return 0, err
	}

	// Compute new value
	newVal := current + delta

	// Write new value
	if err := s.writer.Put(key, Int64Value(newVal)); err != nil {
		return 0, err
	}

	return newVal, nil
}

// DeleteRange deletes all keys in the range [start, end).
// This is more efficient than deleting keys one by one.
// Returns the number of keys deleted.
func (s *Store) DeleteRange(start, end []byte) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return 0, ErrStoreClosed
	}

	// Collect keys in range
	var keys [][]byte
	err := s.reader.ScanPrefix(nil, func(key []byte, _ Value) bool {
		if CompareKeys(key, start) >= 0 && CompareKeys(key, end) < 0 {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			keys = append(keys, keyCopy)
		}
		// Stop if we've passed the end
		return CompareKeys(key, end) < 0
	})
	if err != nil {
		return 0, err
	}

	// Delete all keys in batch
	if len(keys) == 0 {
		return 0, nil
	}

	batch := &Batch{}
	for _, key := range keys {
		batch.Delete(key)
	}

	if err := s.writer.WriteBatch(batch.ops); err != nil {
		return 0, err
	}

	return int64(len(keys)), nil
}

// DeletePrefix deletes all keys with the given prefix.
// Returns the number of keys deleted.
func (s *Store) DeletePrefix(prefix []byte) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed {
		return 0, ErrStoreClosed
	}

	// Collect keys with prefix
	var keys [][]byte
	// Use reader directly since we already hold the lock
	err := s.reader.ScanPrefix(prefix, func(key []byte, _ Value) bool {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keys = append(keys, keyCopy)
		return true
	})
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	// Delete all keys in batch
	batch := &Batch{}
	for _, key := range keys {
		batch.Delete(key)
	}

	if err := s.writer.WriteBatch(batch.ops); err != nil {
		return 0, err
	}

	return int64(len(keys)), nil
}

// valuesEqual compares two values for equality.
func valuesEqual(a, b Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case ValueTypeInt64:
		return a.Int64 == b.Int64
	case ValueTypeFloat64:
		return a.Float64 == b.Float64
	case ValueTypeBool:
		return a.Bool == b.Bool
	case ValueTypeString, ValueTypeBytes:
		return string(a.Bytes) == string(b.Bytes)
	case ValueTypeRecord:
		return reflect.DeepEqual(a.Record, b.Record)
	case ValueTypeTombstone:
		return true
	default:
		return false
	}
}

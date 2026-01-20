package tinykvs

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/freeeve/msgpck"
)

// Error format strings for type mismatches.
const (
	errExpectedMsgpackOrRecord = "expected msgpack or record, got %d"
	errExpectedMsgpack         = "expected msgpack, got %d"
)

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

// PutMap stores a structured record with named fields.
func (s *Store) PutMap(key []byte, fields map[string]any) error {
	data, err := msgpck.MarshalCopy(fields)
	if err != nil {
		return err
	}
	return s.Put(key, MsgpackValue(data))
}

// PutJson stores a record as a JSON string.
// Use this when you want human-readable storage instead of binary msgpack.
func (s *Store) PutJson(key []byte, data any) error {
	jsonBytes, err := EncodeJson(data)
	if err != nil {
		return err
	}
	return s.Put(key, StringValue(string(jsonBytes)))
}

// GetJson retrieves a JSON string and decodes it into the provided destination.
func (s *Store) GetJson(key []byte, dest any) error {
	val, err := s.Get(key)
	if err != nil {
		return err
	}
	if val.Type != ValueTypeString {
		return fmt.Errorf("expected string (JSON), got type %d", val.Type)
	}
	return DecodeJson(val.Bytes, dest)
}

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

// GetMap retrieves a structured record by key.
func (s *Store) GetMap(key []byte) (map[string]any, error) {
	val, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	switch val.Type {
	case ValueTypeMsgpack:
		return msgpck.UnmarshalMapStringAny(val.Bytes, false)
	case ValueTypeRecord:
		// Backward compatibility
		return val.Record, nil
	default:
		return nil, fmt.Errorf(errExpectedMsgpackOrRecord, val.Type)
	}
}

// structToMap converts a struct to map[string]any using reflection.
// Uses msgpack tag, then json tag, then field name for keys.
func structToMap(v any) (map[string]any, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %v", rv.Kind())
	}

	rt := rv.Type()
	result := make(map[string]any, rt.NumField())

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}

		// Get field name from tags
		name := field.Tag.Get("msgpack")
		if name == "" {
			name = field.Tag.Get("json")
		}
		if name == "" || name == "-" {
			name = field.Name
		}
		// Handle tag options like "name,omitempty"
		if idx := strings.Index(name, ","); idx != -1 {
			name = name[:idx]
		}
		if name == "-" {
			continue
		}

		fv := rv.Field(i)
		result[name] = convertToAny(fv)
	}

	return result, nil
}

// convertToAny converts a reflect.Value to any, handling nested structs.
func convertToAny(rv reflect.Value) any {
	if !rv.IsValid() {
		return nil
	}

	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return convertToAny(rv.Elem())
	case reflect.Struct:
		m, _ := structToMap(rv.Interface())
		return m
	case reflect.Slice:
		if rv.IsNil() {
			return nil
		}
		s := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			s[i] = convertToAny(rv.Index(i))
		}
		return s
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		m := make(map[string]any, rv.Len())
		for _, k := range rv.MapKeys() {
			m[fmt.Sprint(k.Interface())] = convertToAny(rv.MapIndex(k))
		}
		return m
	default:
		return rv.Interface()
	}
}

// mapToStruct populates a struct from a map using reflection.
func mapToStruct(m map[string]any, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to struct")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to struct, got %v", rv.Kind())
	}

	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}

		// Get field name from tags
		name := field.Tag.Get("msgpack")
		if name == "" {
			name = field.Tag.Get("json")
		}
		if name == "" || name == "-" {
			name = field.Name
		}
		if idx := strings.Index(name, ","); idx != -1 {
			name = name[:idx]
		}
		if name == "-" {
			continue
		}

		val, ok := m[name]
		if !ok {
			continue
		}

		fv := rv.Field(i)
		if err := setFieldValue(fv, val); err != nil {
			return fmt.Errorf("field %s: %w", name, err)
		}
	}

	return nil
}

// setFieldValue sets a struct field from an any value.
func setFieldValue(fv reflect.Value, val any) error {
	if val == nil {
		return nil
	}
	if !fv.CanSet() {
		return nil
	}

	rv := reflect.ValueOf(val)

	// Handle nested struct
	if fv.Kind() == reflect.Struct && rv.Kind() == reflect.Map {
		if m, ok := val.(map[string]any); ok {
			return mapToStruct(m, fv.Addr().Interface())
		}
	}

	// Handle slice
	if fv.Kind() == reflect.Slice && rv.Kind() == reflect.Slice {
		slice := reflect.MakeSlice(fv.Type(), rv.Len(), rv.Len())
		for i := 0; i < rv.Len(); i++ {
			elem := slice.Index(i)
			if err := setFieldValue(elem, rv.Index(i).Interface()); err != nil {
				return err
			}
		}
		fv.Set(slice)
		return nil
	}

	// Try direct assignment
	if rv.Type().AssignableTo(fv.Type()) {
		fv.Set(rv)
		return nil
	}

	// Try conversion for numeric types
	if rv.Type().ConvertibleTo(fv.Type()) {
		fv.Set(rv.Convert(fv.Type()))
		return nil
	}

	// Handle int64/float64 from JSON-like sources
	switch fv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := val.(type) {
		case float64:
			fv.SetInt(int64(v))
			return nil
		case int64:
			fv.SetInt(v)
			return nil
		case int:
			fv.SetInt(int64(v))
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch v := val.(type) {
		case float64:
			fv.SetFloat(v)
			return nil
		case int64:
			fv.SetFloat(float64(v))
			return nil
		case int:
			fv.SetFloat(float64(v))
			return nil
		}
	case reflect.Bool:
		if b, ok := val.(bool); ok {
			fv.SetBool(b)
			return nil
		}
	case reflect.String:
		if s, ok := val.(string); ok {
			fv.SetString(s)
			return nil
		}
	}

	return nil // Skip incompatible types silently
}

// ScanPrefixMaps scans keys with the given prefix and decodes each value as a map.
func (s *Store) ScanPrefixMaps(prefix []byte, fn func(key []byte, m map[string]any) bool) error {
	return s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		m, err := decodeAsMap(val)
		if err != nil {
			return true // skip invalid entries
		}
		return fn(key, m)
	})
}

// ScanRangeMaps scans keys in [start, end) and decodes each value as a map.
func (s *Store) ScanRangeMaps(start, end []byte, fn func(key []byte, m map[string]any) bool) error {
	return s.ScanRange(start, end, func(key []byte, val Value) bool {
		m, err := decodeAsMap(val)
		if err != nil {
			return true // skip invalid entries
		}
		return fn(key, m)
	})
}

// ScanPrefixStructs scans keys with the given prefix and decodes each value into a struct.
// Uses pre-registered decoder for best performance.
func ScanPrefixStructs[T any](s *Store, prefix []byte, fn func(key []byte, val *T) bool) error {
	dec := msgpck.GetStructDecoder[T](false)
	return s.ScanPrefix(prefix, func(key []byte, v Value) bool {
		if v.Type != ValueTypeMsgpack {
			// Fall back to reflection for legacy record types
			var dest T
			if err := decodeAsStruct(v, &dest); err != nil {
				return true
			}
			return fn(key, &dest)
		}
		var dest T
		if err := dec.Decode(v.Bytes, &dest); err != nil {
			return true // skip invalid entries
		}
		return fn(key, &dest)
	})
}

// ScanRangeStructs scans keys in [start, end) and decodes each value into a struct.
// Uses pre-registered decoder for best performance.
func ScanRangeStructs[T any](s *Store, start, end []byte, fn func(key []byte, val *T) bool) error {
	dec := msgpck.GetStructDecoder[T](false)
	return s.ScanRange(start, end, func(key []byte, v Value) bool {
		if v.Type != ValueTypeMsgpack {
			// Fall back to reflection for legacy record types
			var dest T
			if err := decodeAsStruct(v, &dest); err != nil {
				return true
			}
			return fn(key, &dest)
		}
		var dest T
		if err := dec.Decode(v.Bytes, &dest); err != nil {
			return true // skip invalid entries
		}
		return fn(key, &dest)
	})
}

// ScanPrefixJson scans keys with the given prefix and decodes JSON values into a struct.
func ScanPrefixJson[T any](s *Store, prefix []byte, fn func(key []byte, val *T) bool) error {
	return s.ScanPrefix(prefix, func(key []byte, v Value) bool {
		if v.Type != ValueTypeString {
			return true // skip non-JSON entries
		}
		var dest T
		if err := DecodeJson(v.Bytes, &dest); err != nil {
			return true // skip invalid entries
		}
		return fn(key, &dest)
	})
}

// ScanRangeJson scans keys in [start, end) and decodes JSON values into a struct.
func ScanRangeJson[T any](s *Store, start, end []byte, fn func(key []byte, val *T) bool) error {
	return s.ScanRange(start, end, func(key []byte, v Value) bool {
		if v.Type != ValueTypeString {
			return true // skip non-JSON entries
		}
		var dest T
		if err := DecodeJson(v.Bytes, &dest); err != nil {
			return true // skip invalid entries
		}
		return fn(key, &dest)
	})
}

// decodeAsMap decodes a Value as map[string]any.
func decodeAsMap(val Value) (map[string]any, error) {
	switch val.Type {
	case ValueTypeMsgpack:
		return msgpck.UnmarshalMapStringAny(val.Bytes, false)
	case ValueTypeRecord:
		return val.Record, nil
	default:
		return nil, fmt.Errorf(errExpectedMsgpackOrRecord, val.Type)
	}
}

// decodeAsStruct decodes a Value into a struct pointer.
func decodeAsStruct(val Value, dest any) error {
	switch val.Type {
	case ValueTypeMsgpack:
		return msgpck.UnmarshalStruct(val.Bytes, dest)
	case ValueTypeRecord:
		return mapToStruct(val.Record, dest)
	default:
		return fmt.Errorf(errExpectedMsgpackOrRecord, val.Type)
	}
}

// Fast generic struct APIs - use cached codecs for best performance

// PutStruct stores a struct using cached encoder.
func PutStruct[T any](s *Store, key []byte, v *T) error {
	enc := msgpck.GetStructEncoder[T]()
	data, err := enc.EncodeCopy(v)
	if err != nil {
		return err
	}
	return s.Put(key, MsgpackValue(data))
}

// BatchPutStruct adds a struct to a batch using cached encoder.
func BatchPutStruct[T any](b *Batch, key []byte, v *T) error {
	enc := msgpck.GetStructEncoder[T]()
	data, err := enc.EncodeCopy(v)
	if err != nil {
		return err
	}
	b.Put(key, MsgpackValue(data))
	return nil
}

// GetStruct retrieves and decodes a struct using pre-registered decoder (faster than method version).
// Use this in hot paths where performance matters.
func GetStruct[T any](s *Store, key []byte) (*T, error) {
	val, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	if val.Type != ValueTypeMsgpack {
		return nil, fmt.Errorf(errExpectedMsgpack, val.Type)
	}
	dec := msgpck.GetStructDecoder[T](false)
	var dest T
	if err := dec.Decode(val.Bytes, &dest); err != nil {
		return nil, err
	}
	return &dest, nil
}

// GetStructInto retrieves and decodes a struct into an existing pointer.
// Avoids allocation when you already have a destination.
func GetStructInto[T any](s *Store, key []byte, dest *T) error {
	val, err := s.Get(key)
	if err != nil {
		return err
	}
	if val.Type != ValueTypeMsgpack {
		return fmt.Errorf(errExpectedMsgpack, val.Type)
	}
	dec := msgpck.GetStructDecoder[T](false)
	return dec.Decode(val.Bytes, dest)
}

// Zero-copy APIs - strings point into database buffer, only valid within callback

// GetMapZeroCopy retrieves a map with zero-copy strings.
// Strings are only valid within the callback - they point into the database buffer.
// Copy any strings you need to retain before the callback returns.
func (s *Store) GetMapZeroCopy(key []byte, fn func(m map[string]any) error) error {
	val, err := s.Get(key)
	if err != nil {
		return err
	}
	switch val.Type {
	case ValueTypeMsgpack:
		return msgpck.DecodeMapFunc(val.Bytes, fn)
	case ValueTypeRecord:
		return fn(val.Record)
	default:
		return fmt.Errorf(errExpectedMsgpackOrRecord, val.Type)
	}
}

// GetStructZeroCopy retrieves a struct with zero-copy strings.
// Strings are only valid within the callback - they point into the database buffer.
// This is the fastest way to read structs when you only need temporary access.
func GetStructZeroCopy[T any](s *Store, key []byte, fn func(v *T) error) error {
	val, err := s.Get(key)
	if err != nil {
		return err
	}
	if val.Type != ValueTypeMsgpack {
		return fmt.Errorf(errExpectedMsgpack, val.Type)
	}
	return msgpck.DecodeStructFunc(val.Bytes, fn)
}

// ScanPrefixMapsZeroCopy scans keys with prefix using zero-copy decoding.
// Map strings are only valid within the callback - copy any you need to retain.
func (s *Store) ScanPrefixMapsZeroCopy(prefix []byte, fn func(key []byte, m map[string]any) bool) error {
	return s.ScanPrefix(prefix, func(key []byte, val Value) bool {
		if val.Type != ValueTypeMsgpack {
			return true // skip non-msgpack
		}
		m, err := msgpck.UnmarshalMapStringAny(val.Bytes, true)
		if err != nil {
			return true // skip invalid
		}
		return fn(key, m)
	})
}

// ScanPrefixStructsZeroCopy scans keys with prefix using zero-copy decoding.
// Struct string fields are only valid within the callback - copy any you need.
func ScanPrefixStructsZeroCopy[T any](s *Store, prefix []byte, fn func(key []byte, val *T) bool) error {
	dec := msgpck.GetStructDecoder[T](true)
	return s.ScanPrefix(prefix, func(key []byte, v Value) bool {
		if v.Type != ValueTypeMsgpack {
			return true // skip non-msgpack
		}
		var dest T
		if err := dec.Decode(v.Bytes, &dest); err != nil {
			return true // skip invalid
		}
		return fn(key, &dest)
	})
}

// ScanRangeStructsZeroCopy scans keys in [start, end) using zero-copy decoding.
func ScanRangeStructsZeroCopy[T any](s *Store, start, end []byte, fn func(key []byte, val *T) bool) error {
	dec := msgpck.GetStructDecoder[T](true)
	return s.ScanRange(start, end, func(key []byte, v Value) bool {
		if v.Type != ValueTypeMsgpack {
			return true // skip non-msgpack
		}
		var dest T
		if err := dec.Decode(v.Bytes, &dest); err != nil {
			return true // skip invalid
		}
		return fn(key, &dest)
	})
}

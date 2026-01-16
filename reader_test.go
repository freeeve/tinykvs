package tinykvs

import (
	"testing"
)

func TestHasKeyInRange(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		minKey []byte
		maxKey []byte
		want   bool
	}{
		{
			name:   "prefix within range",
			prefix: []byte("key1"),
			minKey: []byte("key0"),
			maxKey: []byte("key2"),
			want:   true,
		},
		{
			name:   "prefix equals minKey",
			prefix: []byte("key"),
			minKey: []byte("key"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "prefix before minKey",
			prefix: []byte("aaa"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   false,
		},
		{
			name:   "prefix after maxKey",
			prefix: []byte("zzz"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   false,
		},
		{
			name:   "prefix longer than maxKey - match",
			prefix: []byte("key123"),
			minKey: []byte("key"),
			maxKey: []byte("key2"),
			want:   true,
		},
		{
			name:   "prefix longer than maxKey - no match",
			prefix: []byte("key999"),
			minKey: []byte("key0"),
			maxKey: []byte("key1"),
			want:   false,
		},
		{
			name:   "maxKey shorter and prefix greater",
			prefix: []byte("zzz"),
			minKey: []byte("a"),
			maxKey: []byte("b"),
			want:   false,
		},
		{
			name:   "empty prefix matches all",
			prefix: []byte{},
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "minKey longer than prefix - match",
			prefix: []byte("key"),
			minKey: []byte("key123"),
			maxKey: []byte("key999"),
			want:   true,
		},
		{
			name:   "minKey longer than prefix - no match",
			prefix: []byte("aaa"),
			minKey: []byte("bbb123"),
			maxKey: []byte("ccc999"),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasKeyInRange(tt.prefix, tt.minKey, tt.maxKey)
			if got != tt.want {
				t.Errorf("hasKeyInRange(%q, %q, %q) = %v, want %v",
					tt.prefix, tt.minKey, tt.maxKey, got, tt.want)
			}
		})
	}
}

func TestScanPrefixEmpty(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Scan empty store
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixStopEarly(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write 100 keys
	for i := 0; i < 100; i++ {
		store.PutInt64([]byte("key"+string(rune('0'+i/10))+string(rune('0'+i%10))), int64(i))
	}
	store.Flush()

	// Stop after 5 keys
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return count < 5
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

func TestScanPrefixWithTombstones(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys then delete some
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("key"+string(rune('0'+i))), int64(i))
	}
	store.Delete([]byte("key3"))
	store.Delete([]byte("key7"))
	store.Flush()

	// Scan should skip tombstones
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 8 {
		t.Errorf("count = %d, want 8 (10 - 2 deleted)", count)
	}
}

func TestScanPrefixNoMatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys with different prefix
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("abc"+string(rune('0'+i))), int64(i))
	}
	store.Flush()

	// Scan with non-matching prefix
	count := 0
	err = store.ScanPrefix([]byte("xyz"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixClosed(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	store.Close()

	// Scan closed store should return error
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		return true
	})
	if err != ErrStoreClosed {
		t.Errorf("ScanPrefix on closed store = %v, want ErrStoreClosed", err)
	}
}

func TestScanPrefixMultipleSSTables(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Very small to force multiple SSTables
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write enough data to create multiple SSTables
	for i := 0; i < 500; i++ {
		key := []byte("key" + string(rune('0'+i/100)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i%10)))
		store.PutInt64(key, int64(i))
		if i%50 == 49 {
			store.Flush()
		}
	}
	store.Flush()

	// Scan all keys
	count := 0
	err = store.ScanPrefix([]byte("key"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 500 {
		t.Errorf("count = %d, want 500", count)
	}

	// Scan subset with specific prefix
	count = 0
	err = store.ScanPrefix([]byte("key1"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix key1 failed: %v", err)
	}
	if count != 100 {
		t.Errorf("count for key1 = %d, want 100", count)
	}
}

func TestScanPrefixAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write data
	for i := 0; i < 200; i++ {
		key := []byte("key" + string(rune('0'+i/100)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i%10)))
		store.PutInt64(key, int64(i))
	}
	store.Flush()

	// Compact
	store.Compact()

	// Scan should still work after compaction
	count := 0
	err = store.ScanPrefix([]byte("key0"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix after compact failed: %v", err)
	}
	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
}

func TestScanPrefixPrefixBeforeAllKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys starting with 'm'
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("mkey"+string(rune('0'+i))), int64(i))
	}
	store.Flush()

	// Scan with prefix 'a' which is before all keys
	count := 0
	err = store.ScanPrefix([]byte("a"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestScanPrefixPrefixAfterAllKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Write keys starting with 'a'
	for i := 0; i < 10; i++ {
		store.PutInt64([]byte("akey"+string(rune('0'+i))), int64(i))
	}
	store.Flush()

	// Scan with prefix 'z' which is after all keys
	count := 0
	err = store.ScanPrefix([]byte("z"), func(key []byte, value Value) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanPrefix failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

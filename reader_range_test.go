package tinykvs

import (
	"fmt"
	"testing"
)

func TestRangeOverlaps(t *testing.T) {
	tests := []struct {
		name   string
		start  []byte
		end    []byte
		minKey []byte
		maxKey []byte
		want   bool
	}{
		{
			name:   "range fully within table",
			start:  []byte("key5"),
			end:    []byte("key8"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "range equals table bounds",
			start:  []byte("key0"),
			end:    []byte("key9"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "range overlaps start of table",
			start:  []byte("aaa"),
			end:    []byte("key5"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "range overlaps end of table",
			start:  []byte("key5"),
			end:    []byte("zzz"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true,
		},
		{
			name:   "range before table",
			start:  []byte("aaa"),
			end:    []byte("bbb"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   false,
		},
		{
			name:   "range after table",
			start:  []byte("zzz"),
			end:    []byte("zzzz"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   false,
		},
		{
			name:   "range end equals table min",
			start:  []byte("aaa"),
			end:    []byte("key0"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   false, // [start, end) is exclusive of end
		},
		{
			name:   "range start equals table max",
			start:  []byte("key9"),
			end:    []byte("zzz"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true, // table max is inclusive
		},
		{
			name:   "range contains entire table",
			start:  []byte("aaa"),
			end:    []byte("zzz"),
			minKey: []byte("key0"),
			maxKey: []byte("key9"),
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rangeOverlaps(tt.start, tt.end, tt.minKey, tt.maxKey)
			if got != tt.want {
				t.Errorf("rangeOverlaps(%q, %q, %q, %q) = %v, want %v",
					tt.start, tt.end, tt.minKey, tt.maxKey, got, tt.want)
			}
		})
	}
}

func TestScanRangeEdgeCases(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 64 * 1024

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Add some keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%03d", i)
		store.PutInt64([]byte(key), int64(i))
	}
	store.Flush()

	// Test: range with no overlap
	count := 0
	store.ScanRange([]byte("aaa"), []byte("bbb"), func(key []byte, val Value) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("ScanRange(aaa, bbb) got %d keys, want 0", count)
	}

	// Test: range at exact boundaries
	count = 0
	store.ScanRange([]byte("key_050"), []byte("key_055"), func(key []byte, val Value) bool {
		count++
		return true
	})
	if count != 5 {
		t.Errorf("ScanRange(key_050, key_055) got %d keys, want 5", count)
	}

	// Test: range beyond data
	count = 0
	store.ScanRange([]byte("zzz"), []byte("zzzz"), func(key []byte, val Value) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("ScanRange(zzz, zzzz) got %d keys, want 0", count)
	}

	// Test: range covering all keys
	count = 0
	store.ScanRange([]byte("key_000"), []byte("key_999"), func(key []byte, val Value) bool {
		count++
		return true
	})
	if count != 100 {
		t.Errorf("ScanRange(key_000, key_999) got %d keys, want 100", count)
	}

	// Test: early termination
	count = 0
	store.ScanRange([]byte("key_000"), []byte("key_999"), func(key []byte, val Value) bool {
		count++
		return count < 10 // Stop after 10
	})
	if count != 10 {
		t.Errorf("ScanRange with early stop got %d keys, want 10", count)
	}
}

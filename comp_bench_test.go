package tinykvs

import (
	"fmt"
	"testing"
)

func BenchmarkCompressionEncode(b *testing.B) {
	types := []struct {
		name string
		comp CompressionType
	}{
		{"zstd", CompressionZstd},
		{"snappy", CompressionSnappy},
		{"minlz", CompressionMinLZ},
		{"none", CompressionNone},
	}

	for _, tc := range types {
		b.Run(tc.name, func(b *testing.B) {
			builder := newBlockBuilder(16384)
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key%05d", i))
				value := []byte(fmt.Sprintf("value%05d with some extra data to make it compressible and realistic", i))
				builder.Add(key, value)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				builder.BuildWithCompression(blockTypeData, tc.comp, 1)
			}
		})
	}
}

func BenchmarkCompressionDecode(b *testing.B) {
	types := []struct {
		name string
		comp CompressionType
	}{
		{"zstd", CompressionZstd},
		{"snappy", CompressionSnappy},
		{"minlz", CompressionMinLZ},
		{"none", CompressionNone},
	}

	for _, tc := range types {
		b.Run(tc.name, func(b *testing.B) {
			builder := newBlockBuilder(16384)
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key%05d", i))
				value := []byte(fmt.Sprintf("value%05d with some extra data to make it compressible and realistic", i))
				builder.Add(key, value)
			}
			data, _ := builder.BuildWithCompression(blockTypeData, tc.comp, 1)
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				block, _ := DecodeBlock(data, false)
				block.Release()
			}
		})
	}
}

func TestCompressionRatios(t *testing.T) {
	types := []struct {
		name string
		comp CompressionType
	}{
		{"zstd", CompressionZstd},
		{"snappy", CompressionSnappy},
		{"minlz", CompressionMinLZ},
		{"none", CompressionNone},
	}

	builder := newBlockBuilder(16384)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d with some extra data to make it compressible and realistic", i))
		builder.Add(key, value)
	}
	uncompressedSize := builder.Size()

	t.Logf("Uncompressed block size: %d bytes", uncompressedSize)
	for _, tc := range types {
		data, _ := builder.BuildWithCompression(blockTypeData, tc.comp, 1)
		ratio := float64(uncompressedSize) / float64(len(data))
		t.Logf("%s: %d bytes (%.2fx compression)", tc.name, len(data), ratio)
	}
}

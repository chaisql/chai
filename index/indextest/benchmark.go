package indextest

import (
	"fmt"
	"testing"
)

// BenchmarkIndexSet benchmarks the Set method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkIndexSet(b *testing.B, builder Builder) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				idx, cleanup := builder()

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := []byte(fmt.Sprintf("name-%d", j))
					idx.Set(k, k)
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkIndexIteration benchmarks the iterarion of a cursor with 1, 10, 1000 and 10000 items.
func BenchmarkIndexIteration(b *testing.B, builder Builder) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx, cleanup := builder()
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				idx.Set(k, k)
			}

			c := idx.Cursor()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for k, _ := c.First(); k != nil; k, _ = c.Next() {
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkIndexSeek benchmarks the seek method with 1, 10, 1000 and 10000 items.
func BenchmarkIndexSeek(b *testing.B, builder Builder) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx, cleanup := builder()
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				idx.Set(k, k)
			}

			c := idx.Cursor()
			lookup := []byte(fmt.Sprintf("name-%d", size/2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Seek(lookup)
			}
			b.StopTimer()
		})
	}
}

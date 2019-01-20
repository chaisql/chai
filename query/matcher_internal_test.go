package query

import (
	"testing"

	"github.com/asdine/genji/field"
)

func benchmarkIntersection(b *testing.B, size int) {
	set := make([][]byte, size)
	for i := 0; i < size; i++ {
		set[i] = field.EncodeInt64(int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intersection(set, set)
	}
}

func BenchmarkIntersection1(b *testing.B) {
	benchmarkIntersection(b, 1)
}

func BenchmarkIntersection10(b *testing.B) {
	benchmarkIntersection(b, 10)
}

func BenchmarkIntersection100(b *testing.B) {
	benchmarkIntersection(b, 100)
}

func BenchmarkIntersection1000(b *testing.B) {
	benchmarkIntersection(b, 1000)
}

func BenchmarkIntersection10000(b *testing.B) {
	benchmarkIntersection(b, 10000)
}

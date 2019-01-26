package query

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/google/btree"
)

func benchmarkIntersection(b *testing.B, size int) {
	set := btree.New(3)
	for i := 0; i < size; i++ {
		set.ReplaceOrInsert(Item(field.EncodeInt64(int64(i))))
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

func benchmarkUnion(b *testing.B, size int) {
	set := btree.New(3)
	for i := 0; i < size; i++ {
		set.ReplaceOrInsert(Item(field.EncodeInt64(int64(i))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		union(set, set)
	}
}

func BenchmarkUnion1(b *testing.B) {
	benchmarkUnion(b, 1)
}

func BenchmarkUnion10(b *testing.B) {
	benchmarkUnion(b, 10)
}

func BenchmarkUnion100(b *testing.B) {
	benchmarkUnion(b, 100)
}

func BenchmarkUnion1000(b *testing.B) {
	benchmarkUnion(b, 1000)
}

func BenchmarkUnion10000(b *testing.B) {
	benchmarkUnion(b, 10000)
}

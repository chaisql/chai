package query

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/google/btree"
)

func BenchmarkIntersection(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			set := btree.New(3)
			for i := 0; i < size; i++ {
				set.ReplaceOrInsert(Item(field.EncodeInt64(int64(i))))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				intersection(set, set)
			}
		})
	}
}

func BenchmarkUnion(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			set1 := btree.New(3)
			set2 := btree.New(3)

			for i := 0; i < size; i++ {
				set1.ReplaceOrInsert(Item(field.EncodeInt64(int64(i))))
				set2.ReplaceOrInsert(Item(field.EncodeInt64(int64(i))))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				union(set1, set2)
			}
		})
	}
}

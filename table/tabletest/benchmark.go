package tabletest

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B, builder Builder) {
	for i := 1; i <= 10000; i *= 10 {
		b.Run(fmt.Sprintf("%.05d", i), func(b *testing.B) {
			benchmarkTableInsert(b, builder, i)
		})
	}
}

func benchmarkTableInsert(b *testing.B, builder Builder, size int) {
	var fields []field.Field

	for i := int64(0); i < 10; i++ {
		fields = append(fields, field.NewInt64(fmt.Sprintf("name-%d", i), i))
	}

	rec := record.FieldBuffer(fields)

	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		tb, cleanup := builder()

		b.StartTimer()
		for j := 0; j < size; j++ {
			tb.Insert(rec)
		}
		b.StopTimer()
		cleanup()
	}
}

// BenchmarkTableScan benchmarks the Scan method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableScan(b *testing.B, builder Builder) {
	for i := 1; i <= 10000; i *= 10 {
		b.Run(fmt.Sprintf("%.05d", i), func(b *testing.B) {
			benchmarkTableScan(b, builder, i)
		})
	}
}

func benchmarkTableScan(b *testing.B, builder Builder, size int) {
	tb, cleanup := builder()
	defer cleanup()

	var fields []field.Field

	for i := int64(0); i < 10; i++ {
		fields = append(fields, field.NewInt64(fmt.Sprintf("name-%d", i), i))
	}

	rec := record.FieldBuffer(fields)

	for i := 0; i < size; i++ {
		_, err := tb.Insert(rec)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Iterate(func([]byte, record.Record) bool {
			return true
		})
	}
	b.StopTimer()
}

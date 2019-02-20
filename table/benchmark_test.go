package table_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

func benchmarkRecordBufferIterate(b *testing.B, prefill int) {
	var r table.RecordBuffer

	rec := record.FieldBuffer([]field.Field{
		field.NewInt64("age", 10),
	})

	for j := 0; j < prefill; j++ {
		r.Insert(rec)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Iterate(func([]byte, record.Record) bool {
			return true
		})
	}
}

func BenchmarkRecordBufferIterate1(b *testing.B) {
	benchmarkRecordBufferIterate(b, 1)
}

func BenchmarkRecordBufferIterate10(b *testing.B) {
	benchmarkRecordBufferIterate(b, 10)
}

func BenchmarkRecordBufferIterate100(b *testing.B) {
	benchmarkRecordBufferIterate(b, 100)
}

func BenchmarkRecordBufferIterate1000(b *testing.B) {
	benchmarkRecordBufferIterate(b, 1000)
}

func BenchmarkRecordBufferIterate10000(b *testing.B) {
	benchmarkRecordBufferIterate(b, 10000)
}

func benchmarkRecordBufferRecord(b *testing.B, prefill int) {
	var r table.RecordBuffer

	rec := record.FieldBuffer([]field.Field{
		field.NewInt64("age", 10),
	})

	var rowid []byte
	for j := 0; j < prefill; j++ {
		rid, _ := r.Insert(rec)
		if j == prefill/2 {
			rowid = rid
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Record(rowid)
	}
}

func BenchmarkRecordBufferRecord1(b *testing.B) {
	benchmarkRecordBufferRecord(b, 1)
}

func BenchmarkRecordBufferRecord10(b *testing.B) {
	benchmarkRecordBufferRecord(b, 10)
}

func BenchmarkRecordBufferRecord100(b *testing.B) {
	benchmarkRecordBufferRecord(b, 100)
}

func BenchmarkRecordBufferRecord1000(b *testing.B) {
	benchmarkRecordBufferRecord(b, 1000)
}

func BenchmarkRecordBufferRecord10000(b *testing.B) {
	benchmarkRecordBufferRecord(b, 10000)
}

func benchmarkRecordBufferInsertFrom(b *testing.B, prefill int) {
	var r table.RecordBuffer

	rec := record.FieldBuffer([]field.Field{
		field.NewInt64("age", 10),
	})

	for j := 0; j < prefill; j++ {
		r.Insert(rec)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r2 table.RecordBuffer

		r2.InsertFrom(&r)
	}
}

func BenchmarkRecordBufferInsertFrom1(b *testing.B) {
	benchmarkRecordBufferInsertFrom(b, 1)
}

func BenchmarkRecordBufferInsertFrom10(b *testing.B) {
	benchmarkRecordBufferInsertFrom(b, 10)
}

func BenchmarkRecordBufferInsertFrom100(b *testing.B) {
	benchmarkRecordBufferInsertFrom(b, 100)
}

func BenchmarkRecordBufferInsertFrom1000(b *testing.B) {
	benchmarkRecordBufferInsertFrom(b, 1000)
}

func BenchmarkRecordBufferInsertFrom10000(b *testing.B) {
	benchmarkRecordBufferInsertFrom(b, 10000)
}

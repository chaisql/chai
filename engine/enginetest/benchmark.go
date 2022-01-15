package enginetest

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/testutil/assert"
)

var benchmarkEngine engine.Engine

// BenchmarkStorePut benchmarks the Put method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkStorePut(b *testing.B, builder Builder) {
	v := bytes.Repeat([]byte("v"), 512)

	ng := benchmarkEngine
	if ng == nil {
		ng, _ = builder()
	}

	getStore := func() (engine.Store, func()) {
		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		assert.NoError(b, err)
		err = tx.CreateStore([]byte("test"))
		assert.NoError(b, err)
		s, err := tx.GetStore([]byte("test"))
		assert.NoError(b, err)

		return s, func() {
			tx.Rollback()
		}
	}

	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s, cleanup := getStore()

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := []byte(fmt.Sprintf("k%d", j))
					_ = s.Put(k, v)
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

var benchmarkTransientEngine engine.TransientStore

// BenchmarkTransientStorePut benchmarks the Put method of a transient store with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTransientStorePut(b *testing.B, builder Builder) {
	v := bytes.Repeat([]byte("v"), 512)

	ts := benchmarkTransientEngine
	if ts == nil {
		e, _ := builder()

		var err error
		ts, err = e.NewTransientStore(context.Background())
		assert.NoError(b, err)
		benchmarkTransientEngine = ts
	}

	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				for j := 0; j < size; j++ {
					k := []byte(fmt.Sprintf("k%d", j))
					_ = ts.Put(k, v)
				}
				b.StopTimer()

				err := ts.Reset()
				assert.NoError(b, err)
			}
		})
	}
}

// BenchmarkStoreScan benchmarks the AscendGreaterOrEqual method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkStoreScan(b *testing.B, builder Builder) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			st, cleanup := storeBuilder(b, builder)
			defer cleanup()

			v := bytes.Repeat([]byte("v"), 512)

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("k%d", i))
				err := st.Put(k, v)
				assert.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it := st.Iterator(engine.IteratorOptions{})
				for it.Seek(nil); it.Valid(); it.Next() {
				}
				if err := it.Err(); err != nil {
					assert.NoError(b, err)
				}
				if err := it.Close(); err != nil {
					assert.NoError(b, err)
				}
			}
			b.StopTimer()
		})
	}
}

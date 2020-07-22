package enginetest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/genjidb/genji/engine"
	"github.com/stretchr/testify/require"
)

// BenchmarkStorePut benchmarks the Put method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkStorePut(b *testing.B, builder Builder) {
	v := bytes.Repeat([]byte("v"), 512)

	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				st, cleanup := storeBuilder(b, builder)

				for j := 0; j < size; j++ {
					k := []byte(fmt.Sprintf("k%d", j))

					st.Put(k, v)
				}
				cleanup()
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
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it := st.NewIterator(engine.IteratorConfig{})
				for it.Seek(nil); it.Valid(); it.Next() {
				}
			}
			b.StopTimer()
		})
	}
}

package column

import (
	"testing"
)

func BenchmarkColumnAddScalar(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)

	c := NewInt64Column()
	for _, v := range a {
		c.AppendInt64(v)
	}

	dest := NewInt64Column()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AddScalarTo(dest, 1)
	}
}

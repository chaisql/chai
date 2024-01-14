package column

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func makeIntSlice[T constraints.Integer](n int) []T {
	s := make([]T, n)
	for i := 0; i < n; i++ {
		s[i] = T(i)
	}

	return s
}

func TestInt64AddConstant(t *testing.T) {
	from := makeIntSlice[int64](25)
	want := make([]int64, 25)
	got := make([]int64, 25)
	AddConstant(want, from, 1)
	Int64AddConstant(got, from, 1)
	require.Equal(t, want, got)
}

func TestInt64SubConstant(t *testing.T) {
	from := makeIntSlice[int64](25)
	want := make([]int64, 25)
	SubConstant(want, from, 1)
	Int64SubConstant(from, from, 1)
	require.Equal(t, want, from)
}

func TestInt64MulConstant(t *testing.T) {
	from := makeIntSlice[int64](25)
	want := make([]int64, 25)
	MulConstant(want, from, 2)
	Int64MulConstant(from, from, 2)
	require.Equal(t, want, from)
}

func TestInt64DivConstant(t *testing.T) {
	from := makeIntSlice[int64](25)
	want := make([]int64, 25)
	DivConstant(want, from, 2)
	Int64DivConstant(from, from, 2)
	require.Equal(t, want, from)
}

func TestInt64ModConstant(t *testing.T) {
	from := makeIntSlice[int64](25)
	want := make([]int64, 25)
	ModConstant(want, from, 2)
	Int64ModConstant(from, from, 2)
	require.Equal(t, want, from)
}

func BenchmarkAddConstant(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int64AddConstant(to, a, 1)
	}
}

func BenchmarkAddConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AddConstant(to, a, 1)
	}
}

func BenchmarkSubConstant(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int64SubConstant(to, a, 1)
	}
}

func BenchmarkSubConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SubConstant(to, a, 1)
	}
}

func BenchmarkMulConstant(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int64MulConstant(to, a, 2)
	}
}

func BenchmarkMulConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)

	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MulConstant(to, a, 2)
	}
}

func BenchmarkDivConstant(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int64DivConstant(to, a, 2)
	}
}

func BenchmarkDivConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DivConstant(to, a, 2)
	}
}

func BenchmarkModConstant(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int64ModConstant(to, a, 2)
	}
}

func BenchmarkModConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int64](blockSize / 8)
	to := make([]int64, blockSize/8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ModConstant(to, a, 2)
	}
}

func TestInt32AddConstant(t *testing.T) {
	from := makeIntSlice[int32](25)
	want := make([]int32, 25)
	got := make([]int32, 25)
	AddConstant(want, from, 1)
	Int32AddConstant(got, from, 1)
	require.Equal(t, want, got)
}

func TestInt32SubConstant(t *testing.T) {
	from := makeIntSlice[int32](25)
	want := make([]int32, 25)
	SubConstant(want, from, 1)
	Int32SubConstant(from, from, 1)
	require.Equal(t, want, from)
}

func TestInt32MulConstant(t *testing.T) {
	from := makeIntSlice[int32](25)
	want := make([]int32, 25)
	MulConstant(want, from, 2)
	Int32MulConstant(from, from, 2)
	require.Equal(t, want, from)
}

func TestInt32DivConstant(t *testing.T) {
	from := makeIntSlice[int32](25)
	want := make([]int32, 25)
	DivConstant(want, from, 2)
	Int32DivConstant(from, from, 2)
	require.Equal(t, want, from)
}

func TestInt32ModConstant(t *testing.T) {
	from := makeIntSlice[int32](25)
	want := make([]int32, 25)
	ModConstant(want, from, 2)
	Int32ModConstant(from, from, 2)
	require.Equal(t, want, from)
}

func BenchmarkInt32AddConstant(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int32AddConstant(to, a, 1)
	}
}

func BenchmarkInt32AddConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AddConstant(to, a, 1)
	}
}

func BenchmarkInt32SubConstant(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int32SubConstant(to, a, 1)
	}
}

func BenchmarkInt32SubConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SubConstant(to, a, 1)
	}
}

func BenchmarkInt32MulConstant(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int32MulConstant(to, a, 2)
	}
}

func BenchmarkInt32MulConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MulConstant(to, a, 2)
	}
}

func BenchmarkInt32DivConstant(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int32DivConstant(to, a, 2)
	}
}

func BenchmarkInt32DivConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DivConstant(to, a, 2)
	}
}

func BenchmarkInt32ModConstant(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Int32ModConstant(to, a, 2)
	}
}

func BenchmarkInt32ModConstantNoSIMD(b *testing.B) {
	a := makeIntSlice[int32](blockSize / 4)
	to := make([]int32, blockSize/4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ModConstant(to, a, 2)
	}
}

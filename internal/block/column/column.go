package column

import "github.com/chaisql/chai/internal/types"

const blockSize = 64 * 1024 // 64KB

type Column interface {
	// Append appends a value to the column.
	Append(v types.Value)
}

type Int64Column struct {
	// data is the underlying data for the column.
	data []int64
	len  int
}

func NewInt64Column() *Int64Column {
	return &Int64Column{
		data: make([]int64, blockSize/8),
	}
}

func (c *Int64Column) Append(v types.Value) {
	c.AppendInt64(types.As[int64](v))
}

func (c *Int64Column) AppendInt64(v int64) {
	c.data[c.len] = v
	c.len++
}

func (c *Int64Column) Len() int {
	return c.len
}

func (c *Int64Column) Data() []int64 {
	return c.data[:]
}

func (c *Int64Column) Reset() {
	c.len = 0
}

func (c *Int64Column) AddScalarTo(dest *Int64Column, v int64) {
	dest.len = c.len

	Int64AddConstant(dest.data, c.data[:c.len], v)
}

func (c *Int64Column) SubScalarTo(dest *Int64Column, v int64) {
	dest.len = c.len

	Int64SubConstant(dest.data, c.data[:c.len], v)
}

func (c *Int64Column) MulScalarTo(dest *Int64Column, v int64) {
	dest.len = c.len

	Int64MulConstant(dest.data, c.data[:c.len], v)
}

func (c *Int64Column) DivScalarTo(dest *Int64Column, v int64) {
	dest.len = c.len

	Int64DivConstant(dest.data, c.data[:c.len], v)
}

func (c *Int64Column) ModScalarTo(dest *Int64Column, v int64) {
	dest.len = c.len

	Int64ModConstant(dest.data, c.data[:c.len], v)
}

package block

import (
	"github.com/chaisql/chai/internal/block/column"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/types"
)

type Block interface {
	// Len returns the number of rows in the block.
	Len() int

	// Reset resets the block to its initial state.
	Reset()

	// Add performs an addition operation on the given column
	Add(column string, v types.Value) error

	// Sub performs a subtraction operation on the block
	Sub(column string, v types.Value) error

	// Mul performs a multiplication operation on the block
	Mul(column string, v types.Value) error

	// Div performs a division operation on the block
	Div(column string, v types.Value) error

	// Mod performs a modulo operation on the block
	Mod(column string, v types.Value) error

	Filter(e expr.Expr) error
}

type KVBlock struct {
	projected  []string
	vectorized []column.Column
}

func NewKVBlock(
	projectedColumns []string,
	vectorizedColumns []string,
) *KVBlock {
	return &KVBlock{
		projected: projectedColumns,
	}
}

package rows

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
)

// A SkipOperator skips the n first values of the stream.
type SkipOperator struct {
	stream.BaseOperator
	E expr.Expr
}

// Skip ignores the first n values of the stream.
func Skip(e expr.Expr) *SkipOperator {
	return &SkipOperator{E: e}
}

func (op *SkipOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	v, err := op.E.Eval(in)
	if err != nil {
		return nil, err
	}

	if !v.Type().IsNumber() {
		return nil, fmt.Errorf("offset expression must evaluate to a number, got %q", v.Type())
	}

	v, err = v.CastAs(types.TypeBigint)
	if err != nil {
		return nil, err
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &SkipIterator{
		Iterator: prev,
		n:        types.AsInt64(v),
	}, nil
}

type SkipIterator struct {
	stream.Iterator

	skipped int64
	n       int64
}

func (it *SkipIterator) Next() bool {
	if it.skipped < it.n {
		for it.Iterator.Next() {
			if it.skipped < it.n {
				it.skipped++
				continue
			}

			return true
		}
	}

	return it.Iterator.Next()
}

func (op *SkipOperator) String() string {
	return fmt.Sprintf("rows.Skip(%s)", op.E)
}

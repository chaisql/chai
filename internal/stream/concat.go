package stream

import (
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

// A ConcatOperator concatenates two streams.
type ConcatOperator struct {
	baseOperator
	s1 *Stream
	s2 *Stream
}

// Concat turns two individual streams into one.
func Concat(s1 *Stream, s2 *Stream) *ConcatOperator {
	return &ConcatOperator{s1: s1, s2: s2}
}

func (op *ConcatOperator) Iterate(in *expr.Environment, fn func(*expr.Environment) error) error {
	err := op.s1.Iterate(in, func(out *expr.Environment) error {
		fn(out)
		return nil
	})
	if err != nil {
		return err
	}

	return op.s2.Iterate(in, func(out *expr.Environment) error {
		fn(out)
		return nil
	})
}

func (op *ConcatOperator) String() string {
	return stringutil.Sprintf("concat(%s, %s)", op.s1, op.s2)
}

package stream

import (
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stringutil"
)

// A ConcatOperator concatenates two streams.
type ConcatOperator struct {
	baseOperator
	S1 *Stream
	S2 *Stream
}

// Concat turns two individual streams into one.
func Concat(s1 *Stream, s2 *Stream) *ConcatOperator {
	return &ConcatOperator{S1: s1, S2: s2}
}

func (op *ConcatOperator) Iterate(in *environment.Environment, fn func(*environment.Environment) error) error {
	err := op.S1.Iterate(in, fn)
	if err != nil {
		return err
	}

	return op.S2.Iterate(in, fn)
}

func (op *ConcatOperator) String() string {
	return stringutil.Sprintf("concat(%s, %s)", op.S1, op.S2)
}

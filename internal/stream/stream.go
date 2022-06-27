package stream

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

type Stream struct {
	Op Operator
}

func New(op Operator) *Stream {
	return &Stream{Op: op}
}

func (s *Stream) Pipe(op Operator) *Stream {
	if s == nil || s.Op == nil {
		return New(op)
	}
	s.Op = Pipe(s.Op, op)
	return s
}

func (s *Stream) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	if s.Op == nil {
		return nil
	}

	return s.Op.Iterate(in, fn)
}

func (s *Stream) Remove(op Operator) {
	if op == nil {
		return
	}

	next := op.GetNext()
	prev := op.GetPrev()
	if prev != nil {
		prev.SetNext(next)
	}
	if next != nil {
		next.SetPrev(prev)
	}
	op.SetNext(nil) // avoid memory leaks
	op.SetPrev(nil) // avoid memory leaks

	if op == s.Op {
		s.Op = prev
	}
}

func (s *Stream) First() Operator {
	n := s.Op

	for n != nil && n.GetPrev() != nil {
		n = n.GetPrev()
	}

	return n
}

func (s *Stream) String() string {
	if s.Op == nil {
		return ""
	}

	var sb strings.Builder

	for op := s.First(); op != nil; op = op.GetNext() {
		if sb.Len() != 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(op.String())
	}

	return sb.String()
}

func InsertBefore(op, newOp Operator) Operator {
	if op != nil {
		prev := op.GetPrev()
		if prev != nil {
			prev.SetNext(newOp)
			newOp.SetPrev(prev)
		}

		op.SetPrev(newOp)
		newOp.SetNext(op)
	}

	return newOp
}

func InsertAfter(op, newOp Operator) Operator {
	if op == nil {
		return newOp
	}

	next := op.GetNext()
	if next != nil {
		next.SetPrev(newOp)
	}
	op.SetNext(newOp)
	newOp.SetNext(next)
	newOp.SetPrev(op)

	return newOp
}

// DiscardOperator is an operator that doesn't do anything.
type DiscardOperator struct {
	BaseOperator
}

// Discard is an operator that doesn't produce any document.
// It iterates over the previous operator and discards all the documents.
func Discard() *DiscardOperator {
	return &DiscardOperator{}
}

// Iterate iterates over all the streams and returns their union.
func (op *DiscardOperator) Iterate(in *environment.Environment, _ func(out *environment.Environment) error) (err error) {
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		return nil
	})
}

func (it *DiscardOperator) String() string {
	return "discard()"
}

package stream

import (
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/cockroachdb/errors"
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

func (s *Stream) Columns(env *environment.Environment) ([]string, error) {
	if s.Op == nil {
		return nil, nil
	}

	return s.Op.Columns(env)
}

func (s *Stream) Iterator(in *environment.Environment) (Iterator, error) {
	if s.Op == nil {
		return nil, nil
	}

	return s.Op.Iterator(in)
}

func (s *Stream) Iterate(in *environment.Environment, fn func(database.Row) error) error {
	if s.Op == nil {
		return nil
	}

	it, err := s.Op.Iterator(in)
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		row, err := it.Row()
		if err != nil {
			return err
		}
		if err := fn(row); err != nil {
			return err
		}
	}

	return it.Error()
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

func (s *Stream) Clone() *Stream {
	if s == nil {
		return nil
	}

	if s.Op == nil {
		return New(nil)
	}

	op := s.First()
	var ops []Operator
	for op != nil {
		ops = append(ops, op.Clone())
		op = op.GetNext()
	}

	return New(Pipe(ops...))
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

// Discard is an operator that doesn't produce any row.
// It iterates over the previous operator and discards all the objects.
func Discard() *DiscardOperator {
	return &DiscardOperator{}
}

func (it *DiscardOperator) Clone() Operator {
	return &DiscardOperator{
		BaseOperator: it.BaseOperator.Clone(),
	}
}

// Iterator returns an iterator which discards all rows.
func (op *DiscardOperator) Iterator(in *environment.Environment) (Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &DiscardIterator{
		Iterator: prev,
	}, nil
}

func (it *DiscardOperator) String() string {
	return "discard()"
}

type DiscardIterator struct {
	Iterator
}

func (it *DiscardIterator) Next() bool {
	for it.Iterator.Next() {
	}

	return false
}

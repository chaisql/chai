package stream

import (
	"errors"
	"strings"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
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
	s.Op = Pipe(s.Op, op)
	return s
}

func (s *Stream) Remove(op Operator) {
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
		s.Op = nil
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
	var sb strings.Builder

	for op := s.First(); op != nil; op = op.GetNext() {
		if sb.Len() != 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(op.String())
	}

	return sb.String()
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

// Statement is a query.Statement using a Stream.
type Statement struct {
	Stream   *Stream
	ReadOnly bool
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s *Statement) Run(tx *database.Transaction, params []expr.Param) (query.Result, error) {
	env := expr.Environment{
		Tx:     tx,
		Params: params,
	}

	return query.Result{
		Iterator: document.IteratorFunc(func(fn func(d document.Document) error) error {
			err := s.Stream.Op.Iterate(&env, func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				if !ok {
					return nil
				}

				return fn(d)
			})
			if err == ErrStreamClosed {
				err = nil
			}
			return err
		}),
	}, nil
}

// IsReadOnly reports whether the stream will modify the database or only read it.
func (s *Statement) IsReadOnly() bool {
	return s.ReadOnly
}

func (s *Statement) String() string {
	return s.Stream.String()
}

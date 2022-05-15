package stream

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
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

// UnionOperator is an operator that merges the results of multiple operators.
type UnionOperator struct {
	baseOperator
	Streams []*Stream
}

// Union returns a new UnionOperator.
func Union(s ...*Stream) *UnionOperator {
	return &UnionOperator{Streams: s}
}

// Iterate iterates over all the streams and returns their union.
func (it *UnionOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) (err error) {
	var temp *tree.Tree
	var cleanup func() error

	defer func() {
		if cleanup != nil {
			e := cleanup()
			if err != nil {
				err = e
			}
		}
	}()

	// iterate over all the streams and insert each key in the temporary table
	// to deduplicate them
	for _, s := range it.Streams {
		err := s.Iterate(in, func(out *environment.Environment) error {
			doc, ok := out.GetDocument()
			if !ok {
				return errors.New("missing document")
			}

			if temp == nil {
				// create a temporary database
				db := in.GetDB()

				tr, f, err := database.NewTransientTree(db)
				if err != nil {
					return err
				}
				temp = tr
				cleanup = f
			}

			key, err := tree.NewKey(types.NewDocumentValue(doc))
			if err != nil {
				return err
			}
			err = temp.Put(key, nil)
			if err == nil || errors.Is(err, database.ErrIndexDuplicateValue) {
				return nil
			}
			return err
		})
		if err != nil {
			return err
		}
	}

	if temp == nil {
		// the union is empty
		return nil
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	// iterate over the temporary index
	return temp.IterateOnRange(nil, false, func(key tree.Key, _ []byte) error {
		kv, err := key.Decode()
		if err != nil {
			return err
		}

		doc := kv[0].V().(types.Document)

		newEnv.SetDocument(doc)
		return fn(&newEnv)
	})
}

func (it *UnionOperator) String() string {
	var s strings.Builder

	s.WriteString("union(")
	for i, st := range it.Streams {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(st.String())
	}
	s.WriteRune(')')

	return s.String()
}

// A ConcatOperator concatenates two streams.
type ConcatOperator struct {
	baseOperator
	Streams []*Stream
}

// Concat turns two individual streams into one.
func Concat(s ...*Stream) *ConcatOperator {
	return &ConcatOperator{Streams: s}
}

func (it *ConcatOperator) Iterate(in *environment.Environment, fn func(*environment.Environment) error) error {
	for _, s := range it.Streams {
		if err := s.Iterate(in, fn); err != nil {
			return err
		}
	}

	return nil
}

func (it *ConcatOperator) String() string {
	var s strings.Builder

	s.WriteString("concat(")
	for i, st := range it.Streams {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(st.String())
	}
	s.WriteRune(')')

	return s.String()
}

// OnConflictOperator handles any conflicts that occur during the iteration.
type OnConflictOperator struct {
	baseOperator

	OnConflict *Stream
}

func OnConflict(onConflict *Stream) *OnConflictOperator {
	return &OnConflictOperator{
		OnConflict: onConflict,
	}
}

func (op *OnConflictOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		err := fn(out)
		if err != nil {
			if cerr, ok := err.(*errs.ConstraintViolationError); ok {
				if op.OnConflict == nil {
					return nil
				}

				newEnv.SetOuter(out)
				newEnv.Set(environment.DocPKKey, types.NewBlobValue(cerr.Key))

				err = op.OnConflict.Iterate(&newEnv, func(out *environment.Environment) error { return nil })
			}
		}
		return err
	})
}

func (op *OnConflictOperator) String() string {
	if op.OnConflict == nil {
		return fmt.Sprintf("stream.OnConflict(NULL)")
	}

	return fmt.Sprintf("stream.OnConflict(%s)", op.OnConflict)
}

package stream

import (
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
)

// A ConcatOperator concatenates two streams.
type ConcatOperator struct {
	BaseOperator
	Streams []*Stream
}

// Concat turns two individual streams into one.
func Concat(s ...*Stream) *ConcatOperator {
	return &ConcatOperator{Streams: s}
}

func (it *ConcatOperator) Clone() Operator {
	streams := make([]*Stream, len(it.Streams))
	for i, s := range it.Streams {
		streams[i] = s.Clone()
	}

	return &ConcatOperator{
		BaseOperator: it.BaseOperator.Clone(),
		Streams:      streams,
	}
}

func (it *ConcatOperator) Columns(env *environment.Environment) ([]string, error) {
	if len(it.Streams) == 0 {
		return nil, nil
	}

	return it.Streams[0].Columns(env)
}

func (it *ConcatOperator) Iterator(in *environment.Environment) (Iterator, error) {
	return &ConcatIterator{
		streams: it.Streams,
		env:     in,
	}, nil
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

type ConcatIterator struct {
	streams []*Stream
	index   int
	env     *environment.Environment
	current Iterator
	err     error
}

func (it *ConcatIterator) Close() error {
	if it.current != nil {
		return it.current.Close()
	}
	return nil
}

func (it *ConcatIterator) Next() bool {
	it.err = nil

	if len(it.streams) == 0 {
		return false
	}

	if it.current == nil {
		it.current, it.err = it.streams[it.index].Op.Iterator(it.env)
		if it.err != nil {
			return false
		}
	}

	for !it.current.Next() {
		if err := it.current.Error(); err != nil {
			it.err = err
			return false
		}
		it.err = it.current.Close()
		if it.err != nil {
			return false
		}

		it.index++
		if it.index >= len(it.streams) {
			return false
		}
		it.current, it.err = it.streams[it.index].Op.Iterator(it.env)
		if it.err != nil {
			return false
		}
	}

	return true
}

func (it *ConcatIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	if it.current != nil {
		return it.current.Error()
	}

	return nil
}

func (it *ConcatIterator) Row() (database.Row, error) {
	if it.current == nil {
		return nil, nil
	}

	return it.current.Row()
}

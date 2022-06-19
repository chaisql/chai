package stream

import (
	"strings"

	"github.com/genjidb/genji/internal/environment"
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

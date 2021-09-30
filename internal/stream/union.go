package stream

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
)

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
	var temp *database.TempResources
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

				tmp, f, err := database.NewTransientIndex(db, "union",
					// passing a single path with nothing inside for setting the arity
					// TODO(asdine): Is the path really useful when creating an index?
					[]document.Path{{}}, true)
				if err != nil {
					return err
				}
				temp = tmp
				cleanup = f
			}

			err := temp.Index.Set([]types.Value{types.NewDocumentValue(doc)}, []byte{0})
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
	return temp.Index.AscendGreaterOrEqual(nil, func(val, _ []byte) error {
		a, _, err := encoding.DecodeArray(val)
		if err != nil {
			return err
		}
		v, err := a.GetByIndex(0)
		if err != nil {
			return err
		}
		doc := v.V().(types.Document)

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

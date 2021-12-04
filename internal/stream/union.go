package stream

import (
	"strings"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
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
	return temp.Iterate(nil, false, func(key tree.Key, _ types.Value) error {
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

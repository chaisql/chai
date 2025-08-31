package stream

import (
	"errors"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
)

// UnionOperator is an operator that merges the results of multiple operators.
type UnionOperator struct {
	BaseOperator
	Streams []*Stream
}

// Union returns a new UnionOperator.
func Union(s ...*Stream) *UnionOperator {
	return &UnionOperator{Streams: s}
}

func (it *UnionOperator) Columns(env *environment.Environment) ([]string, error) {
	if len(it.Streams) == 0 {
		return nil, nil
	}

	return it.Streams[0].Columns(env)
}

// Iterate iterates over all the streams and returns their union.
func (op *UnionOperator) Iterator(in *environment.Environment) (Iterator, error) {
	return &UnionIterator{
		streams: op.Streams,
		env:     in,
	}, nil
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

type UnionIterator struct {
	streams []*Stream
	err     error
	env     *environment.Environment
	temp    *tree.Tree
	tempIt  *tree.Iterator
	cleanup func() error
}

func (it *UnionIterator) Close() error {
	var errs []error
	if it.tempIt != nil {
		err := it.tempIt.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if it.cleanup != nil {
		err := it.cleanup()
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (it *UnionIterator) Next() bool {
	it.err = nil

	if it.tempIt != nil {
		return it.tempIt.Next()
	}

	// create a temporary tree
	db := it.env.GetDB()
	tns := it.env.GetTx().Catalog.GetFreeTransientNamespace()
	it.temp, it.cleanup, it.err = tree.NewTransient(db.Engine.NewTransientSession(), tns, 0)
	if it.err != nil {
		return false
	}

	// iterate over all the steams and add them to the temp tree
	for _, s := range it.streams {
		if err := it.iterateOnStream(s); err != nil {
			it.err = err
			return false
		}
	}

	it.tempIt, it.err = it.temp.Iterator(nil)
	if it.err != nil {
		return false
	}

	return it.tempIt.Start(false)
}

func (it *UnionIterator) iterateOnStream(s *Stream) error {
	sit, err := s.Iterator(it.env)
	if err != nil {
		return err
	}
	defer sit.Close()

	var buf []byte

	for sit.Next() {
		buf = buf[:0]
		var tableName string
		var encKey []byte

		r, err := sit.Row()
		if err != nil {
			return err
		}

		if r.TableName() != "" {
			// encode the row key and table name as the value
			tableName = r.TableName()

			info, err := it.env.GetTx().Catalog.GetTableInfo(tableName)
			if err != nil {
				return err
			}

			k := r.Key()
			if k == nil {
				if br, ok := r.(*database.BasicRow); ok {
					k = br.OriginalRow().Key()
				}

				if k == nil {
					return errors.New("missing row key")
				}
			}

			encKey, err = info.EncodeKey(k)
			if err != nil {
				return err
			}
		}

		key := tree.NewKey(row.Flatten(r)...)
		buf, err = types.EncodeValuesAsKey(buf, types.NewBlobValue(encKey), types.NewTextValue(tableName))
		if err != nil {
			return err
		}

		err = it.temp.Put(key, buf)
		if err == nil || errors.Is(err, database.ErrIndexDuplicateValue) {
			continue
		}
		return err
	}

	if err := sit.Error(); err != nil {
		return err
	}

	return nil
}

func (it *UnionIterator) Error() error {
	return it.err
}

func (it *UnionIterator) Row() (database.Row, error) {
	var basicRow database.BasicRow

	key := it.tempIt.Key()
	value, err := it.tempIt.Value()
	if err != nil {
		return nil, err
	}

	kv, err := key.Decode()
	if err != nil {
		return nil, err
	}

	var tableName string
	var pk *tree.Key

	obj := row.Unflatten(kv)

	if len(value) > 1 {
		ser := types.DecodeValues(value)
		pk = tree.NewEncodedKey(types.AsByteSlice(ser[0]))
		tableName = types.AsString(ser[1])
	}

	basicRow.ResetWith(tableName, pk, obj)

	return &basicRow, nil
}

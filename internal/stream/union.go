package stream

import (
	"errors"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/object"
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
	fb := object.NewFieldBuffer()
	var buf []byte

	for _, s := range it.Streams {
		err := s.Iterate(in, func(out *environment.Environment) error {
			fb.Reset()
			buf = buf[:0]

			row, ok := out.GetRow()
			if !ok {
				return errors.New("missing row")
			}

			if temp == nil {
				// create a temporary tree
				db := in.GetDB()
				tns := in.GetTx().Catalog.GetFreeTransientNamespace()
				temp, cleanup, err = tree.NewTransient(db.Engine.NewTransientSession(), tns, 0)
				if err != nil {
					return err
				}
			}

			key := tree.NewKey(types.NewObjectValue(row.Object()))

			if row.Key() != nil {
				// encode the row key and table name as the value
				info, err := in.GetTx().Catalog.GetTableInfo(row.TableName())
				if err != nil {
					return err
				}
				encKey, err := info.EncodeKey(row.Key())
				if err != nil {
					return err
				}

				fb.Add("key", types.NewBlobValue(encKey))
				fb.Add("table", types.NewTextValue(row.TableName()))
				buf, err = encoding.EncodeObject(buf, fb)
				if err != nil {
					return err
				}
			}

			err = temp.Put(key, buf)
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

	var vb object.ValueBuffer
	var basicRow database.BasicRow
	// iterate over the temporary index
	return temp.IterateOnRange(nil, false, func(key *tree.Key, value []byte) error {
		vb.Reset()
		kv, err := key.Decode()
		if err != nil {
			return err
		}

		var tableName string
		var pk *tree.Key

		obj := types.AsObject(kv[0])

		if len(value) > 1 {
			ser := encoding.DecodeObject(value, false)
			pkf, err := ser.GetByField("key")
			if err != nil {
				return err
			}
			pk = tree.NewEncodedKey(types.AsByteSlice(pkf))
			tf, err := ser.GetByField("table")
			if err != nil {
				return err
			}
			tableName = types.AsString(tf)
		}

		basicRow.ResetWith(tableName, pk, obj)

		newEnv.SetRow(&basicRow)
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

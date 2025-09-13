package rows

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// A TempTreeSortOperator consumes every value of the stream and outputs them in order.
type TempTreeSortOperator struct {
	stream.BaseOperator
	Expr expr.Expr
	Desc bool
}

// TempTreeSort consumes every value of the stream, sorts them by the given expr and outputs them in order.
// It creates a temporary index and uses it to sort the stream.
func TempTreeSort(e expr.Expr) *TempTreeSortOperator {
	return &TempTreeSortOperator{Expr: e}
}

// TempTreeSortReverse does the same as TempTreeSort but in descending order.
func TempTreeSortReverse(e expr.Expr) *TempTreeSortOperator {
	return &TempTreeSortOperator{Expr: e, Desc: true}
}

func (op *TempTreeSortOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &TempTreeSortIterator{
		prev: prev,
		expr: op.Expr,
		desc: op.Desc,
		env:  in,
	}, nil
}

func (op *TempTreeSortOperator) String() string {
	if op.Desc {
		return fmt.Sprintf("rows.TempTreeSortReverse(%s)", op.Expr)
	}

	return fmt.Sprintf("rows.TempTreeSort(%s)", op.Expr)
}

func encodeTempRow(buf []byte, r row.Row) ([]byte, error) {
	// encode each column directly into buf: column name, type, value
	err := r.Iterate(func(column string, v types.Value) error {
		// encode column name as text
		var e error
		buf, e = types.NewTextValue(column).EncodeAsKey(buf)
		if e != nil {
			return e
		}

		// encode the type as an integer value
		buf, e = types.NewIntegerValue(int32(v.Type())).EncodeAsKey(buf)
		if e != nil {
			return e
		}

		// encode the value itself
		buf, e = v.EncodeAsKey(buf)
		if e != nil {
			return e
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to iterate row")
	}

	return buf, nil
}

func decodeTempRow(b []byte) row.Row {
	cb := row.NewColumnBuffer()

	for len(b) > 0 {
		colv, n := types.DecodeValue(b)
		b = b[n:]
		typev, n := types.DecodeValue(b)
		b = b[n:]
		v, n := types.Type(types.AsInt32(typev)).Def().Decode(b)
		cb.Add(types.AsString(colv), v)
		b = b[n:]
	}

	return cb
}

type TempTreeSortIterator struct {
	prev    stream.Iterator
	expr    expr.Expr
	desc    bool
	env     *environment.Environment
	err     error
	temp    *tree.Tree
	tempIt  *tree.Iterator
	cleanup func() error
}

func (it *TempTreeSortIterator) Close() error {
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

	err := it.prev.Close()
	if err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (it *TempTreeSortIterator) Next() bool {
	it.err = nil

	if it.tempIt != nil {
		return it.tempIt.Move(it.desc)
	}

	// create a temporary tree
	db := it.env.GetDB()
	tns := it.env.GetTx().Catalog.GetFreeTransientNamespace()
	it.temp, it.cleanup, it.err = tree.NewTransient(db.Engine.NewTransientSession(), tns, 0)
	if it.err != nil {
		return false
	}

	// iterate over the steam and add it to the temp tree
	if err := it.iterateOnStream(); err != nil {
		it.err = err
		return false
	}

	it.tempIt, it.err = it.temp.Iterator(nil)
	if it.err != nil {
		return false
	}

	return it.tempIt.Start(it.desc)
}

func (it *TempTreeSortIterator) iterateOnStream() error {
	var buf []byte
	var counter int64

	for it.prev.Next() {
		buf = buf[:0]

		r, err := it.prev.Row()
		if err != nil {
			return err
		}

		// evaluate the sort expression
		v, err := it.expr.Eval(it.env.Clone(r))
		if err != nil {
			if !errors.Is(err, types.ErrColumnNotFound) {
				return err
			}

			v = nil
		}

		if v == nil {
			// the expression might be pointing to the original row.
			dr, ok := r.(*database.BasicRow)
			if ok {
				v, err = it.expr.Eval(it.env.Clone(dr.OriginalRow()))
				if err != nil {
					return err
				}
			}
			if !ok {
				return types.ErrColumnNotFound
			}
		}

		buf, err = encodeTempRow(buf, r)
		if err != nil {
			return errors.Wrap(err, "failed to encode row")
		}

		var encKey []byte
		key := r.Key()
		if key != nil {
			info, err := it.env.GetTx().Catalog.GetTableInfo(r.TableName())
			if err != nil {
				return err
			}
			encKey, err = info.EncodeKey(key)
			if err != nil {
				return err
			}
		}

		tk := tree.NewKey(v, types.NewTextValue(r.TableName()), types.NewBlobValue(encKey), types.NewBigintValue(counter))

		counter++

		err = it.temp.Put(tk, buf)
		if err != nil {
			return err
		}
	}
	if err := it.prev.Error(); err != nil {
		return err
	}

	return nil
}

func (it *TempTreeSortIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	if it.tempIt != nil {
		return it.tempIt.Error()
	}

	return it.prev.Error()
}

func (it *TempTreeSortIterator) Row() (database.Row, error) {
	kv, err := it.tempIt.Key().Decode()
	if err != nil {
		return nil, err
	}

	var tableName string
	tf := kv[1]
	if tf.Type() != types.TypeNull {
		tableName = types.AsString(tf)
	}

	var key *tree.Key
	kf := kv[2]
	if kf.Type() != types.TypeNull {
		key = tree.NewEncodedKey(types.AsByteSlice(kf))
	}

	data, err := it.tempIt.Value()
	if err != nil {
		return nil, err
	}

	r := decodeTempRow(data)

	var basicRow database.BasicRow

	basicRow.ResetWith(tableName, key, r)

	return &basicRow, nil
}

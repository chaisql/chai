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

func (op *TempTreeSortOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	db := in.GetDB()

	catalog := in.GetTx().Catalog
	tns := catalog.GetFreeTransientNamespace()
	tr, cleanup, err := tree.NewTransient(db.Engine.NewTransientSession(), tns, 0)
	if err != nil {
		return err
	}
	defer cleanup()

	var counter int64

	var buf []byte
	err = op.Prev.Iterate(in, func(out *environment.Environment) error {
		buf = buf[:0]

		// evaluate the sort expression
		v, err := op.Expr.Eval(out)
		if err != nil {
			if !errors.Is(err, types.ErrColumnNotFound) {
				return err
			}

			v = nil
		}

		if v == nil {
			// the expression might be pointing to the original row.
			v, err = op.Expr.Eval(out.Outer)
			if err != nil {
				// the only valid error here is a missing column.
				if !errors.Is(err, types.ErrColumnNotFound) {
					return err
				}
			}
		}

		r, ok := out.GetDatabaseRow()
		if !ok {
			return errors.New("missing row")
		}

		// TODO: we should find a way to encode using the table info.

		buf, err = encodeTempRow(buf, r)
		if err != nil {
			return errors.Wrap(err, "failed to encode row")
		}

		var encKey []byte
		key := r.Key()
		if key != nil {
			info, err := catalog.GetTableInfo(r.TableName())
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

		return tr.Put(tk, buf)
	})
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)
	var br database.BasicRow
	return tr.IterateOnRange(nil, op.Desc, func(k *tree.Key, data []byte) error {
		kv, err := k.Decode()
		if err != nil {
			return err
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

		r := decodeTempRow(data)

		br.ResetWith(tableName, key, r)

		newEnv.SetRow(&br)

		return fn(&newEnv)
	})
}

func (op *TempTreeSortOperator) String() string {
	if op.Desc {
		return fmt.Sprintf("rows.TempTreeSortReverse(%s)", op.Expr)
	}

	return fmt.Sprintf("rows.TempTreeSort(%s)", op.Expr)
}

func encodeTempRow(buf []byte, r row.Row) ([]byte, error) {
	var values []types.Value
	err := r.Iterate(func(column string, v types.Value) error {
		values = append(values, types.NewTextValue(column))
		values = append(values, types.NewIntegerValue(int32(v.Type())))
		values = append(values, v)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to iterate row")
	}

	return types.EncodeValuesAsKey(buf, values...)
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

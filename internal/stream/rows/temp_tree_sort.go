package rows

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/internal/types"
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
	tr, cleanup, err := tree.NewTransient(db.Store.NewTransientSession(), tns, 0)
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
			return err
		}

		if types.IsNull(v) {
			// the expression might be pointing to the original row.
			v, err = op.Expr.Eval(out.Outer)
			if err != nil {
				// the only valid error here is a missing field.
				if !errors.Is(err, types.ErrFieldNotFound) {
					return err
				}
			}
		}

		row, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		var info *database.TableInfo
		if row.TableName() != "" {
			info, err = catalog.GetTableInfo(row.TableName())
			if err != nil {
				return err
			}

			buf, err = info.EncodeObject(in.GetTx(), buf, row.Object())
			if err != nil {
				return err
			}
		} else {
			buf, err = encoding.EncodeObject(buf, row.Object())
			if err != nil {
				return err
			}
		}

		var encKey []byte
		key := row.Key()
		if key != nil {
			encKey, err = info.EncodeKey(key)
			if err != nil {
				return err
			}
		}

		tk := tree.NewKey(v, types.NewTextValue(row.TableName()), types.NewBlobValue(encKey), types.NewIntegerValue(counter))

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
		if tf.Type() != types.NullValue {
			tableName = types.As[string](tf)
		}

		var key *tree.Key
		kf := kv[2]
		if kf.Type() != types.NullValue {
			key = tree.NewEncodedKey(types.As[[]byte](kf))
		}

		var obj types.Object

		if tableName != "" {
			info, err := catalog.GetTableInfo(tableName)
			if err != nil {
				return err
			}
			obj = database.NewEncodedObject(&info.FieldConstraints, data)
		} else {
			obj = encoding.DecodeObject(data, false /* intAsDouble */)
		}

		br.ResetWith(tableName, key, obj)

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

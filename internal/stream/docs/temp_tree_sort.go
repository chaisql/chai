package docs

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
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

	catalog := in.GetCatalog()
	tns := catalog.GetFreeTransientNamespace()
	tr, cleanup, err := tree.NewTransient(db.Store.NewTransientSession(), tns)
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
			// the expression might be pointing to the original document.
			v, err = op.Expr.Eval(out.Outer)
			if err != nil {
				// the only valid error here is a missing field.
				if !errors.Is(err, types.ErrFieldNotFound) {
					return err
				}
			}
		}

		doc, ok := out.GetDocument()
		if !ok {
			panic("missing document")
		}

		tableName, _ := out.Get(environment.TableKey)

		var encKey []byte
		key, ok := out.GetKey()
		if ok {
			encKey = key.Encoded
		}

		tk := tree.NewKey(v, tableName, types.NewBlobValue(encKey), types.NewIntegerValue(counter))

		counter++

		buf, err = encoding.EncodeDocument(buf, doc)
		if err != nil {
			return err
		}
		return tr.Put(tk, buf)
	})
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	return tr.IterateOnRange(nil, op.Desc, func(k *tree.Key, data []byte) error {
		kv, err := k.Decode()
		if err != nil {
			return err
		}

		tableName := kv[1]
		if tableName.Type() != types.NullValue {
			newEnv.Set(environment.TableKey, tableName)
		}

		docKey := kv[2]
		if docKey.Type() != types.NullValue {
			newEnv.SetKey(tree.NewEncodedKey(types.As[[]byte](docKey)))
		}

		newEnv.SetDocument(encoding.DecodeDocument(data, false /* intAsDouble */))

		return fn(&newEnv)
	})
}

func (op *TempTreeSortOperator) String() string {
	if op.Desc {
		return fmt.Sprintf("docs.TempTreeSortReverse(%s)", op.Expr)
	}

	return fmt.Sprintf("docs.TempTreeSort(%s)", op.Expr)
}

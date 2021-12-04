package stream

import (
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

// A TempTreeSortOperator consumes every value of the stream and outputs them in order.
type TempTreeSortOperator struct {
	baseOperator
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

	tr, cleanup, err := database.NewTransientTree(db)
	if err != nil {
		return err
	}
	defer cleanup()

	var counter int64

	err = op.Prev.Iterate(in, func(out *environment.Environment) error {
		// evaluate the sort expression
		v, err := op.Expr.Eval(out)
		if err != nil {
			return err
		}

		doc, ok := out.GetDocument()
		if !ok {
			panic("missing document")
		}

		tableName, _ := out.Get(environment.TableKey)

		key, _ := out.Get(environment.DocPKKey)

		tk, err := tree.NewKey(v, tableName, key, types.NewIntegerValue(counter))
		if err != nil {
			return err
		}

		counter++

		return tr.Put(tk, types.NewDocumentValue(doc))
	})
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	return tr.Iterate(nil, op.Desc, func(k tree.Key, v types.Value) error {
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
			newEnv.Set(environment.DocPKKey, docKey)
		}

		doc := v.V().(types.Document)

		newEnv.SetDocument(doc)

		return fn(&newEnv)
	})
}

func (op *TempTreeSortOperator) String() string {
	if op.Desc {
		return stringutil.Sprintf("tempTreeSortReverse(%s)", op.Expr)
	}

	return stringutil.Sprintf("tempTreeSort(%s)", op.Expr)
}

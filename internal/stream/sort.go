package stream

import (
	"bytes"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
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

	tmp, cleanup, err := database.NewTransientIndex(db, "sort", []document.Path{{}, {}, {}}, false)
	if err != nil {
		return err
	}
	defer cleanup()

	var buf bytes.Buffer

	err = op.Prev.Iterate(in, func(out *environment.Environment) error {
		doc, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		v, err := op.Expr.Eval(out)
		if err != nil {
			return err
		}

		buf.Reset()
		// TODO check if the document is already encoded
		err = db.Codec.NewEncoder(&buf).EncodeDocument(doc)
		if err != nil {
			return err
		}

		tableName, ok := out.Get(environment.TableKey)
		if !ok {
			return errors.New("missing table name")
		}
		// document key is optional
		key, _ := out.Get(environment.DocPKKey)

		return tmp.Index.Set([]types.Value{v, tableName, key}, buf.Bytes())
	})
	if err != nil {
		return err
	}

	iterate := tmp.Index.AscendGreaterOrEqual
	if op.Desc {
		iterate = tmp.Index.DescendLessOrEqual
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	doc := db.Codec.NewDecoder(nil)
	return iterate(nil, func(idxKey, encDoc []byte) error {
		a, _, err := encoding.DecodeArray(idxKey)
		if err != nil {
			return err
		}

		tableName, err := a.GetByIndex(1)
		if err != nil {
			return err
		}
		newEnv.Set(environment.TableKey, tableName)

		docKey, err := a.GetByIndex(2)
		if err != nil && err != document.ErrFieldNotFound {
			return err
		} else if err == nil {
			newEnv.Set(environment.DocPKKey, docKey)
		}

		doc.Reset(encDoc)

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

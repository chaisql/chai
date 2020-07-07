package query

import (
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/sql/query/expr"
)

// updateBufferSize is the size of the buffer used to update documents.
const updateBufferSize = 100

// UpdateStmt is a DSL that allows creating a full Update query.
type UpdateStmt struct {
	TableName string

	// SetPairs is used along with the Set clause. It holds
	// each field with its corresponding value that
	// should be set in the document.
	SetPairs map[string]expr.Expr
	// UnsetFields is used along with the Unset clause. It holds
	// each field that should be unset from the document.
	UnsetFields []string

	WhereExpr expr.Expr
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt UpdateStmt) IsReadOnly() bool {
	return false
}

// Run runs the Update table statement in the given transaction.
// It implements the Statement interface.
func (stmt UpdateStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	isSet := len(stmt.SetPairs) != 0
	isUnset := len(stmt.UnsetFields) != 0
	if !isSet && !isUnset {
		return res, errors.New("neither Set or Unset method called")
	}

	t, err := tx.GetTable(stmt.TableName)
	if err != nil {
		return res, err
	}
	// replace store implementation by a resumable store, temporarily.
	rit := resumableIterator{store: t.Store}

	stack := expr.EvalStack{
		Tx:     tx,
		Params: args,
	}

	st := document.NewStream(&rit)
	st = st.Filter(whereClause(stmt.WhereExpr, stack)).Limit(updateBufferSize)

	keys := make([][]byte, updateBufferSize)
	docs := make([]document.FieldBuffer, updateBufferSize)

	for {
		var i int

		err = st.Iterate(func(d document.Document) error {
			rk, ok := d.(document.Keyer)

			if !ok {
				return errors.New("attempt to update document without key")
			}
			docs[i].Reset()
			err := docs[i].ScanDocument(d)
			if err != nil {
				return err
			}

			switch {
			case isSet:
				err = stmt.set(&docs[i], tx, args)
				if err != nil {
					return err
				}
			case isUnset:
				err = stmt.unset(&docs[i], t)
				if err != nil {
					return err
				}
			}

			// copy the key and reuse the buffer
			keys[i] = append(keys[i][0:0], rk.Key()...)
			i++

			return nil
		})

		for j := 0; j < i; j++ {
			err = t.Replace(keys[j], docs[j])
			if err != nil {
				return res, err
			}
		}

		if i < deleteBufferSize {
			break
		}

		rit.curKey = keys[i-1]
	}

	return res, err
}

// set executes the Set clause.
func (stmt UpdateStmt) set(d *document.FieldBuffer, tx *database.Transaction, args []expr.Param) error {
	for fname, e := range stmt.SetPairs {
		ev, err := e.Eval(expr.EvalStack{
			Tx:       tx,
			Document: d,
			Params:   args,
		})
		if err != nil && err != document.ErrFieldNotFound {
			return err
		}

		path := document.NewValuePath(fname)
		err = d.Set(path, ev)
		fmt.Printf("ERROR %s\n", err)
		if err != nil {
			return err
		}
	}

	return nil
}

// unset executes the Unset clause.
func (stmt UpdateStmt) unset(d *document.FieldBuffer, t *database.Table) error {
	for _, f := range stmt.UnsetFields {
		_, err := d.GetByField(f)
		if err != nil {
			// The field doesn't exist, we process the next one.
			continue
		}

		err = d.Delete(f)
		if err != nil {
			return err
		}
	}

	_, err := t.ValidateConstraints(d)
	if err != nil {
		return fmt.Errorf("cannot unset: %v", err)
	}

	return nil
}

// storeFromKey implements an engine.Store which iterates from a certain key.
// it is used to resume iteration.
type resumableIterator struct {
	store engine.Store

	curKey []byte
}

// AscendGreaterOrEqual uses key as pivot if pivot is nil
func (u *resumableIterator) Iterate(fn func(d document.Document) error) error {
	var d encodedDocumentWithKey
	var err error

	it := u.store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	for it.Seek(u.curKey); it.Valid(); it.Next() {
		item := it.Item()

		d.key = item.Key()
		d.EncodedDocument, err = item.ValueCopy(d.EncodedDocument)
		if err != nil {
			return err
		}

		err = fn(&d)
		if err != nil {
			return err
		}
	}

	return nil
}

type encodedDocumentWithKey struct {
	encoding.EncodedDocument

	key []byte
}

func (e encodedDocumentWithKey) Key() []byte {
	return e.key
}

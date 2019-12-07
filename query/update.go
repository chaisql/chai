package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine"
)

// updateBufferSize is the size of the buffer used to update documents.
const updateBufferSize = 100

// UpdateStmt is a DSL that allows creating a full Update query.
type UpdateStmt struct {
	TableName string
	Pairs     map[string]Expr
	WhereExpr Expr
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt UpdateStmt) IsReadOnly() bool {
	return false
}

// Run runs the Update table statement in the given transaction.
// It implements the Statement interface.
func (stmt UpdateStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if len(stmt.Pairs) == 0 {
		return res, errors.New("Set method not called")
	}

	stack := EvalStack{
		Tx:     tx,
		Params: args,
	}

	t, err := tx.GetTable(stmt.TableName)
	if err != nil {
		return res, err
	}

	// replace store implementation by a resumable store, temporarily.
	resumableStore := storeFromKey{Store: t.Store}
	t.Store = &resumableStore

	st := document.NewStream(t)
	st = st.Filter(whereClause(stmt.WhereExpr, stack)).Limit(updateBufferSize)

	keys := make([][]byte, updateBufferSize)
	docs := make([]document.FieldBuffer, updateBufferSize)

	for {
		var i int

		err = st.Iterate(func(r document.Document) error {
			rk, ok := r.(document.Keyer)
			if !ok {
				return errors.New("attempt to update record without key")
			}

			docs[i].Reset()
			err := docs[i].ScanDocument(r)
			if err != nil {
				return err
			}

			for fname, e := range stmt.Pairs {
				_, err := docs[i].GetByField(fname)
				if err != nil {
					continue
				}

				ev, err := e.Eval(EvalStack{
					Tx:     tx,
					Record: r,
					Params: args,
				})
				if err != nil {
					return err
				}

				err = docs[i].Replace(fname, ev)
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

		resumableStore.key = keys[i-1]
	}

	return res, err
}

// storeFromKey implements an engine.Store which iterates from a certain key.
// it is used to resume iteration.
type storeFromKey struct {
	engine.Store

	key []byte
}

// AscendGreaterOrEqual uses key as pivot if pivot is nil
func (s *storeFromKey) AscendGreaterOrEqual(pivot []byte, fn func(k, v []byte) error) error {
	if len(pivot) == 0 {
		pivot = s.key
	}

	return s.Store.AscendGreaterOrEqual(pivot, fn)
}

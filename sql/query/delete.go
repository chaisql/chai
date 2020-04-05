package query

import (
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
)

// deleteBufferSize is the size of the buffer used to delete documents.
const deleteBufferSize = 100

// DeleteStmt is a DSL that allows creating a full Delete query.
type DeleteStmt struct {
	TableName string
	WhereExpr Expr
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt DeleteStmt) IsReadOnly() bool {
	return false
}

// Run deletes matching documents by batches of deleteBufferSize documents.
// Some engines can't iterate while deleting keys (https://github.com/etcd-io/bbolt/issues/146)
// and some can't create more than one iterator per read-write transaction (https://github.com/dgraph-io/badger/issues/1093).
// To deal with these limitations, Run will iterate on a limited number of documents, copy the keys
// to a buffer and delete them after the iteration is complete, and it will do that until there is no document
// left to delete.
// Increasing deleteBufferSize will occasionate less key searches (O(log n) for most engines) but will take more memory.
func (stmt DeleteStmt) Run(tx *database.Transaction, args []Param) (Result, error) {
	var res Result
	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	stack := EvalStack{Tx: tx, Params: args}

	t, err := tx.GetTable(stmt.TableName)
	if err != nil {
		return res, err
	}

	st := document.NewStream(t)
	st = st.Filter(whereClause(stmt.WhereExpr, stack)).Limit(deleteBufferSize)

	keys := make([][]byte, deleteBufferSize)

	for {
		var i int

		err = st.Iterate(func(d document.Document) error {
			k, ok := d.(document.Keyer)
			if !ok {
				return errors.New("attempt to delete document without key")
			}
			// copy the key and reuse the buffer
			keys[i] = append(keys[i][0:0], k.Key()...)
			i++
			return nil
		})
		if err != nil {
			return res, err
		}

		keys = keys[:i]

		for _, key := range keys {
			err = t.Delete(key)
			if err != nil {
				return res, err
			}
		}

		if i < deleteBufferSize {
			break
		}
	}

	return res, nil
}

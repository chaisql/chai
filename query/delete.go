package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/record"
)

// DeleteStmt is a DSL that allows creating a full Delete query.
type deleteStmt struct {
	tableName string
	whereExpr expr
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt deleteStmt) IsReadOnly() bool {
	return false
}

const deleteBufferSize = 100

// Run deletes matching records by batches of deleteBufferSize records.
// Some engines can't iterate while deleting keys (https://github.com/etcd-io/bbolt/issues/146)
// and some can't create more than one iterator per read-write transaction (https://github.com/dgraph-io/badger/issues/1093).
// To deal with these limitations, Run will iterate on a limited number of records, copy the keys
// to a buffer and delete them after the iteration is complete, and it will do that until there is no record
// left to delete.
// Increasing deleteBufferSize will occasionate less key searches (O(log n) for most engines) but will take more memory.
func (stmt deleteStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result
	if stmt.tableName == "" {
		return res, errors.New("missing table name")
	}

	stack := evalStack{Tx: tx, Params: args}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return res, err
	}

	st := record.NewStream(t)
	st = st.Filter(whereClause(stmt.whereExpr, stack)).Limit(deleteBufferSize)

	keys := make([][]byte, deleteBufferSize)

	for {
		var i int

		err = st.Iterate(func(r record.Record) error {
			k, ok := r.(record.Keyer)
			if !ok {
				return errors.New("attempt to delete record without key")
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

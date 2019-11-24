package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/record"
)

// updateStmt is a DSL that allows creating a full Update query.
type updateStmt struct {
	tableName string
	pairs     map[string]expr
	whereExpr expr
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt updateStmt) IsReadOnly() bool {
	return false
}

// Run runs the Update table statement in the given transaction.
// It implements the Statement interface.
func (stmt updateStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.tableName == "" {
		return res, errors.New("missing table name")
	}

	if len(stmt.pairs) == 0 {
		return res, errors.New("Set method not called")
	}

	stack := evalStack{
		Tx:     tx,
		Params: args,
	}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return res, err
	}

	st := record.NewStream(t)
	st = st.Filter(whereClause(stmt.whereExpr, stack))

	err = st.Iterate(func(r record.Record) error {
		rk, ok := r.(record.Keyer)
		if !ok {
			return errors.New("attempt to update record without key")
		}

		var fb record.FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return err
		}

		for fname, e := range stmt.pairs {
			f, err := fb.GetField(fname)
			if err != nil {
				continue
			}

			v, err := e.Eval(evalStack{
				Tx:     tx,
				Record: r,
				Params: args,
			})
			if err != nil {
				return err
			}

			if v.IsList {
				return fmt.Errorf("expected value got list")
			}

			f.Type = v.Value.Type
			f.Data = v.Value.Data
			err = fb.Replace(f.Name, f)
			if err != nil {
				return err
			}
		}

		err = t.Replace(rk.Key(), &fb)
		if err != nil {
			return err
		}

		return nil
	})
	return res, err
}

package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
)

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

	st := document.NewStream(t)
	st = st.Filter(whereClause(stmt.WhereExpr, stack))

	err = st.Iterate(func(r document.Document) error {
		rk, ok := r.(document.Keyer)
		if !ok {
			return errors.New("attempt to update record without key")
		}

		var fb document.FieldBuffer
		err := fb.ScanDocument(r)
		if err != nil {
			return err
		}

		for fname, e := range stmt.Pairs {
			v, err := fb.GetByField(fname)
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

			if ev.IsList {
				return fmt.Errorf("expected value got list")
			}

			v.Type = ev.Value.Type
			v.Data = ev.Value.Data
			err = fb.Replace(fname, v)
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

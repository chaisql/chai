package statement

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/cockroachdb/errors"
)

// A Statement represents a unique action that can be executed against the database.
type Statement interface {
	Bind(*Context) error
	Run(*Context) (Result, error)
	IsReadOnly() bool
}

type basePreparedStatement struct {
	Preparer Preparer
	ReadOnly bool
}

func (stmt *basePreparedStatement) IsReadOnly() bool {
	return stmt.ReadOnly
}

func (stmt *basePreparedStatement) Run(ctx *Context) (Result, error) {
	s, err := stmt.Preparer.Prepare(ctx)
	if err != nil {
		return Result{}, err
	}

	return s.Run(ctx)
}

type Context struct {
	DB     *database.Database
	Conn   *database.Connection
	Tx     *database.Transaction
	Params []environment.Param
}

type Preparer interface {
	Prepare(*Context) (Statement, error)
}

// Result of a query.
type Result struct {
	Result database.Result
	Tx     *database.Transaction
	closed bool
	err    error
}

func (r *Result) Iterator() (database.Iterator, error) {
	if r.Result == nil {
		return nil, nil
	}

	return r.Result.Iterator()
}

func (r *Result) Iterate(fn func(r database.Row) error) (err error) {
	if r.Result == nil {
		return nil
	}
	defer func() {
		if err != nil {
			r.err = err
		}
	}()

	it, err := r.Result.Iterator()
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		rr, err := it.Row()
		if err != nil {
			return err
		}
		if err := fn(rr); err != nil {
			return err
		}
	}
	return it.Error()
}

// Skip iterates over the result and skips all rows.
// It is useful when you need the query to be executed
// but don't care about the results.
func (r *Result) Skip() (err error) {
	if r.Result == nil {
		return nil
	}
	defer func() {
		if err != nil {
			r.err = err
		}
	}()

	it, err := r.Result.Iterator()
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
	}

	return it.Error()
}

// Close the result stream.
// After closing the result, Stream is not supposed to be used.
// If the result stream was already closed, it returns an error.
func (r *Result) Close() (err error) {
	if r == nil {
		return nil
	}

	if r.closed {
		return errors.New("result already closed")
	}

	r.closed = true

	if r.Tx != nil {
		if r.Tx.Writable && r.err == nil {
			err = r.Tx.Commit()
		} else {
			err = r.Tx.Rollback()
		}
	}

	return err
}

func BindExpr(ctx *Context, tableName string, e expr.Expr) (err error) {
	if e == nil {
		return nil
	}

	var info *database.TableInfo
	if tableName != "" {
		info, err = ctx.Tx.Catalog.GetTableInfo(tableName)
		if err != nil {
			return err
		}
	}

	expr.Walk(e, func(e expr.Expr) bool {
		switch t := e.(type) {
		case *expr.Column:
			if t == nil {
				return true
			}

			if info == nil {
				err = errors.New("no table specified")
				return false
			}

			cc := info.ColumnConstraints.GetColumnConstraint(t.Name)
			if cc == nil {
				err = errors.Newf("column %s does not exist", t)
				return false
			}
			t.Table = tableName
		}

		return true
	})

	return err
}

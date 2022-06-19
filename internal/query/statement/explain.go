package statement

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/types"
)

// ExplainStmt is a Statement that
// displays information about how a statement
// is going to be executed, without executing it.
type ExplainStmt struct {
	Statement Preparer
}

// Run analyses the inner statement and displays its execution plan.
// If the statement is a stream, Optimize will be called prior to
// displaying all the operations.
// Explain currently only works on SELECT, UPDATE, INSERT and DELETE statements.
func (stmt *ExplainStmt) Run(ctx *Context) (Result, error) {
	st, err := stmt.Statement.Prepare(ctx)
	if err != nil {
		return Result{}, err
	}

	s, ok := st.(*PreparedStreamStmt)
	if !ok {
		return Result{}, errors.New("EXPLAIN only works on INSERT, SELECT, UPDATE AND DELETE statements")
	}

	var plan string
	if s.Stream != nil {
		plan = s.Stream.String()
	} else {
		plan = "<no exec>"
	}

	newStatement := PreparedStreamStmt{
		Stream: &stream.Stream{
			Op: docs.Project(
				&expr.NamedExpr{
					ExprName: "plan",
					Expr:     expr.LiteralValue{Value: types.NewTextValue(plan)},
				}),
		},
		ReadOnly: true,
	}
	return newStatement.Run(ctx)
}

// IsReadOnly indicates that this statement doesn't write anything into
// the database.
func (s *ExplainStmt) IsReadOnly() bool {
	return true
}

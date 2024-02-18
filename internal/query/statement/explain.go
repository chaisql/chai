package statement

import (
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/planner"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

var _ Statement = &ExplainStmt{}

// ExplainStmt is a Statement that
// displays information about how a statement
// is going to be executed, without executing it.
type ExplainStmt struct {
	Statement Preparer
}

func (stmt *ExplainStmt) Bind(ctx *Context) error {
	if s, ok := stmt.Statement.(Statement); ok {
		return s.Bind(ctx)
	}

	return nil
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

	// Optimize the stream.
	s.Stream, err = planner.Optimize(s.Stream, ctx.Tx.Catalog, ctx.Params)
	if err != nil {
		return Result{}, err
	}

	var plan string
	if s.Stream != nil {
		plan = s.Stream.String()
	} else {
		plan = "<no exec>"
	}

	newStatement := PreparedStreamStmt{
		Stream: &stream.Stream{
			Op: rows.Project(
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

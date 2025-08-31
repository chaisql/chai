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
	if s, ok := stmt.Statement.(Bindable); ok {
		return s.Bind(ctx)
	}

	return nil
}

// Run analyses the inner statement and displays its execution plan.
// If the statement is a stream, Optimize will be called prior to
// displaying all the operations.
// Explain currently only works on SELECT, UPDATE, INSERT and DELETE statements.
func (stmt *ExplainStmt) Run(ctx *Context) (*Result, error) {
	st, err := stmt.Statement.Prepare(ctx)
	if err != nil {
		return nil, err
	}

	var s *stream.Stream

	switch stmt := st.(type) {
	case *InsertStmt:
		s = stmt.Stream
	case *SelectStmt:
		s = stmt.Stream
	case *UpdateStmt:
		s = stmt.Stream
	case *DeleteStmt:
		s = stmt.Stream
	default:
		return nil, errors.New("EXPLAIN only works on INSERT, SELECT, UPDATE AND DELETE statements")
	}

	// Optimize the stream.
	s, err = planner.Optimize(s, ctx.Conn.GetTx().Catalog, ctx.Params)
	if err != nil {
		return nil, err
	}

	var plan string
	if s != nil {
		plan = s.String()
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
	}
	return newStatement.Run(ctx)
}

// IsReadOnly indicates that this statement doesn't write anything into
// the database.
func (s *ExplainStmt) IsReadOnly() bool {
	return true
}

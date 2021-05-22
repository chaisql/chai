package query

import (
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/planner"
	"github.com/genjidb/genji/stream"
)

// ExplainStmt is a Statement that
// displays information about how a statement
// is going to be executed, without executing it.
type ExplainStmt struct {
	Statement Statement
}

// Run analyses the inner statement and displays its execution plan.
// If the statement is a stream, Optimize will be called prior to
// displaying all the operations.
// Explain currently only works on SELECT, UPDATE, INSERT and DELETE statements.
func (s *ExplainStmt) Run(tx *database.Transaction, params []expr.Param) (Result, error) {
	switch t := s.Statement.(type) {
	case *StreamStmt:
		s, err := planner.Optimize(t.Stream, tx, params)
		if err != nil {
			return Result{}, err
		}

		var plan string
		if s != nil {
			plan = s.String()
		} else {
			plan = "<no exec>"
		}

		newStatement := StreamStmt{
			Stream: &stream.Stream{
				Op: stream.Project(
					&expr.NamedExpr{
						ExprName: "plan",
						Expr:     expr.LiteralValue(document.NewTextValue(plan)),
					}),
			},
			ReadOnly: true,
		}
		return newStatement.Run(tx, params)
	}

	return Result{}, errors.New("EXPLAIN only works on INSERT, SELECT, UPDATE AND DELETE statements")
}

// IsReadOnly indicates that this statement doesn't write anything into
// the database.
func (s *ExplainStmt) IsReadOnly() bool {
	return true
}

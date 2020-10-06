package planner

import (
	"context"
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

// ExplainStmt is a query.Statement that
// displays information about how a statement
// is going to be executed, without executing it.
type ExplainStmt struct {
	Statement query.Statement
}

// Run analyses the inner statement and displays its execution plan.
// If the statement is a tree, Bind and Optimize will be called prior to
// displaying all the operations.
// Explain currently only works on SELECT, UPDATE and DELETE statements.
func (s *ExplainStmt) Run(ctx context.Context, tx *database.Transaction, params []expr.Param) (query.Result, error) {
	switch t := s.Statement.(type) {
	case *Tree:
		err := Bind(ctx, t, tx, params)
		if err != nil {
			return query.Result{}, err
		}

		t, err = Optimize(ctx, t)
		if err != nil {
			return query.Result{}, err
		}

		return s.createResult(t.String())
	}

	return query.Result{}, errors.New("EXPLAIN only works on SELECT, UPDATE AND DELETE statements")
}

func (s *ExplainStmt) createResult(text string) (query.Result, error) {
	return query.Result{
		Stream: document.NewStream(
			document.NewIterator(
				document.NewFieldBuffer().
					Add("plan", document.NewTextValue(text)))),
	}, nil
}

// IsReadOnly indicates that this statement doesn't write anything into
// the database.
func (s *ExplainStmt) IsReadOnly() bool {
	return true
}

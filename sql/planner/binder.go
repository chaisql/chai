package planner

import (
	"context"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/sql/query/expr"
)

// Bind updates every node that refers to a database ressource.
func Bind(ctx context.Context, t *Tree, tx *database.Transaction, params []expr.Param) error {
	if t.Root != nil {
		return bindNode(ctx, t.Root, tx, params)
	}

	return nil
}

func bindNode(ctx context.Context, n Node, tx *database.Transaction, params []expr.Param) error {
	var err error

	err = n.Bind(ctx, tx, params)
	if err != nil {
		return err
	}

	if n.Left() != nil {
		err = bindNode(ctx, n.Left(), tx, params)
		if err != nil {
			return err
		}
	}

	if n.Right() != nil {
		err = bindNode(ctx, n.Right(), tx, params)
		if err != nil {
			return err
		}
	}

	return nil
}

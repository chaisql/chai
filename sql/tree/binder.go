package tree

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/sql/query/expr"
)

// Bind updates every node that refers to a database ressource.
func Bind(t *Tree, tx *database.Transaction, params []expr.Param) error {
	if t.Root != nil {
		return bindNode(t.Root, tx, params)
	}

	return nil
}

func bindNode(n Node, tx *database.Transaction, params []expr.Param) error {
	var err error

	err = n.Bind(tx, params)
	if err != nil {
		return err
	}

	if n.Left() != nil {
		err = bindNode(n.Left(), tx, params)
		if err != nil {
			return err
		}
	}

	if n.Right() != nil {
		err = bindNode(n.Right(), tx, params)
		if err != nil {
			return err
		}
	}

	return nil
}

package tree

import "github.com/genjidb/genji/database"

// Bind updates every node that refers to a database ressource.
func Bind(t *Tree, tx *database.Transaction) error {
	if t.Root != nil {
		return bindNode(t.Root, tx)
	}

	return nil
}

func bindNode(n Node, tx *database.Transaction) error {
	var err error

	switch op := n.(type) {
	case *tableInputNode:
		op.table, err = tx.GetTable(op.tableName)
		if err != nil {
			return err
		}
	case *indexInputNode:
		op.table, err = tx.GetTable(op.tableName)
		if err != nil {
			return err
		}
		op.index, err = tx.GetIndex(op.indexName)
		if err != nil {
			return err
		}
	case *replacementNode:
		op.table, err = tx.GetTable(op.tableName)
		if err != nil {
			return err
		}
	case *deletionNode:
		op.table, err = tx.GetTable(op.tableName)
		if err != nil {
			return err
		}
	}

	if n.Left() != nil {
		err = bindNode(n.Left(), tx)
		if err != nil {
			return err
		}
	}

	if n.Right() != nil {
		err = bindNode(n.Right(), tx)
		if err != nil {
			return err
		}
	}

	return nil
}

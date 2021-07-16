package dbutil

import (
	"context"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/types"
)

func QueryTables(tx *genji.Tx, tables []string, fn func(name, query string) error) error {
	query := "SELECT name, sql FROM __genji_catalog WHERE type = 'table' AND name != '__genji_sequence'"
	if len(tables) > 0 {
		query += " AND name IN ?"
	}

	res, err := tx.Query(query, tables)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(d types.Document) error {
		// Get table name.
		var name, query string
		if err := document.Scan(d, &name, &query); err != nil {
			return err
		}

		return fn(name, query)
	})
}

func ListIndexes(ctx context.Context, db *genji.DB, tableName string) ([]string, error) {
	var listName []string
	err := db.View(func(tx *genji.Tx) error {
		q := "SELECT sql FROM __genji_catalog WHERE type = 'index'"
		if tableName != "" {
			q += " AND table_name = ?"
		}
		res, err := tx.Query(q, tableName)
		if err != nil {
			return err
		}
		defer res.Close()

		return res.Iterate(func(d types.Document) error {
			var query string
			err = document.Scan(d, &query)
			if err != nil {
				return err
			}

			q, err := parser.ParseQuery(query)
			if err != nil {
				return err
			}

			listName = append(listName, q.Statements[0].(*statement.CreateIndexStmt).Info.IndexName)
			return nil
		})
	})

	return listName, err
}

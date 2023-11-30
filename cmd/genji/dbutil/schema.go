package dbutil

import (
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
)

func QueryTables(tx *genji.Tx, tables []string, fn func(name, query string) error) error {
	query := "SELECT name, sql FROM __genji_catalog WHERE type = 'table' AND name NOT LIKE '__genji_%'"
	if len(tables) > 0 {
		query += " AND name IN ?"
	}

	res, err := tx.Query(query, tables)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(r *genji.Row) error {
		// Get table name.
		var name, query string
		if err := r.Scan(&name, &query); err != nil {
			return err
		}

		return fn(name, query)
	})
}

func ListIndexes(db *genji.DB, tableName string) ([]string, error) {
	var listName []string
	q := "SELECT sql FROM __genji_catalog WHERE type = 'index'"
	if tableName != "" {
		q += " AND owner.table_name = ?"
	}
	res, err := db.Query(q, tableName)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	err = res.Iterate(func(r *genji.Row) error {
		var query string
		err = r.Scan(&query)
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
	if err != nil {
		return nil, err
	}

	return listName, err
}

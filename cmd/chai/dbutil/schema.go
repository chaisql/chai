package dbutil

import (
	"fmt"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
)

func QueryTables(tx *chai.Tx, tables []string, fn func(name, query string) error) error {
	query := "SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name NOT LIKE '__chai_%'"
	var args []any
	if len(tables) > 0 {
		var arg string

		for i := range tables {
			arg += "?"

			if i < len(tables)-1 {
				arg += ", "
			}

			args = append(args, tables[i])
		}

		query += fmt.Sprintf(" AND name IN (%s)", arg)
	}

	res, err := tx.Query(query, args...)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(r *chai.Row) error {
		// Get table name.
		var name, query string
		if err := r.Scan(&name, &query); err != nil {
			return err
		}

		return fn(name, query)
	})
}

func ListIndexes(db *chai.DB, tableName string) ([]string, error) {
	var listName []string
	q := "SELECT sql FROM __chai_catalog WHERE type = 'index'"
	if tableName != "" {
		q += " AND owner_table_name = ?"
	}
	res, err := db.Query(q, tableName)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	err = res.Iterate(func(r *chai.Row) error {
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

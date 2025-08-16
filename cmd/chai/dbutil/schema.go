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

	it, err := res.Iterator()
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		var name, query string
		r, err := it.Row()
		if err != nil {
			return err
		}

		err = r.Scan(&name, &query)
		if err != nil {
			return err
		}

		if err := fn(name, query); err != nil {
			return err
		}
	}

	return it.Error()
}

func ListIndexes(db *chai.DB, tableName string) ([]string, error) {
	var listName []string
	q := "SELECT sql FROM __chai_catalog WHERE type = 'index'"
	if tableName != "" {
		q += " AND owner_table_name = ?"
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	res, err := conn.Query(q, tableName)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	it, err := res.Iterator()
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for it.Next() {
		var query string
		r, err := it.Row()
		if err != nil {
			return nil, err
		}

		err = r.Scan(&query)
		if err != nil {
			return nil, err
		}

		q, err := parser.ParseQuery(query)
		if err != nil {
			return nil, err
		}

		listName = append(listName, q.Statements[0].(*statement.CreateIndexStmt).Info.IndexName)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}

	return listName, err
}

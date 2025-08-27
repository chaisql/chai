package dbutil

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
)

func QueryTables(ctx context.Context, tx *sql.Tx, tables []string, fn func(name, query string) error) error {
	query := "SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name NOT LIKE '__chai_%'"
	var args []any
	if len(tables) > 0 {
		var arg string

		for i := range tables {
			arg += fmt.Sprintf("$%d", i+1)

			if i < len(tables)-1 {
				arg += ", "
			}

			args = append(args, tables[i])
		}

		query += fmt.Sprintf(" AND name IN (%s)", arg)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name, query string
		err := rows.Scan(&name, &query)
		if err != nil {
			return err
		}

		if err := fn(name, query); err != nil {
			return err
		}
	}

	return rows.Err()
}

func ListIndexes(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	var listName []string
	q := "SELECT sql FROM __chai_catalog WHERE type = 'index'"
	if tableName != "" {
		q += " AND owner_table_name = $1"
	}

	rows, err := db.QueryContext(ctx, q, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var query string

		err = rows.Scan(&query)
		if err != nil {
			return nil, err
		}

		q, err := parser.ParseQuery(query)
		if err != nil {
			return nil, err
		}

		listName = append(listName, q.Statements[0].(*statement.CreateIndexStmt).Info.IndexName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return listName, err
}

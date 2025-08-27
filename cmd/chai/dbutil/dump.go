package dbutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Dump takes a database and dumps its content as SQL queries in the given writer.
// If tables is provided, only selected tables will be outputted.
func Dump(ctx context.Context, db *sql.DB, w io.Writer, tables ...string) (err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = fmt.Fprintln(w, "BEGIN TRANSACTION;"); err != nil {
		return err
	}

	i := 0
	err = QueryTables(ctx, tx, tables, func(name, query string) error {
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
		}
		i++

		return dumpTable(ctx, tx, w, query, name)
	})
	if err != nil {
		_, er := fmt.Fprintln(w, "ROLLBACK;")
		return errors.Join(err, er)
	}

	_, err = fmt.Fprintln(w, "COMMIT;")
	return err
}

// dumpTable displays the content of the given table as SQL statements.
func dumpTable(ctx context.Context, tx *sql.Tx, w io.Writer, query, tableName string) error {
	// Dump schema first.
	if err := dumpSchema(tx, w, query, tableName); err != nil {
		return err
	}

	q := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := tx.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		cols, err := rows.Columns()
		if err != nil {
			return err
		}

		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range cols {
			valuePtrs[i] = &values[i]
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		var sb strings.Builder

		for i := range cols {
			if i > 0 {
				sb.WriteString(", ")
			}

			v := values[i]
			if v == nil {
				sb.WriteString("NULL")
				continue
			}

			fmt.Fprintf(&sb, "%v", v)
		}

		if _, err := fmt.Fprintf(w, "INSERT INTO %s VALUES (%s);\n", tableName, sb.String()); err != nil {
			return err
		}
	}

	return rows.Err()
}

// DumpSchema takes a database and dumps its schema as SQL queries in the given writer.
// If tables are provided, only selected tables will be outputted.
func DumpSchema(ctx context.Context, db *sql.DB, w io.Writer, tables ...string) (err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	i := 0
	return QueryTables(ctx, tx, tables, func(name, query string) error {
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
		}
		i++

		return dumpSchema(tx, w, query, name)
	})
}

// dumpSchema displays the schema of the given table as SQL statements.
func dumpSchema(tx *sql.Tx, w io.Writer, query string, tableName string) error {
	_, err := fmt.Fprintf(w, "%s;\n", query)
	if err != nil {
		return err
	}

	// Indexes statements.
	rows, err := tx.Query(`
		SELECT sql FROM __chai_catalog WHERE 
			type = 'index' AND owner_table_name = ? OR
			type = 'sequence' AND owner_table_name IS NULL
	`, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(w, "%s;\n", q); err != nil {
			return err
		}
	}

	return rows.Err()
}

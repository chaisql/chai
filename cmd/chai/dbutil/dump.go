package dbutil

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/chaisql/chai"
)

// Dump takes a database and dumps its content as SQL queries in the given writer.
// If tables is provided, only selected tables will be outputted.
func Dump(db *chai.DB, w io.Writer, tables ...string) error {
	conn, err := db.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = fmt.Fprintln(w, "BEGIN TRANSACTION;"); err != nil {
		return err
	}

	i := 0
	err = QueryTables(tx, tables, func(name, query string) error {
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
		}
		i++

		return dumpTable(tx, w, query, name)
	})
	if err != nil {
		_, er := fmt.Fprintln(w, "ROLLBACK;")
		return errors.Join(err, er)
	}

	_, err = fmt.Fprintln(w, "COMMIT;")
	return err
}

// dumpTable displays the content of the given table as SQL statements.
func dumpTable(tx *chai.Tx, w io.Writer, query, tableName string) error {
	// Dump schema first.
	if err := dumpSchema(tx, w, query, tableName); err != nil {
		return err
	}

	q := fmt.Sprintf("SELECT * FROM %s", tableName)
	res, err := tx.Query(q)
	if err != nil {
		return err
	}
	defer res.Close()

	// Inserts statements.
	return res.Iterate(func(r *chai.Row) error {
		cols, err := r.Columns()
		if err != nil {
			return err
		}

		m := make(map[string]interface{}, len(cols))
		err = r.MapScan(m)
		if err != nil {
			return err
		}

		var sb strings.Builder

		for i, c := range cols {
			if i > 0 {
				sb.WriteString(", ")
			}

			v := m[c]
			if v == nil {
				sb.WriteString("NULL")
				continue
			}

			fmt.Fprintf(&sb, "%v", v)
		}

		if _, err := fmt.Fprintf(w, "INSERT INTO %s VALUES (%s);\n", tableName, sb.String()); err != nil {
			return err
		}

		return nil
	})
}

// DumpSchema takes a database and dumps its schema as SQL queries in the given writer.
// If tables are provided, only selected tables will be outputted.
func DumpSchema(db *chai.DB, w io.Writer, tables ...string) error {
	conn, err := db.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	i := 0
	return QueryTables(tx, tables, func(name, query string) error {
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
func dumpSchema(tx *chai.Tx, w io.Writer, query string, tableName string) error {
	_, err := fmt.Fprintf(w, "%s;\n", query)
	if err != nil {
		return err
	}

	// Indexes statements.
	res, err := tx.Query(`
		SELECT sql FROM __chai_catalog WHERE 
			type = 'index' AND owner_table_name = ? OR
			type = 'sequence' AND owner_table_name IS NULL
	`, tableName)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(r *chai.Row) error {
		var q string

		err = r.Scan(&q)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "%s;\n", q)
		return err
	})
}

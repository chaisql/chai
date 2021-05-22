package dbutil

import (
	"context"
	"fmt"
	"io"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"go.uber.org/multierr"
)

// Dump takes a database and dumps its content as SQL queries in the given writer.
// If tables is provided, only selected tables will be outputted.
func Dump(ctx context.Context, db *genji.DB, w io.Writer, tables ...string) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = fmt.Fprintln(w, "BEGIN TRANSACTION;"); err != nil {
		return err
	}

	query := "SELECT table_name FROM __genji_tables"
	if len(tables) > 0 {
		query += " WHERE table_name IN ?"
	}

	res, err := tx.Query(query, tables)
	if err != nil {
		_, er := fmt.Fprintln(w, "ROLLBACK;")
		return multierr.Append(err, er)
	}
	defer res.Close()

	i := 0
	err = res.Iterate(func(d document.Document) error {
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
		}
		i++

		// Get table name.
		var tableName string
		if err := document.Scan(d, &tableName); err != nil {
			return err
		}

		return dumpTable(tx, w, tableName)
	})
	if err != nil {
		_, er := fmt.Fprintln(w, "ROLLBACK;")
		return multierr.Append(err, er)
	}

	_, err = fmt.Fprintln(w, "COMMIT;")
	return err
}

// dumpTable displays the content of the given table as SQL statements.
func dumpTable(tx *genji.Tx, w io.Writer, tableName string) error {
	// Dump schema first.
	if err := dumpSchema(tx, w, tableName); err != nil {
		return err
	}

	q := fmt.Sprintf("SELECT * FROM %s", tableName)
	res, err := tx.Query(q)
	if err != nil {
		return err
	}
	defer res.Close()

	// Inserts statements.
	insert := fmt.Sprintf("INSERT INTO %s VALUES", tableName)
	return res.Iterate(func(d document.Document) error {
		data, err := document.MarshalJSON(d)
		if err != nil {
			return err
		}

		if _, err := fmt.Fprintf(w, "%s %s;\n", insert, string(data)); err != nil {
			return err
		}

		return nil
	})
}

// DumpSchema takes a database and dumps its schema as SQL queries in the given writer.
// If tables are provided, only selected tables will be outputted.
func DumpSchema(ctx context.Context, db *genji.DB, w io.Writer, tables ...string) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := "SELECT table_name FROM __genji_tables"
	if len(tables) > 0 {
		query += " WHERE table_name IN ?"
	}

	res, err := tx.Query(query, tables)
	if err != nil {
		return err
	}
	defer res.Close()

	i := 0
	return res.Iterate(func(d document.Document) error {
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
		}
		i++

		// Get table name.
		var tableName string
		if err := document.Scan(d, &tableName); err != nil {
			return err
		}

		return dumpSchema(tx, w, tableName)
	})
}

// dumpSchema displays the schema of the given table as SQL statements.
func dumpSchema(tx *genji.Tx, w io.Writer, tableName string) error {
	d, err := tx.QueryDocument("SELECT sql FROM __genji_tables WHERE table_name = ?", tableName)
	if err != nil {
		return err
	}

	var tableSchema string
	err = document.Scan(d, &tableSchema)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "%s;\n", tableSchema)
	if err != nil {
		return err
	}

	// Indexes statements.
	res, err := tx.Query("SELECT sql FROM __genji_indexes WHERE table_name = ?", tableName)
	if err != nil {
		return err
	}

	defer res.Close()

	return res.Iterate(func(d document.Document) error {
		var indexQuery string

		err = document.Scan(d, &indexQuery)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "%s;\n", indexQuery)
		return err
	})
}

func ListIndexes(ctx context.Context, db *genji.DB, tableName string) ([]string, error) {
	var listName []string
	err := db.View(func(tx *genji.Tx) error {
		q := "SELECT index_name FROM __genji_indexes"
		if tableName != "" {
			q += " WHERE table_name = ?"
		}
		res, err := tx.Query(q, tableName)
		if err != nil {
			return err
		}
		defer res.Close()

		return res.Iterate(func(d document.Document) error {
			var name string
			err = document.Scan(d, &name)
			if err != nil {
				return err
			}
			listName = append(listName, name)
			return nil
		})
	})

	return listName, err
}

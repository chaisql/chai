package dbutil

import (
	"context"
	"fmt"
	"io"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
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

	i := 0
	err = queryTables(tx, tables, func(query string, ti *database.TableInfo) error {
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
		}
		i++

		return dumpTable(tx, w, query, ti.TableName)
	})
	if err != nil {
		_, er := fmt.Fprintln(w, "ROLLBACK;")
		return multierr.Append(err, er)
	}

	_, err = fmt.Fprintln(w, "COMMIT;")
	return err
}

func queryTables(tx *genji.Tx, tables []string, fn func(string, *database.TableInfo) error) error {
	query := "SELECT sql FROM __genji_tables"
	if len(tables) > 0 {
		query += " WHERE table_name IN ?"
	}

	res, err := tx.Query(query, tables)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(d document.Document) error {
		// Get table name.
		var query string
		if err := document.Scan(d, &query); err != nil {
			return err
		}

		q, err := parser.ParseQuery(query)
		if err != nil {
			return err
		}

		return fn(query, &q.Statements[0].(*statement.CreateTableStmt).Info)
	})
}

// dumpTable displays the content of the given table as SQL statements.
func dumpTable(tx *genji.Tx, w io.Writer, query, tableName string) error {
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

	i := 0
	return queryTables(tx, tables, func(query string, ti *database.TableInfo) error {
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
		}
		i++

		return dumpSchema(tx, w, query, ti.TableName)
	})
}

// dumpSchema displays the schema of the given table as SQL statements.
func dumpSchema(tx *genji.Tx, w io.Writer, query string, tableName string) error {
	_, err := fmt.Fprintf(w, "%s;\n", query)
	if err != nil {
		return err
	}

	// Indexes statements.
	res, err := tx.Query("SELECT sql FROM __genji_indexes WHERE sql LIKE ?", "%ON "+tableName+" %")
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
		q := "SELECT sql FROM __genji_indexes"
		var param string
		if tableName != "" {
			q += " WHERE sql LIKE ?"
			param = "%ON " + tableName + " %"
		}
		res, err := tx.Query(q, param)
		if err != nil {
			return err
		}
		defer res.Close()

		return res.Iterate(func(d document.Document) error {
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

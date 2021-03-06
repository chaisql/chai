package dbutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

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

		return dumpTable(tx, tableName, w)
	})
	if err != nil {
		_, er := fmt.Fprintln(w, "ROLLBACK;")
		return multierr.Append(err, er)
	}

	_, err = fmt.Fprintln(w, "COMMIT;")
	return err
}

// dumpTable displays the content of the given table as SQL statements.
func dumpTable(tx *genji.Tx, tableName string, w io.Writer) error {
	var buf bytes.Buffer

	t, err := tx.GetTable(tableName)
	if err != nil {
		return err
	}

	if _, err = fmt.Fprintf(w, "CREATE TABLE %s", t.Name()); err != nil {
		return err
	}

	ti := t.Info()

	fcs := ti.FieldConstraints
	// Fields constraints should be displayed between parenthesis.
	if len(fcs) > 0 {
		buf.WriteString(" (\n")
	}

	for i, fc := range fcs {
		// Don't display the last comma.
		if i > 0 {
			buf.WriteString(",\n")
		}

		buf.WriteString(" " + fcs[i].Path.String() + " ")
		buf.WriteString(strings.ToUpper(fcs[i].Type.String()))
		if fc.IsPrimaryKey {
			buf.WriteString(" PRIMARY KEY")
		}

		if fc.IsNotNull {
			buf.WriteString(" NOT NULL")
		}

		if fc.HasDefaultValue() {
			buf.WriteString(" DEFAULT ")
			buf.WriteString(fc.DefaultValue.String())
		}
	}

	// Fields constraints close parenthesis.
	if len(fcs) > 0 {
		buf.WriteString("\n);\n")
	} else {
		buf.WriteString(";\n")
	}

	// Print CREATE TABLE statement.
	if _, err = buf.WriteTo(w); err != nil {
		return err
	}

	// Indexes statements.
	indexes := t.Indexes()

	for _, index := range indexes {
		u := ""
		if index.Info.Unique {
			u = " UNIQUE"
		}

		_, err = fmt.Fprintf(w, "CREATE%s INDEX %s ON %s (%s);\n", u, index.Info.IndexName, index.Info.TableName,
			index.Info.Path)
		if err != nil {
			return err
		}
	}

	q := fmt.Sprintf("SELECT * FROM %s", t.Name())
	res, err := tx.Query(q)
	if err != nil {
		return err
	}
	defer res.Close()

	// Inserts statements.
	insert := fmt.Sprintf("INSERT INTO %s VALUES ", t.Name())
	return res.Iterate(func(d document.Document) error {
		buf.WriteString(insert)

		data, err := document.MarshalJSON(d)
		if err != nil {
			return err
		}
		buf.Write(data)
		buf.WriteString(";\n")
		_, err = buf.WriteTo(w)
		return err
	})
}

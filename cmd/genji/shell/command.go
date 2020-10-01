package shell

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/genjidb/genji/database"

	"github.com/agnivade/levenshtein"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
)

var commands = []struct {
	Name        string
	Options     string
	Description string
}{
	{".exit", ``, "Exit this program."},
	{".help", ``, "List all commands."},
	{".tables", ``, "List names of tables."},
	{".indexes", `[table_name]`, "Display all indexes or the indexes of the given table name."},
	{".dump", `[table_name]`, "Dump database content or table content as SQL statements"},
}

// runTablesCmd shows all tables.
func runTablesCmd(db *genji.DB, cmd []string) error {
	if len(cmd) > 1 {
		return fmt.Errorf("usage: .tables")
	}

	res, err := db.Query("SELECT table_name FROM __genji_tables")
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(d document.Document) error {
		var tableName string
		err = document.Scan(d, &tableName)
		if err != nil {
			return err
		}
		fmt.Println(tableName)
		return nil
	})
}

// displayTableIndex prints all indexes that the given table contains.
func displayTableIndex(db *genji.DB, tableName string) error {
	err := db.View(func(tx *genji.Tx) error {
		t, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		indexes, err := t.Indexes()
		if err != nil {
			return err
		}

		for _, idx := range indexes {
			fmt.Printf("%s on %s(%s)\n", idx.Opts.IndexName, idx.Opts.TableName, idx.Opts.Path)
		}

		return nil
	})

	return err
}

// displayAllIndexes shows all indexes that the database contains.
func displayAllIndexes(db *genji.DB) error {
	err := db.View(func(tx *genji.Tx) error {
		indexes, err := tx.ListIndexes()
		if err != nil {
			return err
		}

		for _, idx := range indexes {
			fmt.Printf("%s on %s(%s)\n", idx.IndexName, idx.TableName, idx.Path)
		}

		return nil
	})

	return err
}

// runIndexesCmd executes all indexes of the database or all indexes of the given table.
func runIndexesCmd(db *genji.DB, in []string) error {
	switch len(in) {
	case 1:
		// If the input is ".indexes"
		return displayAllIndexes(db)
	case 2:
		// If the input is ".indexes <tableName>"
		return displayTableIndex(db, in[1])
	}

	return fmt.Errorf("usage: .indexes [tablename]")
}

// runHelpCmd shows all available commands.
func runHelpCmd() error {
	for _, c := range commands {
		// spaces indentation for readability.
		spaces := 25
		indent := spaces - len(c.Name) - len(c.Options)
		fmt.Printf("%s %s %*s %s\n", c.Name, c.Options, indent, "", c.Description)
	}

	return nil
}

// displaySuggestions shows suggestions.
func displaySuggestions(in string) error {
	var suggestions []string
	for _, c := range commands {
		d := levenshtein.ComputeDistance(c.Name, in)
		// input should be at least half the command size to get a suggestion.
		if d < (len(c.Name) / 2) {
			suggestions = append(suggestions, c.Name)
		}
	}

	if len(suggestions) == 0 {
		return fmt.Errorf("Unknown command %q. Enter \".help\" for help.", in)
	}

	fmt.Printf("\"%s\" is not a command. Did you mean: ", in)
	for i := range suggestions {
		if i > 0 {
			fmt.Printf(", ")
		}

		fmt.Printf("%q", suggestions[i])
	}

	fmt.Println()
	return nil
}

// dumpTable displays the content of the given table as SQL statements.
func dumpTable(tx *genji.Tx, tableName string, w io.Writer) error {
	var buf bytes.Buffer

	t, err := tx.GetTable(tableName)
	if err != nil {
		return err
	}

	c := fmt.Sprintf("CREATE TABLE %s", t.Name())
	buf.WriteString(c)

	ti, err := t.Info()
	if err != nil {
		return err
	}

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

		buf.WriteString("  " + fcs[i].Path.String() + " ")
		buf.WriteString(strings.ToUpper(fcs[i].Type.String()))
		if fc.IsPrimaryKey {
			buf.WriteString(" PRIMARY KEY")
		}

		if fc.IsNotNull {
			buf.WriteString(" NOT NULL")
		}
	}

	// Fields constraints close parenthesis.
	if len(fcs) > 0 {
		buf.WriteString("\n);")
	} else {
		buf.WriteString(";")
	}

	// Print CREATE TABLE statement.
	_, err = fmt.Fprintln(w, buf.String())
	if err != nil {
		return err
	}
	buf.Reset()

	// Indexes statement.
	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, index := range indexes {
		u := ""
		if index.Opts.Unique {
			u = " UNIQUE"
		}

		info := fmt.Sprintf("CREATE%s INDEX %s ON %s (%s);", u, index.Opts.IndexName, index.Opts.TableName,
			index.Opts.Path)
		_, err = fmt.Fprintln(w, info)
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
		buf.WriteString(";")

		if _, err = fmt.Fprintln(w, buf.String()); err != nil {
			return err
		}

		buf.Reset()

		return nil
	})
}

// runDumpCmd run .dump command where tables slice is the given tables or it can be empty that is consider all database.
func runDumpCmd(db *genji.DB, tables []string, w io.Writer) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = fmt.Fprintln(w, "BEGIN TRANSACTION;")
	if err != nil {
		return err
	}

	i := 0
	for _, table := range tables {
		err = dumpTable(tx, table, w)
		switch err {
		case nil:
			// Blank separation between tables.
			if i > 0 {
				fmt.Println()
			}
			i++

		case database.ErrTableNotFound: // If table doesn't exist we skip it.
			continue
		default:
			_, err = fmt.Fprintln(w, "ROLLBACK;")
			return err
		}
	}

	// tables slice argument is not empty, all args tables has been displayed.
	// If it is empty we should print the database content.
	if len(tables) > 0 {
		_, err = fmt.Fprintln(w, "COMMIT;")
		return err
	}

	// tables slice argument is empty.
	// Dump database content.
	res, err := tx.Query("SELECT table_name FROM __genji_tables")
	if err != nil {
		_, err = fmt.Fprintln(w, "ROLLBACK;")
		return err
	}
	defer res.Close()

	i = 0
	err = res.Iterate(func(d document.Document) error {
		// Blank separation between tables.
		if i > 0 {
			fmt.Println()
		}
		i++

		// Get table name.
		var tableName string
		if err := document.Scan(d, &tableName); err != nil {
			return err
		}

		if err := dumpTable(tx, tableName, w); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		_, err = fmt.Fprintln(w, "ROLLBACK;")
		return err
	}

	_, err = fmt.Fprintln(w, "ROLLBACK;")
	return err
}

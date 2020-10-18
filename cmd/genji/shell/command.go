package shell

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/agnivade/levenshtein"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
)

var commands = []struct {
	Name        string
	Options     string
	DisplayName string
	Description string
	Aliases     []string
}{
	{
		Name:        ".exit",
		DisplayName: ".exit or exit",
		Description: "Exit this program.",
		Aliases:     []string{"exit"},
	},
	{
		Name:        ".help",
		DisplayName: ".help or help",
		Description: "List all commands.",
		Aliases:     []string{"help"},
	},
	{
		Name:        ".tables",
		DisplayName: ".tables",
		Description: "List names of tables.",
	},
	{
		Name:        ".indexes",
		Options:     "[table_name]",
		DisplayName: ".indexes",
		Description: "Display all indexes or the indexes of the given table name.",
	},
	{
		Name:        ".dump",
		Options:     "[table_name]",
		DisplayName: ".dump",
		Description: "Dump database content or table content as SQL statements.",
	},
}

// runTablesCmd shows all tables.
func runTablesCmd(db *genji.DB, cmd []string) error {
	if len(cmd) > 1 {
		return fmt.Errorf("usage: .tables")
	}

	ctx := context.Background()

	res, err := db.Query(ctx, "SELECT table_name FROM __genji_tables")
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
	return db.View(func(tx *genji.Tx) error {
		ctx := context.Background()
		_, err := tx.QueryDocument(ctx, "SELECT table_name FROM __genji_tables WHERE table_name = ?", tableName)
		if err != nil {
			if err == database.ErrDocumentNotFound {
				return fmt.Errorf("%w: %q", database.ErrTableNotFound, tableName)
			}

			return err
		}

		res, err := tx.Query(ctx, "SELECT * FROM __genji_indexes WHERE table_name = ?", tableName)
		if err != nil {
			return err
		}
		defer res.Close()

		return res.Iterate(func(d document.Document) error {
			var index database.IndexConfig

			if err := index.ScanDocument(d); err != nil {
				return err
			}

			fmt.Printf("%s ON %s (%s)\n", index.IndexName, index.TableName, index.Path)

			return nil
		})
	})
}

// displayAllIndexes shows all indexes that the database contains.
func displayAllIndexes(db *genji.DB) error {
	ctx := context.Background()

	res, err := db.Query(ctx, "SELECT * FROM __genji_indexes")
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(d document.Document) error {
		var index database.IndexConfig

		if err := index.ScanDocument(d); err != nil {
			return err
		}

		fmt.Printf("%s ON %s (%s)\n", index.IndexName, index.TableName, index.Path)

		return nil
	})

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
		indent := spaces - len(c.DisplayName) - len(c.Options)
		fmt.Printf("%s %s %*s %s\n", c.DisplayName, c.Options, indent, "", c.Description)
	}

	return nil
}

func shouldDisplaySuggestion(name, in string) bool {
	// input should be at least half the command size to get a suggestion.
	d := levenshtein.ComputeDistance(name, in)
	return d < (len(name) / 2)
}

// displaySuggestions shows suggestions.
func displaySuggestions(in string) error {
	var suggestions []string
	for _, c := range commands {
		if shouldDisplaySuggestion(c.Name, in) {
			suggestions = append(suggestions, c.Name)
		}

		for _, alias := range c.Aliases {
			if shouldDisplaySuggestion(alias, in) {
				suggestions = append(suggestions, alias)
			}
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

	if _, err = fmt.Fprintf(w, "CREATE TABLE %s", t.Name()); err != nil {
		return err
	}

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
		buf.WriteString("\n);\n")
	} else {
		buf.WriteString(";\n")
	}

	// Print CREATE TABLE statement.
	if _, err = buf.WriteTo(w); err != nil {
		return err
	}
	buf.Reset()

	// Indexes statements.
	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, index := range indexes {
		u := ""
		if index.Opts.Unique {
			u = " UNIQUE"
		}

		_, err = fmt.Fprintf(w, "CREATE%s INDEX %s ON %s (%s);\n", u, index.Opts.IndexName, index.Opts.TableName,
			index.Opts.Path)
		if err != nil {
			return err
		}
	}

	q := fmt.Sprintf("SELECT * FROM %s", t.Name())
	res, err := tx.Query(context.Background(), q)
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

		if _, err = buf.WriteTo(w); err != nil {
			return err
		}

		buf.Reset()

		return nil
	})
}

// runDumpCmd dumps the given tables if provided, otherwise it dumps the whole database.
func runDumpCmd(db *genji.DB, tables []string, w io.Writer) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = fmt.Fprintln(w, "BEGIN TRANSACTION;"); err != nil {
		return err
	}

	for i, table := range tables {
		err = dumpTable(tx, table, w)
		if err != nil {
			// If table doesn’t exist we skip it.
			if errors.Is(err, database.ErrTableNotFound) {
				continue
			}
			_, err = fmt.Fprintln(w, "COMMIT;")
			return err
		}
		
		// Blank separation between tables.
		if i > 0 {
			if _, err := fmt.Fprintln(w, ""); err != nil {
				return err
			}
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
	res, err := tx.Query(context.Background(), "SELECT table_name FROM __genji_tables")
	if err != nil {
		_, err = fmt.Fprintln(w, "ROLLBACK;")
		return err
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
		_, err = fmt.Fprintln(w, "ROLLBACK;")
		return err
	}

	_, err = fmt.Fprintln(w, "COMMIT;")
	return err
}

package shell

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/genjidb/genji/cmd/genji/doc"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
)

type command struct {
	Name        string
	Options     string
	DisplayName string
	Description string
	Aliases     []string
}

func (c *command) Usage() string {
	return fmt.Sprintf("%s %s", c.DisplayName, c.Options)
}

var commands = []command{
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
	{
		Name:        ".doc",
		Options:     "[function_name]",
		DisplayName: ".doc",
		Description: "Display inline documentation for a function",
	},
	{
		Name:        ".save",
		Options:     "[badger] [filename]",
		DisplayName: ".save",
		Description: "Save database content in the specified file.",
	},
	{
		Name:        ".schema",
		Options:     "[table_name]",
		DisplayName: ".schema",
		Description: "Show the CREATE statements of all tables or of the selected ones.",
	},
	{
		Name:        ".import",
		Options:     "TYPE FILE table",
		DisplayName: ".import",
		Description: "Import data from a file. Only supported type is 'csv'",
	},
}

func getUsage(cmdName string) string {
	for _, c := range commands {
		if c.Name == cmdName {
			return c.Usage()
		}
	}

	return ""
}

// runHelpCmd shows all available commands.
func runHelpCmd() error {
	for _, c := range commands {
		// indentation for readability.
		spaces := 25
		indent := spaces - len(c.DisplayName) - len(c.Options)
		fmt.Printf("%s %s %*s %s\n", c.DisplayName, c.Options, indent, "", c.Description)
	}

	return nil
}

// runDocCommand prints the docstring for a given function
func runDocCmd(expr string) error {
	doc, err := doc.DocString(expr)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", doc)
	return nil
}

// runTablesCmd displays all tables.
func runTablesCmd(db *genji.DB, w io.Writer) error {
	res, err := db.Query("SELECT name FROM __genji_catalog WHERE type = 'table' AND name != '__genji_sequence'")
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
		_, err = fmt.Fprintln(w, tableName)
		return err
	})
}

// runIndexesCmd displays a list of indexes. If table is non-empty, it only
// display that table's indexes. If not, it displays all indexes.
func runIndexesCmd(db *genji.DB, tableName string, w io.Writer) error {
	// ensure table exists
	if tableName != "" {
		err := db.View(func(tx *genji.Tx) error {
			_, err := tx.QueryDocument("SELECT 1 FROM __genji_catalog WHERE table_name = ? LIMIT 1", tableName)
			if err != nil {
				if err == errs.ErrDocumentNotFound {
					return fmt.Errorf("%w: %q", errs.NotFoundError{Name: tableName}, tableName)
				}
			}
			return err
		})
		if err != nil {
			return err
		}
	}

	indexes, err := dbutil.ListIndexes(context.Background(), db, tableName)
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		_, err = fmt.Fprintln(w, idx)
		if err != nil {
			return err
		}
	}

	return nil
}

// runSaveCommand saves the currently opened database at the given path.
// If a path already exists, existing values in the target database will be overwritten.
func runSaveCmd(ctx context.Context, db *genji.DB, engineName string, dbPath string) error {
	// Open the new database
	otherDB, err := dbutil.OpenDB(ctx, dbPath, engineName, dbutil.DBOptions{})
	if err != nil {
		return err
	}
	otherDB = otherDB.WithContext(ctx)
	defer otherDB.Close()

	var dbDump bytes.Buffer

	err = dbutil.Dump(ctx, db, &dbDump)
	if err != nil {
		return err
	}

	return otherDB.Exec(dbDump.String())
}

func runImportCmd(ctx context.Context, db *genji.DB, fileType, path, table string) error {
	if strings.ToLower(fileType) != "csv" {
		return errors.New("TYPE should be csv")
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	r := csv.NewReader(f)

	err = tx.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", table))
	if err != nil {
		return err
	}

	headers, err := r.Read()
	if err != nil {
		return err
	}

	for {
		columns, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = tx.Exec("INSERT INTO "+table+" VALUES ?", document.NewFromCSV(headers, columns))
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

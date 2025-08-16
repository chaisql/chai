package shell

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/chaisql/chai/cmd/chai/dbutil"
	"github.com/chaisql/chai/internal/row"
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
		Name:        ".save",
		Options:     "[filename]",
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
	{
		Name:        ".timer",
		Options:     "[on|off]",
		DisplayName: ".timer",
		Description: "Display the execution time after each query or hide it.",
	},
	{
		Name:        ".restore",
		Options:     "[dumpFile]",
		DisplayName: ".restore",
		Description: "The restore command can restore a database from a text file.",
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
func runHelpCmd(out io.Writer) error {
	for _, c := range commands {
		// indentation for readability.
		spaces := 25
		indent := spaces - len(c.DisplayName) - len(c.Options)
		fmt.Fprintf(out, "%s %s %*s %s\n", c.DisplayName, c.Options, indent, "", c.Description)
	}

	return nil
}

// runTablesCmd displays all tables.
func runTablesCmd(ctx context.Context, db *sql.DB, w io.Writer) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "SELECT name FROM __chai_catalog WHERE type = 'table' AND name NOT LIKE '__chai_%'")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(w, tableName)
		if err != nil {
			return err
		}
	}

	return rows.Err()
}

// runIndexesCmd displays a list of indexes. If table is non-empty, it only
// displays that table's indexes. If not, it displays all indexes.
func runIndexesCmd(ctx context.Context, db *sql.DB, tableName string, w io.Writer) error {
	// ensure table exists
	if tableName != "" {
		err := db.QueryRowContext(ctx, "SELECT 1 FROM __chai_catalog WHERE name = ? AND type = 'table' LIMIT 1", tableName).Scan(new(int))
		if err != nil {
			if sql.ErrNoRows == err {
				return errors.Wrapf(err, "table %s does not exist", tableName)
			}
			return err
		}
	}

	indexes, err := dbutil.ListIndexes(ctx, db, tableName)
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
func runSaveCmd(ctx context.Context, db *sql.DB, dbPath string) error {
	// Open the new database
	otherDB, err := dbutil.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer otherDB.Close()

	var dbDump bytes.Buffer

	err = dbutil.Dump(ctx, db, &dbDump)
	if err != nil {
		return err
	}

	_, err = otherDB.Exec(dbDump.String())
	return err
}

const csvBatchSize = 1000

func runImportCmd(ctx context.Context, db *sql.DB, fileType, path, table string) error {
	if strings.ToLower(fileType) != "csv" {
		return errors.New("TYPE should be csv")
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

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

	r := csv.NewReader(f)

	headers, err := r.Read()
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, strings.Join(headers, " TEXT, ")))
	if err != nil {
		return err
	}

	baseQ := fmt.Sprintf("INSERT INTO %s VALUES ", table)

	buf := make([][]string, csvBatchSize)
	fbs := make([]*row.ColumnBuffer, csvBatchSize)
	for i := range fbs {
		fbs[i] = row.NewColumnBuffer()
	}
	args := make([]any, csvBatchSize)
	for i := range args {
		args[i] = fbs[i]
	}

	var sb strings.Builder
	var stop bool
	var stmt *sql.Stmt

	for !stop {
		sb.Reset()
		n, err := csvReadN(r, csvBatchSize, buf)
		if errors.Is(err, io.EOF) {
			stop = true
		} else if err != nil {
			return err
		}

		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			fbs[i].Reset()
			fbs[i].ScanCSV(headers, buf[i])
		}

		if stmt == nil || n < csvBatchSize {
			sb.WriteString(baseQ)
			for i := 0; i < n; i++ {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString("?")
			}

			stmt, err = tx.Prepare(sb.String())
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec(args[:n]...)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func csvReadN(r *csv.Reader, n int, dst [][]string) (int, error) {
	for i := 0; i < n; i++ {
		record, err := r.Read()
		if err != nil {
			return i, err
		}
		dst[i] = record
	}
	return n, nil
}

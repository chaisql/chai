package commands

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/urfave/cli/v2"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/dbutil"
)

// NewInsertCommand returns a cli.Command for "genji insert".
func NewInsertCommand() *cli.Command {
	return &cli.Command{
		Name:      "insert",
		Usage:     "Insert objects from arguments or standard input",
		UsageText: "genji insert [options] [json...]",
		Description: `
The insert command inserts objects into an existing table.

Insert can take JSON objects as separate arguments:

$ genji insert --db mydb -t foo '{"a": 1}' '{"a": 2}'

It is also possible to pass an array of objects:

$ genji insert --db mydb -t foo '[{"a": 1}, {"a": 2}]'

Also you can use -a flag to create database automatically.
This example will create a database with name 'data_${current unix timestamp}'
It can be combined with --db to select an existing database but automatically create the table.

$ genji insert -a '[{"a": 1}, {"a": 2}]'

Insert can also insert a stream of objects or an array of objects from standard input:

$ echo '{"a": 1} {"a": 2}' | genji insert --db mydb -t foo
$ echo '[{"a": 1},{"a": 2}]' | genji insert --db mydb -t foo
$ curl https://api.github.com/repos/genjidb/genji/issues | genji insert --db mydb -t foo`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "db",
				Usage:    "path of the database",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "table",
				Aliases:  []string{"t"},
				Usage:    "name of the table, it must already exist",
				Required: false,
			},
			&cli.BoolFlag{
				Name:     "auto",
				Aliases:  []string{"a"},
				Usage:    `automatically creates a database and a table whose name is equal to "data_" followed by the current unix timestamp.`,
				Required: false,
				Value:    false,
			},
		},
		Action: func(c *cli.Context) error {
			dbPath := c.String("db")
			table := c.String("table")
			args := c.Args().Slice()
			return runInsertCommand(c.Context, dbPath, table, c.Bool("auto"), args)
		},
	}
}

func runInsertCommand(ctx context.Context, dbPath, table string, auto bool, args []string) error {
	generatedName := "data_" + strconv.FormatInt(time.Now().Unix(), 10)
	createTable := false
	if table == "" && auto {
		table = generatedName
		createTable = true
	}

	if dbPath == "" && auto {
		dbPath = generatedName
	}

	db, err := dbutil.OpenDB(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	err = insert(db, table, createTable, args...)
	if err != nil {
		if createTable {
			_ = os.RemoveAll(dbPath)
		}

		return err
	}

	return nil
}

func insert(db *genji.DB, table string, createTable bool, args ...string) error {
	if createTable {
		err := db.Exec("CREATE TABLE " + table)
		if err != nil {
			return err
		}
	}

	if dbutil.CanReadFromStandardInput() {
		return dbutil.InsertJSON(db, table, os.Stdin)
	}

	if len(args) == 0 {
		return errors.New("no data to insert")
	}

	for _, arg := range args {
		if err := dbutil.InsertJSON(db, table, strings.NewReader(arg)); err != nil {
			return err
		}
	}

	return nil
}

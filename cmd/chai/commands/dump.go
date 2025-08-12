package commands

import (
	"context"
	"io"
	"os"

	"github.com/chaisql/chai/cmd/chai/dbutil"
	"github.com/cockroachdb/errors"
	"github.com/urfave/cli/v3"
)

// NewDumpCommand returns a cli.Command for "chai dump".
func NewDumpCommand() *cli.Command {
	cmd := cli.Command{
		Name:      "dump",
		Usage:     "Dump a database or a list of tables as a text file.",
		UsageText: `chai dump [options] dbpath`,
		Description: `The dump command can dump a database as a text file.

By default, the content of the database is sent to the standard output:

$ chai dump my.db
CREATE TABLE foo;
...

It is possible to specify a list of tables to output:

$ chai dump -t foo -f bar my.db

The dump command can also write directly into a file:

$ chai dump -f dump.sql my.db`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "file",
				Aliases: []string{"f"},
				Usage:   "name of the file to output to. Defaults to STDOUT.",
			},
			&cli.StringSliceFlag{
				Name:    "table",
				Aliases: []string{"t"},
				Usage:   "name of the table, it must already exist. Defaults to all tables.",
			},
		},
	}

	cmd.Action = func(ctx context.Context, cmd *cli.Command) error {
		tables := cmd.StringSlice("table")
		f := cmd.String("file")
		dbPath := cmd.Args().First()
		if dbPath == "" {
			return errors.New(cmd.UsageText)
		}

		db, err := dbutil.OpenDB(ctx, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		var w io.Writer = os.Stdout

		if f != "" {
			file, err := os.Create(f)
			if err != nil {
				return err
			}
			defer file.Close()

			w = file
		}

		return dbutil.Dump(db, w, tables...)
	}

	return &cmd
}

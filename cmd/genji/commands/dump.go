package commands

import (
	"errors"
	"io"
	"os"

	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/urfave/cli/v2"
)

// NewDumpCommand returns a cli.Command for "genji dump".
func NewDumpCommand() *cli.Command {
	cmd := cli.Command{
		Name:      "dump",
		Usage:     "Dump a database or a list of tables as a text file.",
		UsageText: `genji dump [options] dbpath`,
		Description: `The dump command can dump a database as a text file.

By default, the content of the database is sent to the standard output:

$ genji dump my.db
CREATE TABLE foo;
...

It is possible to specify a list of tables to output:

$ genji dump -t foo -f bar my.db

The dump command can also write directly into a file:

$ genji dump -f dump.sql my.db`,
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
			&cli.StringFlag{
				Name:    "engine",
				Aliases: []string{"e"},
				Usage:   "name of the engine to use, options are 'bolt' or 'badger'",
				Value:   "bolt",
			},
		},
	}

	cmd.Action = func(c *cli.Context) error {
		tables := c.StringSlice("table")
		f := c.String("file")
		engine := c.String("engine")
		dbPath := c.Args().First()
		if dbPath == "" {
			return errors.New(cmd.UsageText)
		}

		db, err := dbutil.OpenDB(c.Context, dbPath, engine)
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

		return dbutil.Dump(c.Context, db, w, tables...)
	}

	return &cmd
}

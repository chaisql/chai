package commands

import (
	"errors"

	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/urfave/cli/v2"
)

// NewBenchCommand returns a cli.Command for "genji bench".
func NewBenchCommand() *cli.Command {
	cmd := cli.Command{
		Name:      "bench",
		Usage:     "Simple load testing command",
		UsageText: `genji bench query`,
		Description: `The bench command runs a query repeatedly (100 times by default, -n option) and outputs a series of results.
Each result represent the average time for a given sample of queries (10 by default, -s/--sample option).

$ genji bench -n 200 -s 5 "SELECT 1"
{
	"totalQueries": 5,
	"sampleSpeed": "2.191µs"
}
{
	"totalQueries": 10,
	"sampleSpeed": "1.941µs"
}
{
	"totalQueries": 15,
	"sampleSpeed": "2.237µs"
}
...


By default, queries are run in-memory. To choose a different engine, use the -e/--engine and -p/--path options.
The database will be created if it doesn't exist.

$ genji bench -e bolt -p my.db "SELECT 1"
$ genji bench -e badger -p mydb/ "SELECT 1"

To prepare the database before running a query, use the -i/--init option

$ genji bench -p "CREATE TABLE foo; INSERT INTO foo(a) VALUES (1), (2), (3)" "SELECT * FROM foo"

By default, each query is run in a separate transaction. To run everything, including the setup,
in the same transaction, use -t`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "engine",
				Aliases: []string{"e"},
				Usage:   "name of the engine to use, options are 'bolt', 'badger' or 'memory'. Default to 'memory'",
				Value:   "memory",
			},
			&cli.StringFlag{
				Name:    "path",
				Aliases: []string{"p"},
				Usage:   "Path of the database to open or create. Only valid if for bolt or badger engines",
			},
			&cli.StringFlag{
				Name:    "init",
				Aliases: []string{"i"},
				Usage:   "Queries to run to initialize the database before running the benchmark.",
			},
			&cli.BoolFlag{
				Name:    "tx",
				Aliases: []string{"x"},
				Usage:   "Run everything in the same transaction.",
			},
			&cli.IntFlag{
				Name:    "number",
				Aliases: []string{"n"},
				Value:   100,
				Usage:   "Total number of queries to run.",
			},
			&cli.IntFlag{
				Name:    "sample",
				Aliases: []string{"s"},
				Value:   10,
				Usage:   "Number of queries to use to determine the average speed of the query.",
			},
			&cli.BoolFlag{
				Name:  "prepare",
				Usage: "Prepare the query before running the benchmark",
			},
			&cli.BoolFlag{
				Name:  "csv",
				Usage: "Output the results in csv",
			},
		},
	}

	cmd.Action = func(c *cli.Context) error {
		query := c.Args().First()
		if query == "" {
			return errors.New(cmd.UsageText)
		}

		engine := c.String("engine")
		path := c.String("path")
		if engine == "" {
			return errors.New(cmd.UsageText)
		}

		db, err := dbutil.OpenDB(c.Context, path, engine, dbutil.DBOptions{})
		if err != nil {
			return err
		}
		defer db.Close()

		return dbutil.Bench(c.Context, db, query, dbutil.BenchOptions{
			Init:       c.String("init"),
			N:          c.Int("number"),
			SampleSize: c.Int("sample"),
			SameTx:     c.Bool("tx"),
			Prepare:    c.Bool("prepare"),
			CSV:        c.Bool("csv"),
		})
	}

	return &cmd
}

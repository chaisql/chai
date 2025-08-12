package commands

import (
	"context"
	"errors"

	"github.com/chaisql/chai/cmd/chai/dbutil"
	"github.com/urfave/cli/v3"
)

// NewBenchCommand returns a cli.Command for "chai bench".
func NewBenchCommand() *cli.Command {
	cmd := cli.Command{
		Name:      "bench",
		Usage:     "Simple load testing command",
		UsageText: `chai bench query`,
		Description: `The bench command runs a query repeatedly (100 times by default, -n option) and outputs a series of results.
Each result represent the average time for a given sample of queries (10 by default, -s/--sample option).

$ chai bench -n 200 -s 5 "SELECT 1"
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


By default, queries are run in-memory. To write them on disk, use the -p/--path options.
The database will be created if it doesn't exist.

$ chai bench -p mydb/ "SELECT 1"

To prepare the database before running a query, use the -i/--init option

$ chai bench -p "CREATE TABLE foo; INSERT INTO foo(a) VALUES (1), (2), (3)" "SELECT * FROM foo"

By default, each query is run in a separate transaction. To run everything, including the setup,
in the same transaction, use -t`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "path",
				Aliases: []string{"p"},
				Usage:   "Path of the database to open or create. If not specified, the database will be in-memory",
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

	cmd.Action = func(ctx context.Context, cmd *cli.Command) error {
		query := cmd.Args().First()
		if query == "" {
			return errors.New(cmd.UsageText)
		}

		path := cmd.String("path")

		db, err := dbutil.OpenDB(ctx, path)
		if err != nil {
			return err
		}
		defer db.Close()

		return dbutil.Bench(db, query, dbutil.BenchOptions{
			Init:       cmd.String("init"),
			N:          cmd.Int("number"),
			SampleSize: cmd.Int("sample"),
			SameTx:     cmd.Bool("tx"),
			Prepare:    cmd.Bool("prepare"),
			CSV:        cmd.Bool("csv"),
		})
	}

	return &cmd
}

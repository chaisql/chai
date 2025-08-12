package commands

import (
	"context"

	"github.com/chaisql/chai/cmd/chai/dbutil"
	"github.com/cockroachdb/errors"
	"github.com/urfave/cli/v3"
)

// NewRestoreCommand returns a cli.Command for "chai restore".
func NewRestoreCommand() (cmd *cli.Command) {
	return &cli.Command{
		Name:      "restore",
		Usage:     "Restore a database from a file created by chai dump",
		UsageText: `chai restore dumpFile dbPath`,
		Description: `The restore command can restore a database from a text file.

	$ chai restore dump.sql mydb`,
		Flags: []cli.Flag{},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			args := cmd.Args()
			if args.Len() != 2 {
				return errors.New(cmd.UsageText)
			}
			return dbutil.Restore(ctx, nil, args.First(), args.Get(args.Len()-1))
		},
	}
}

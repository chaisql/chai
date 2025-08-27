package commands

import (
	"context"
	"os"

	"github.com/chaisql/chai/cmd/chai/dbutil"
	"github.com/chaisql/chai/cmd/chai/shell"
	"github.com/urfave/cli/v3"
)

// NewApp creates the Chai CLI app.
func NewApp() *cli.Command {
	var cmd cli.Command
	cmd.Name = "chai"
	cmd.Usage = "Shell for the ChaiSQL database"
	cmd.EnableShellCompletion = true

	cmd.Commands = []*cli.Command{
		NewVersionCommand(),
		NewDumpCommand(),
		NewRestoreCommand(),
		NewBenchCommand(),
		NewPebbleCommand(),
	}

	// Root command
	cmd.Action = func(ctx context.Context, cmd *cli.Command) error {
		dbpath := cmd.Args().First()

		if dbutil.CanReadFromStandardInput() {
			db, err := dbutil.OpenDB(dbpath)
			if err != nil {
				return err
			}
			defer db.Close()

			return dbutil.ExecSQL(ctx, db, os.Stdin, os.Stdout)
		}

		return shell.Run(ctx, &shell.Options{
			DBPath: dbpath,
		})
	}

	return &cmd
}

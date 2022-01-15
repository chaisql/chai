package commands

import (
	"os"

	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/genjidb/genji/cmd/genji/shell"
	"github.com/urfave/cli/v2"
)

// NewApp creates the Genji CLI app.
func NewApp() *cli.App {
	app := cli.NewApp()
	app.Name = "Genji"
	app.Usage = "Shell for the Genji database"
	app.EnableBashCompletion = true
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "encryption-key",
			Aliases: []string{"k"},
			Usage:   "encryption key, badger only",
		},
	}

	app.Commands = []*cli.Command{
		NewInsertCommand(),
		NewVersionCommand(),
		NewDumpCommand(),
		NewRestoreCommand(),
		NewBenchCommand(),
	}

	// Root command
	app.Action = func(c *cli.Context) error {
		var opts dbutil.DBOptions

		dbpath := c.Args().First()

		k := c.String("encryption-key")
		if k != "" {
			opts.EncryptionKey = k
		}

		if dbutil.CanReadFromStandardInput() {
			db, err := dbutil.OpenDB(c.Context, dbpath, opts)
			if err != nil {
				return err
			}
			defer db.Close()

			return dbutil.ExecSQL(c.Context, db, os.Stdin, os.Stdout)
		}

		return shell.Run(c.Context, &shell.Options{
			DBPath:        dbpath,
			EncryptionKey: k,
		})
	}

	return app
}

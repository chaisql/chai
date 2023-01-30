package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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

	app.Commands = []*cli.Command{
		NewInsertCommand(),
		NewVersionCommand(),
		NewDumpCommand(),
		NewRestoreCommand(),
		NewBenchCommand(),
		NewPebbleCommand(),
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "encryption-key",
			Aliases: []string{"k"},
			Usage: `Encryption key to use to encrypt/decrypt the database.
			The key must be a 32, 48 or 64 bytes long hexadecimal string.`,
		},
	}

	// inject cancelable context to all commands (except the shell command)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()
		<-ch
	}()

	for i := range app.Commands {
		action := app.Commands[i].Action
		app.Commands[i].Action = func(c *cli.Context) error {
			c.Context = ctx
			return action(c)
		}
	}

	// Root command
	app.Action = func(c *cli.Context) error {
		dbpath := c.Args().First()

		if dbutil.CanReadFromStandardInput() {
			db, err := dbutil.OpenDB(c.Context, dbpath, c.String("encryption-key"))
			if err != nil {
				return err
			}
			defer db.Close()

			return dbutil.ExecSQL(c.Context, db, os.Stdin, os.Stdout)
		}

		return shell.Run(c.Context, &shell.Options{
			DBPath:        dbpath,
			EncryptionKey: c.String("encryption-key"),
		})
	}

	app.After = func(c *cli.Context) error {
		cancel()
		return nil
	}

	return app
}

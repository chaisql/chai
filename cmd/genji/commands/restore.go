package commands

import (
	"errors"
	"os"

	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/urfave/cli/v2"
)

// NewRestoreCommand returns a cli.Command for "genji restore".
func NewRestoreCommand() (cmd *cli.Command) {
	return &cli.Command{
		Name:      "restore",
		Usage:     "Restore a database from a file created by genji dump",
		UsageText: `genji restore dumpFile dbPath`,
		Description: `The restore command can restore a database from a text file.

	$ genji restore dump.sql my.db`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "engine",
				Aliases: []string{"e"},
				Usage:   "name of the engine to use, options are 'bolt' or 'badger'",
				Value:   "bolt",
			},
			&cli.StringFlag{
				Name:    "encryption-key",
				Aliases: []string{"k"},
				Usage:   "encryption key, badger only",
			},
		},
		Action: func(c *cli.Context) error {
			engine := c.String("engine")
			k := c.String("encryption-key")
			if k != "" && engine != "badger" {
				return cli.Exit("encryption key is only supported by the badger engine", 2)
			}

			if c.Args().Len() != 2 {
				return errors.New(cmd.UsageText)
			}
			dbPath := c.Args().Get(c.Args().Len() - 1)
			if dbPath == "" {
				return errors.New("database path expected")
			}

			f := c.Args().First()
			if f == "" {
				return errors.New("dump file expected")
			}

			file, err := os.Open(f)
			if err != nil {
				return err
			}
			defer file.Close()

			db, err := dbutil.OpenDB(c.Context, dbPath, engine, dbutil.DBOptions{EncryptionKey: k})
			if err != nil {
				return err
			}
			defer db.Close()

			return dbutil.ExecSQL(c.Context, db, file, os.Stdout)
		},
	}
}

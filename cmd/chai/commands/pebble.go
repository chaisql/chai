package commands

import (
	"github.com/chaisql/chai/cmd/chai/dbutil"
	"github.com/chaisql/chai/internal/kv"
	"github.com/urfave/cli/v2"
)

// NewPebbleCommand returns a cli.Command for "chai pebble".
func NewPebbleCommand() *cli.Command {
	cmd := cli.Command{
		Name:        "pebble",
		Usage:       "Outputs the content of the Pebble database",
		UsageText:   `chai pebble`,
		Description: `The pebble command simply outputs the content of the Pebble database in the standard output.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "path",
				Aliases: []string{"p"},
				Usage:   "Path of the database to open.",
			},
			&cli.BoolFlag{
				Name:    "keys-only",
				Aliases: []string{"k"},
				Usage:   "Only output the keys.",
			},
		},
	}

	cmd.Action = func(c *cli.Context) error {
		path := c.String("path")

		db, err := dbutil.OpenDB(c.Context, path)
		if err != nil {
			return err
		}
		defer db.Close()

		ng := db.DB.Engine.(*kv.PebbleEngine)
		return dbutil.DumpPebble(c.Context, ng.DB(), dbutil.DumpPebbleOptions{
			KeysOnly: c.Bool("keys-only"),
		})
	}

	return &cmd
}

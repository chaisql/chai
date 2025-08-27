package commands

import (
	"context"

	"github.com/chaisql/chai/cmd/chai/dbutil"
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/database/catalogstore"
	"github.com/chaisql/chai/internal/kv"
	"github.com/urfave/cli/v3"
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

	cmd.Action = func(ctx context.Context, cmd *cli.Command) error {
		path := cmd.String("path")

		db, err := database.Open(path, &database.Options{
			CatalogLoader: catalogstore.LoadCatalog,
		})
		if err != nil {
			return err
		}
		defer db.Close()

		ng := db.Engine.(*kv.PebbleEngine)
		return dbutil.DumpPebble(ctx, ng.DB(), dbutil.DumpPebbleOptions{
			KeysOnly: cmd.Bool("keys-only"),
		})
	}

	return &cmd
}

package main

import (
	"fmt"
	"os"

	"github.com/genjidb/genji/cmd/genji/shell"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()
	app.Name = "Genji"
	app.Usage = "Shell for the Genji database"
	app.EnableBashCompletion = true
	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:  "bolt",
			Usage: "use bolt engine",
		},
		&cli.BoolFlag{
			Name:  "badger",
			Usage: "use badger engine",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:      "insert",
			Usage:     "Insert documents from the command line",
			UsageText: "genji insert [options] [arguments...]",
			HideHelp:  true,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "engine",
					Aliases: []string{"e"},
					Usage:   "name of the engine to use, options are 'bolt' or 'badger'. defaults to bolt",
					Value:   "bolt",
				},
				&cli.StringFlag{
					Name:     "db",
					Usage:    "path of the database file",
					Required: true,
				},
				&cli.StringFlag{
					Name:     "table",
					Aliases:  []string{"t"},
					Usage:    "name of the table, it must already exist",
					Required: true,
				},
			},
			Action: func(c *cli.Context) error {
				dbPath := c.String("db")
				table := c.String("table")
				// Use bolt as default engine.
				engine := c.String("engine")
				args := c.Args().Slice()

				err := runInsertCommand(engine, dbPath, table, args)
				switch err {
				case ErrNoData:
					cli.ShowAppHelpAndExit(c, 2)
				case nil:
					break
				default:
					return err
				}

				return nil
			},

			OnUsageError: func(c *cli.Context, err error, isSubcommand bool) error {
				cli.ShowAppHelpAndExit(c, 2)
				return nil
			},
		},
	}

	// Root command
	app.Action = func(c *cli.Context) error {
		useBolt := c.Bool("bolt")
		useBadger := c.Bool("badger")
		if useBolt && useBadger {
			return cli.NewExitError("cannot use bolt and badger options at the same time", 2)
		}

		dbpath := c.Args().First()

		if (useBolt || useBadger) && dbpath == "" {
			return cli.NewExitError("db path required when using bolt or badger", 2)
		}

		engine := "memory"

		if useBolt || dbpath != "" {
			engine = "bolt"
		}

		if useBadger {
			engine = "badger"
		}

		return shell.Run(&shell.Options{
			Engine: engine,
			DBPath: dbpath,
		})
	}

	err := app.Run(os.Args)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "error: %v\n", err)
		os.Exit(2)
	}
}

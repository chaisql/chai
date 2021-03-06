package commands

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"
	"github.com/urfave/cli/v2"
)

// NewRestoreCommand returns a cli.Command for "genji restore".
func NewRestoreCommand() *cli.Command {
	return &cli.Command{
		Name:      "restore",
		Usage:     "Restore a database from a file created by genji dump",
		UsageText: `genji restore dumpFile dbPath`,
		Description: `The restore command can restore a database from a text file.

	$ genji restore dump.sql -e bolt my.db`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "engine",
				Aliases: []string{"e"},
				Usage:   "name of the engine to use, options are 'bolt' or 'badger'",
				Value:   "bolt",
			},
		},
		Action: func(c *cli.Context) error {
			engine := c.String("engine")
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

			db, err := dbutil.OpenDB(c.Context, dbPath, engine)
			if err != nil {
				return err
			}
			defer db.Close()

			return dbutil.ExecSQL(c.Context, db, file, os.Stdout)
		},
	}
}

func executeRestore(ctx context.Context, r io.Reader, e, dbPath string) error {
	var (
		ng  engine.Engine
		err error
	)

	switch e {
	case "bolt":
		ng, err = boltengine.NewEngine(dbPath, 0660, nil)
	case "badger":
		ng, err = badgerengine.NewEngine(badger.DefaultOptions(dbPath).WithLogger(nil))
	default:
		return fmt.Errorf(`engine should be "bolt" or "badger, got %q`, e)
	}
	if err != nil {
		return err
	}

	db, err := genji.New(ctx, ng)
	if err != nil {
		return err
	}
	db = db.WithContext(ctx)
	defer db.Close()

	scanner := bufio.NewScanner(r)

	// Every query ends with a semicolon.
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		for i := 0; i < len(data); i++ {
			if data[i] == ';' {
				return i + 1, data[:i], nil
			}
		}

		if !atEOF {
			return 0, nil, nil
		}

		return 0, data, bufio.ErrFinalToken
	})

	for scanner.Scan() {
		q := scanner.Text()
		if err := db.Exec(q); err != nil {
			_ = db.Exec("ROLLBACK")
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

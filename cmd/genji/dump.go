package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"

	"github.com/dgraph-io/badger/v2"
	"github.com/genjidb/genji/engine"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/shell"
)

func executeDump(ctx context.Context, f string, tables []string, e string, dbPath string, w io.Writer) error {
	if dbPath == "" {
		return errors.New("db path should be specified")
	}

	if f != "" {
		file, err := os.Create(f)
		if err != nil {
			return err
		}
		defer file.Close()

		// file as io.writer for the RunDumpCmd function.
		w = file
	}

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
		return fmt.Errorf(`engine should be "bolt" or "badger", got %q`, e)
	}
	if err != nil {
		return err
	}

	// dbPath cannot be empty it is checked in the main.
	db, err := genji.New(ctx, ng)
	if err != nil {
		return err
	}
	defer db.Close()

	return shell.RunDumpCmd(db.WithContext(ctx), tables, w)
}

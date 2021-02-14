package main

import (
	"context"
	"fmt"
	"io"

	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji/engine"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/shell"
)

func executeDump(ctx context.Context, w io.Writer, tables []string, e, dbPath string) error {
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

	db, err := genji.New(ctx, ng)
	if err != nil {
		return err
	}
	defer db.Close()

	return shell.RunDumpCmd(db.WithContext(ctx), w, tables)
}

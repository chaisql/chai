package main

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"
)

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

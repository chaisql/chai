package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"
)

func skipSpaces(r *bufio.Reader) (byte, error) {
	var c byte
	var err error

	for {
		c, err = r.ReadByte()
		if err != nil {
			return c, err
		}

		if c != '\n' && c != '\r' && c != ' ' && c != '\t' {
			break
		}
	}

	return c, nil
}

func executeInsertCommand(ctx context.Context, db *genji.DB, table string, r io.Reader) error {
	q := fmt.Sprintf("INSERT INTO %s VALUES ?", table)
	rd := bufio.NewReader(r)
	var c byte
	var err error

	// Ignore spaces.
	c, err = skipSpaces(rd)
	if err != nil {
		return err
	}
	switch c {
	case '{': // Json stream
		if err := rd.UnreadByte(); err != nil {
			return err
		}

		dec := json.NewDecoder(rd)
		for {
			var fb document.FieldBuffer
			err := dec.Decode(&fb)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			if err := db.Exec(ctx, q, &fb); err != nil {
				return err
			}
		}

	case '[': // Array of json objects
		if err := rd.UnreadByte(); err != nil {
			return err
		}

		dec := json.NewDecoder(rd)
		t, err := dec.Token()
		if err != nil {
			return err
		}

		for dec.More() {
			var fb document.FieldBuffer
			err := dec.Decode(&fb)
			if err != nil && err != io.EOF {
				return err
			}

			if err := db.Exec(ctx, q, &fb); err != nil {
				return err
			}
		}

		t, err = dec.Token()
		if err != nil {
			return err
		}
		d, ok := t.(json.Delim)
		if ok && d.String() != "]" {
			return fmt.Errorf("found %q, but expected ']'", c)
		}

	default:
		return fmt.Errorf("found %q, but expected '{' or '['", c)
	}

	return nil
}

func runInsertCommand(ctx context.Context, e, dbPath, table string, auto bool, args []string) error {
	var ng engine.Engine
	var err error

	generatedName := "data_" + strconv.FormatInt(time.Now().Unix(), 10)
	createTable := false
	if table == "" && auto {
		table = generatedName
		createTable = true
	}

	switch e {
	case "bolt":
		if dbPath == "" && auto {
			dbPath = generatedName + ".db"
		}
		ng, err = boltengine.NewEngine(dbPath, 0660, nil)
	case "badger":
		if dbPath == "" && auto {
			dbPath = generatedName
		}
		ng, err = badgerengine.NewEngine(badger.DefaultOptions(dbPath).WithLogger(nil))
	}
	if err != nil {
		return err
	}

	db, err := genji.New(ng)
	if err != nil {
		return err
	}
	defer db.Close()

	if createTable {
		err := db.Exec(ctx, "CREATE TABLE "+table)
		if err != nil {
			return err
		}
	}

	fi, _ := os.Stdin.Stat()
	m := fi.Mode()
	if (m & os.ModeNamedPipe) != 0 {
		return executeInsertCommand(ctx, db, table, os.Stdin)
	}

	if len(args) == 0 {
		return errors.New("no data to insert")
	}

	for _, arg := range args {
		if err := executeInsertCommand(ctx, db, table, strings.NewReader(arg)); err != nil {
			return err
		}
	}

	return nil
}

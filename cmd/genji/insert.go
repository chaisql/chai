package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"
	"io"
	"os"
	"strings"
)

var ErrNoData = errors.New("no data to insert")

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

func executeInsertCommand(db *genji.DB, table string, r io.Reader) error {
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

			if err := db.Exec(q, &fb); err != nil {
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

			if err := db.Exec(q, &fb); err != nil {
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

func runInsertCommand(e, DBPath, table string, args []string) error {
	var ng engine.Engine
	var err error

	switch e {
	case "bolt":
		ng, err = boltengine.NewEngine(DBPath, 0660, nil)
	case "badger":
		ng, err = badgerengine.NewEngine(badger.DefaultOptions(DBPath).WithLogger(nil))
	}
	if err != nil {
		return err
	}

	db, err := genji.New(ng)
	if err != nil {
		return err
	}

	defer db.Close()

	fi, _ := os.Stdin.Stat()
	m := fi.Mode()
	// Insert command is given in the pipe
	if (m & os.ModeNamedPipe) != 0 {
		return executeInsertCommand(db, table, os.Stdin)
	}

	if len(args) == 0 {
		return ErrNoData
	}

	for _, arg := range args {
		if err := executeInsertCommand(db, table, strings.NewReader(arg)); err != nil {
			return err
		}
	}

	return nil
}

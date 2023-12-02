package dbutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/object"
)

// InsertJSON reads json objects from r and inserts them into the selected table.
// The reader can be either a stream of json objects or an array of objects.
func InsertJSON(db *chai.DB, table string, r io.Reader) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	q := fmt.Sprintf("INSERT INTO %s VALUES ?", table)
	rd := bufio.NewReader(r)

	// read first non-white space byte to determine
	// whether we are reading from a json stream or
	// an array of json objects.
	c, err := readByteIgnoreWhitespace(rd)
	if err != nil {
		return err
	}
	switch c {
	case '{': // json stream
		if err := rd.UnreadByte(); err != nil {
			return err
		}

		dec := json.NewDecoder(rd)
		for {
			var fb object.FieldBuffer
			err := dec.Decode(&fb)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}

			if err := tx.Exec(q, &fb); err != nil {
				return err
			}
		}

	case '[': // Array of json objects
		if err := rd.UnreadByte(); err != nil {
			return err
		}

		dec := json.NewDecoder(rd)
		_, err := dec.Token()
		if err != nil {
			return err
		}

		for dec.More() {
			var fb object.FieldBuffer
			err := dec.Decode(&fb)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}

			if err := tx.Exec(q, &fb); err != nil {
				return err
			}
		}

		t, err := dec.Token()
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

	return tx.Commit()
}

func readByteIgnoreWhitespace(r *bufio.Reader) (byte, error) {
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

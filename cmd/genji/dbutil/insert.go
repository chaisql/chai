package dbutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
)

// InsertJSON reads json documents from r and inserts them into the selected table.
// The reader can be either a stream of json objects or an array of objects.
func InsertJSON(db *genji.DB, table string, r io.Reader) error {
	q := fmt.Sprintf("INSERT INTO %s VALUES ?", table)
	rd := bufio.NewReader(r)
	var c byte
	var err error

	// read first non white space byte to determine
	// whether we are reading from a json stream or
	// an array of json objects.
	c, err = readByteIgnoreWhitespace(rd)
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

package table

import (
	"bufio"
	"fmt"
	"io"

	"github.com/asdine/genji/record"
	"github.com/pkg/errors"
)

// Errors.
var (
	// ErrRecordNotFound is returned when no record is associated with the provided key.
	ErrRecordNotFound = errors.New("not found")
	// ErrDuplicate is returned when another record is already associated with a given key, primary key,
	// or if there is a unique index violation.
	ErrDuplicate = errors.New("duplicate")
)

// A Table represents a collection of records.
type Table interface {
	Reader
	RecordGetter
	Writer
}

// A Reader can read data from a table.
type Reader interface {
	// Iterate goes through all the records of the table and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(func(r record.Record) error) error
}

// NewReaderFromRecords creates a reader that will iterate over
// the given records.
func NewReaderFromRecords(records ...record.Record) Reader {
	return recordsReader(records)
}

type recordsReader []record.Record

func (rr recordsReader) Iterate(fn func(r record.Record) error) error {
	var err error

	for _, r := range rr {
		err = fn(r)
		if err != nil {
			return err
		}
	}

	return nil
}

// A RecordGetter is a type that allows to get one record by key.
// It is usually implemented by tables that provide random access.
type RecordGetter interface {
	// GetRecord returns one record by key.
	GetRecord(key []byte) (record.Record, error)
}

// A Writer can manipulate a table.
type Writer interface {
	// Insert a record into the table and returns its key.
	Insert(record.Record) (key []byte, err error)
	// Delete a record by key. If the record is not found, returns ErrRecordNotFound.
	Delete(key []byte) error
	// Replace a record by another one. If the record is not found, returns ErrRecordNotFound.
	Replace(key []byte, r record.Record) error
	// Truncate deletes all the records from the table.
	Truncate() error
}

// A PrimaryKeyer is a record that generates a key based on its primary key.
type PrimaryKeyer interface {
	PrimaryKey() ([]byte, error)
}

// A Scanner is a type that can read all the records of a table reader.
type Scanner interface {
	ScanTable(Reader) error
}

// Dump table information to w, structured as a csv .
func Dump(w io.Writer, t Reader) error {
	buf := bufio.NewWriter(w)

	err := t.Iterate(func(r record.Record) error {
		first := true
		err := r.Iterate(func(f record.Field) error {
			if !first {
				buf.WriteString(", ")
			}
			first = false

			v, err := f.Decode()

			fmt.Fprintf(buf, "%s(%s): %#v", f.Name, f.Type, v)
			return err
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(buf, "\n")
		return nil
	})
	if err != nil {
		return err
	}

	return buf.Flush()
}

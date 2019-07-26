package table

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	b "github.com/asdine/genji/table/internal"
)

// Errors.
var (
	ErrRecordNotFound = errors.New("not found")
)

// A Table represents a collection of records.
type Table interface {
	Reader
	Writer
}

// A Reader can read data from a table.
type Reader interface {
	// Iterate goes through all the records of the table and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(func(recordID []byte, r record.Record) error) error
	// Record returns one record by recordID.
	Record(recordID []byte) (record.Record, error)
}

// A Writer can manipulate a table.
type Writer interface {
	// Insert a record into the table and returns its recordID.
	Insert(record.Record) (recordID []byte, err error)
	// Delete a record by recordID. If the record is not found, returns ErrRecordNotFound.
	Delete(recordID []byte) error
	// Replace a record by another one. If the record is not found, returns ErrRecordNotFound.
	Replace(recordID []byte, r record.Record) error
	// Truncate deletes all the records from the table.
	Truncate() error
}

// A Pker is a record that generates a recordID based on its primary key.
type Pker interface {
	Pk() ([]byte, error)
}

// A Scanner is a type that can read all the records of a table reader.
type Scanner interface {
	ScanTable(Reader) error
}

// RecordBuffer is table that stores records in memory in a B+Tree. It implements the Table interface.
type RecordBuffer struct {
	tree    *b.Tree
	counter int64
}

// Insert adds a record to the buffer.
func (rb *RecordBuffer) Insert(r record.Record) (recordID []byte, err error) {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	if pker, ok := r.(Pker); ok {
		recordID, err = pker.Pk()
		if err != nil {
			return nil, err
		}
		if len(recordID) == 0 {
			return nil, errors.New("empty pk")
		}
	} else {
		recordID = field.EncodeInt64(atomic.AddInt64(&rb.counter, 1))
	}

	rb.tree.Set(recordID, r)

	return recordID, nil
}

// ScanTable copies all the records of t to the buffer.
func (rb *RecordBuffer) ScanTable(t Reader) error {
	return t.Iterate(func(recordID []byte, r record.Record) error {
		_, err := rb.Insert(r)
		return err
	})
}

// Record returns a record by recordID. If the record is not found, returns ErrRecordNotFound.
func (rb *RecordBuffer) Record(recordID []byte) (record.Record, error) {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	r, ok := rb.tree.Get(recordID)
	if !ok {
		return nil, ErrRecordNotFound
	}

	return r, nil
}

// Set replaces a record if it already exists or creates one if not.
func (rb *RecordBuffer) Set(recordID []byte, r record.Record) error {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	rb.tree.Set(recordID, r)
	return nil
}

// Delete a record by recordID. If the record is not found, returns ErrRecordNotFound.
func (rb *RecordBuffer) Delete(recordID []byte) error {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	ok := rb.tree.Delete(recordID)
	if !ok {
		return ErrRecordNotFound
	}

	return nil
}

// Iterate goes through all the records of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (rb *RecordBuffer) Iterate(fn func(recordID []byte, r record.Record) error) error {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	e, err := rb.tree.SeekFirst()
	if err == io.EOF {
		return nil
	}

	for k, r, err := e.Next(); err != io.EOF; k, r, err = e.Next() {
		if err := fn(k, r); err != nil {
			return err
		}
	}

	e.Close()
	return nil
}

// Replace a record by another one. If the record is not found, returns ErrRecordNotFound.
func (rb *RecordBuffer) Replace(recordID []byte, r record.Record) error {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	_, ok := rb.tree.Get(recordID)
	if !ok {
		return ErrRecordNotFound
	}

	rb.tree.Set(recordID, r)
	return nil
}

// Truncate deletes all the records from the table.
func (rb *RecordBuffer) Truncate() error {
	if rb.tree != nil {
		rb.tree.Clear()
	}

	return nil
}

type schemaer interface {
	Schema() (schema record.Schema, schemaful bool)
}

// Dump table information to w, structured as a csv .
func Dump(w io.Writer, t Reader) error {
	buf := bufio.NewWriter(w)

	var schema record.Schema
	var schemaful bool

	if s, ok := t.(schemaer); ok {
		schema, schemaful = s.Schema()
	}

	if schemaful {
		fmt.Fprintf(buf, "%s\n", schema.String())
	}

	err := t.Iterate(func(recordID []byte, r record.Record) error {
		first := true
		err := r.Iterate(func(f field.Field) error {
			if !first {
				buf.WriteString(", ")
			}
			first = false

			v, err := field.Decode(f)
			if schemaful {
				fmt.Fprintf(buf, "%#v", v)
			} else {
				fmt.Fprintf(buf, "%s(%s): %#v", f.Name, f.Type, v)
			}
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

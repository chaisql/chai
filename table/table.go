package table

import (
	"bytes"
	"errors"
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
	Iterate(func(rowid []byte, r record.Record) error) error
	// Record returns one record by rowid.
	Record(rowid []byte) (record.Record, error)
}

// A Writer can manipulate a table.
type Writer interface {
	// Insert a record into the table and returns its rowid.
	Insert(record.Record) (rowid []byte, err error)
	// Delete a record by rowid. If the record is not found, returns ErrRecordNotFound.
	Delete(rowid []byte) error
	// Replace a record by another one. If the record is not found, returns ErrRecordNotFound.
	Replace(rowid []byte, r record.Record) error
	// Truncate deletes all the records from the table.
	Truncate() error
}

// A Pker is a record that generates a rowid based on its primary key.
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
func (rb *RecordBuffer) Insert(r record.Record) (rowid []byte, err error) {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	if pker, ok := r.(Pker); ok {
		rowid, err = pker.Pk()
		if err != nil {
			return nil, err
		}
		if len(rowid) == 0 {
			return nil, errors.New("empty pk")
		}
	} else {
		rowid = field.EncodeInt64(atomic.AddInt64(&rb.counter, 1))
	}

	rb.tree.Set(rowid, r)

	return rowid, nil
}

// ScanTable copies all the records of t to the buffer.
func (rb *RecordBuffer) ScanTable(t Reader) error {
	return t.Iterate(func(rowid []byte, r record.Record) error {
		_, err := rb.Insert(r)
		return err
	})
}

// Record returns a record by rowid. If the record is not found, returns ErrRecordNotFound.
func (rb *RecordBuffer) Record(rowid []byte) (record.Record, error) {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	r, ok := rb.tree.Get(rowid)
	if !ok {
		return nil, ErrRecordNotFound
	}

	return r, nil
}

// Set replaces a record if it already exists or creates one if not.
func (rb *RecordBuffer) Set(rowid []byte, r record.Record) error {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	rb.tree.Set(rowid, r)
	return nil
}

// Delete a record by rowid. If the record is not found, returns ErrRecordNotFound.
func (rb *RecordBuffer) Delete(rowid []byte) error {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	ok := rb.tree.Delete(rowid)
	if !ok {
		return ErrRecordNotFound
	}

	return nil
}

// Iterate goes through all the records of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (rb *RecordBuffer) Iterate(fn func(rowid []byte, r record.Record) error) error {
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
func (rb *RecordBuffer) Replace(rowid []byte, r record.Record) error {
	if rb.tree == nil {
		rb.tree = b.TreeNew(bytes.Compare)
	}

	_, ok := rb.tree.Get(rowid)
	if !ok {
		return ErrRecordNotFound
	}

	rb.tree.Set(rowid, r)
	return nil
}

// Truncate deletes all the records from the table.
func (rb *RecordBuffer) Truncate() error {
	if rb.tree != nil {
		rb.tree.Clear()
	}

	return nil
}

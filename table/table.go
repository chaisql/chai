package table

import (
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
	record.Iterator
	RecordGetter
	Writer
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
	ScanTable(record.Iterator) error
}

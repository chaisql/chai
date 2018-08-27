package engine

import "github.com/asdine/genji/record"

// A Table represents a group of records.
type Table interface {
	Cursor() Cursor
	Insert(record.Record) (rowid []byte, err error)
}

// A Cursor iterates over the fields of a record.
type Cursor interface {
	// Next advances the cursor to the next record which will then be available
	// through the Record method. It returns false when the cursor stops.
	// If an error occurs during iteration, the Err method will return it.
	Next() bool

	// Err returns the error, if any, that was encountered during iteration.
	Err() error

	// Record returns the current record.
	Record() (record.Record, error)
}

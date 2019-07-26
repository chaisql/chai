// Package table defines interfaces, implementations and helpers to manipulate tables.
package table

import (
	"errors"
	"sort"

	"github.com/asdine/genji/record"
)

// A Browser provides functional style methods to read a table.
// A Browser is immutable and doesn't modify underlying tables, each method creates a new in memory table.
type Browser struct {
	Reader

	err error
}

// NewBrowser creates a Browser for the given reader.
func NewBrowser(t Reader) Browser {
	return Browser{
		Reader: t,
	}
}

// Err returns the current error.
// Browser methods don't return errors when they fail, instead they store the error
// in the browser for later verification. Every methods check if the error is empty before
// running, otherwise the method is skipped.
// Err must be checked at the end of a pipeline.
func (b Browser) Err() error {
	return b.err
}

// ForEach goes through every record of the reader until the end or until fn returns an error.
func (b Browser) ForEach(fn func(rowid []byte, r record.Record) error) Browser {
	if b.err != nil {
		return b
	}

	err := b.Iterate(func(rowid []byte, r record.Record) error {
		return fn(rowid, r)
	})

	if err != nil && b.err == nil {
		b.err = err
	}

	return b
}

// Filter goes through all the records, filter them using fn and returns a new table reader containing
// only the selected records.
// If fn returns true, the record is kept, otherwise it is skipped.
// If fn returns an error, Filter stops immediately.
func (b Browser) Filter(fn func(rowid []byte, r record.Record) (bool, error)) Browser {
	var rb RecordBuffer

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		ok, err := fn(rowid, r)
		if err != nil {
			return err
		}

		if ok {
			err = rb.Set(rowid, r)
		}

		return err
	})

	if b.err == nil || rb.tree != nil {
		b.Reader = &rb
	}

	return b
}

// Map goes through all the records, calls fn with each one of them and creates a new table reader containing the records returned by fn.
// If fn returns an error, Map stops immediately.
func (b Browser) Map(fn func(rowid []byte, r record.Record) (record.Record, error)) Browser {
	var rb RecordBuffer

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		r, err := fn(rowid, r)
		if err != nil {
			return err
		}

		return rb.Set(rowid, r)
	})

	if b.err == nil || rb.tree != nil {
		b.Reader = &rb
	}

	return b
}

// Limit creates a new table with maximum n records.
func (b Browser) Limit(n int) Browser {
	var rb RecordBuffer

	var i int
	errStop := errors.New("stop")

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		if i >= n {
			return errStop
		}

		i++
		return rb.Set(rowid, r)
	})

	if err := b.Err(); err != errStop && err != nil {
		return b
	}

	b.err = nil
	b.Reader = &rb

	return b
}

// Offset creates a new table with n records skipped.
func (b Browser) Offset(n int) Browser {
	var rb RecordBuffer

	var i int

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		if i < n {
			i++
			return nil
		}

		return rb.Set(rowid, r)
	})

	if b.err == nil {
		b.Reader = &rb
	}

	return b
}

// GroupBy goes through all the records and creates multiple table readers grouped by fieldName.
// All the records containing the same value for the given field are grouped in the same table reader.
func (b Browser) GroupBy(fieldName string) BrowserGroup {
	var g BrowserGroup

	if b.err != nil {
		g.err = b.err
		return g
	}

	m := make(map[string]*RecordBuffer)
	var values []string

	tr := b.ForEach(func(rowid []byte, r record.Record) error {
		f, err := r.Field(fieldName)
		if err != nil {
			return err
		}

		k := string(f.Data)
		tr, ok := m[k]
		if !ok {
			tr = new(RecordBuffer)
			m[k] = tr
			values = append(values, k)
		}

		return tr.Set(rowid, r)
	})

	if err := tr.Err(); err != nil {
		g.err = err
		return g
	}

	sort.Strings(values)

	for _, v := range values {
		g.Readers = append(g.Readers, NewBrowser(m[v]))
	}

	return g
}

// Chunk splits the table into multiple tables of size n.
func (b Browser) Chunk(n int) BrowserGroup {
	var g BrowserGroup

	i := 0
	var fb RecordBuffer
	b = b.ForEach(func(rowid []byte, r record.Record) error {
		if i%n == 0 {
			fb = RecordBuffer{}
			g.Readers = append(g.Readers, NewBrowser(&fb))
		}

		i++
		return fb.Set(rowid, r)
	})

	if b.err != nil {
		g.err = b.err
	}

	return g
}

// Count counts all the records of the table.
func (b Browser) Count() (int, error) {
	if b.err != nil {
		return 0, b.err
	}

	counter := 0
	b = b.ForEach(func(rowid []byte, r record.Record) error {
		counter++
		return nil
	})

	return counter, b.err
}

// First returns the first record of the table
func (b Browser) First() (record.Record, error) {
	if b.err != nil {
		return nil, b.err
	}

	errStop := errors.New("stop")
	var rec record.Record

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		rec = r
		return errStop
	})
	if err := b.Err(); err != errStop {
		return nil, err
	}

	return rec, nil
}

// A BrowserGroup manages a group of tables.
type BrowserGroup struct {
	Readers []Browser
	err     error
}

// Err returns the current error.
// BrowserGroup methods don't return errors when they fail, instead they store the error
// in the browser for later verification. Every methods check if the error is empty before
// running, otherwise the method is skipped.
// Err must be checked at the end of a pipeline.
func (g BrowserGroup) Err() error {
	return g.err
}

// Concat returns a table containing all the records of all the tables.
func (g BrowserGroup) Concat() Browser {
	var b Browser

	if g.err != nil {
		b.err = g.err
		return b
	}

	var fb RecordBuffer

	for _, r := range g.Readers {
		err := fb.ScanTable(r)
		if err != nil {
			b.err = err
			return b
		}
	}

	b.Reader = &fb
	return b
}

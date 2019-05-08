package genji

import (
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type Store struct {
	db        *DB
	tx        *Tx
	tableName string
	schema    *record.Schema
	indexes   []string
}

// NewStore creates a Store.
func NewStore(db *DB, tableName string, schema *record.Schema, indexes []string) *Store {
	return &Store{
		db:        db,
		tableName: tableName,
		schema:    schema,
		indexes:   indexes,
	}
}

// NewStoreWithTx creates a Store valid for the lifetime of the given transaction.
func NewStoreWithTx(tx *Tx, tableName string, schema *record.Schema, indexes []string) *Store {
	return &Store{
		tx:        tx,
		tableName: tableName,
		schema:    schema,
		indexes:   indexes,
	}
}

func (s *Store) Tx(writable bool, fn func(tx *Tx) error) error {
	tx := s.tx
	var err error

	if tx == nil {
		tx, err = s.db.Begin(writable)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	err = fn(tx)
	if err != nil {
		return err
	}

	if s.tx == nil && writable {
		return tx.Commit()
	}

	return nil
}

func (s *Store) View(fn func(tx *Tx) error) error {
	return s.Tx(false, fn)
}

func (s *Store) Update(fn func(tx *Tx) error) error {
	return s.Tx(true, fn)
}

func (s *Store) ViewTable(fn func(table.Table) error) error {
	return s.View(func(tx *Tx) error {
		tb, err := tx.Table(s.tableName)
		if err != nil {
			return err
		}

		return fn(tb)
	})
}

func (s *Store) UpdateTable(fn func(table.Table) error) error {
	return s.Update(func(tx *Tx) error {
		tb, err := tx.Table(s.tableName)
		if err != nil {
			return err
		}

		return fn(tb)
	})
}

// Init makes sure the table exists. No error is returned if the table already exists.
func (s *Store) Init() error {
	return s.Update(func(tx *Tx) error {
		var err error
		if s.schema != nil {
			err = tx.CreateTableWithSchema(s.tableName, s.schema)
		} else {
			err = tx.CreateTable(s.tableName)
		}
		if err == engine.ErrTableAlreadyExists {
			return nil
		}

		if err != nil && err != engine.ErrTableAlreadyExists {
			return err
		}

		if s.schema != nil {
			schema, err := tx.schemas.Get(s.tableName)
			if err != nil {
				return err
			}

			if !s.schema.Equal(schema) {
				return errors.New("schema mismatch")
			}
		}

		if s.indexes != nil {
			for _, fname := range s.indexes {
				_, err = tx.CreateIndex(s.tableName, fname)
				if err != nil && err != engine.ErrIndexAlreadyExists {
					return err
				}
			}
		}

		return nil
	})
}

// Insert a record in the table and return the primary key.
func (s *Store) Insert(r record.Record) (rowid []byte, err error) {
	err = s.UpdateTable(func(t table.Table) error {
		rowid, err = t.Insert(r)
		return err
	})
	return
}

// Get a record using its primary key.
func (s *Store) Get(rowid []byte, scanner record.Scanner) error {
	return s.ViewTable(func(t table.Table) error {
		rec, err := t.Record(rowid)
		if err != nil {
			return err
		}

		return scanner.ScanRecord(rec)
	})
}

// Delete a record using its primary key.
func (s *Store) Delete(rowid []byte) error {
	return s.UpdateTable(func(t table.Table) error {
		return t.Delete(rowid)
	})
}

// List records from the specified offset. If the limit is equal to -1, it returns all records after the selected offset.
func (s *Store) List(offset, limit int, fn func(rowid []byte, r record.Record) error) error {
	return s.ViewTable(func(t table.Table) error {
		var skipped, count int
		errStop := errors.New("stop")

		err := t.Iterate(func(rowid []byte, r record.Record) error {
			if skipped < offset {
				skipped++
				return nil
			}

			if count >= limit && limit != -1 {
				return errStop
			}

			count++
			return fn(rowid, r)
		})
		if err != errStop {
			return err
		}

		return nil
	})
}

// Replace a record by another one.
func (s *Store) Replace(rowid []byte, r record.Record) error {
	return s.UpdateTable(func(t table.Table) error {
		return t.Replace(rowid, r)
	})
}

// Package bolt implements a BoltDB engine.
package bolt

import (
	"os"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

const (
	separator       byte = 0x1F
	indexBucketName      = "__genji.index"
)

// Engine represents a BoltDB engine. Each table is stored in a dedicated bucket.
type Engine struct {
	DB *bolt.DB
}

// NewEngine creates a BoltDB engine. It takes the same argument as Bolt's Open function.
func NewEngine(path string, mode os.FileMode, opts *bolt.Options) (*Engine, error) {
	db, err := bolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}

	return &Engine{
		DB: db,
	}, nil
}

// Begin creates a transaction using Bolt's transaction API.
func (e *Engine) Begin(writable bool) (engine.Transaction, error) {
	tx, err := e.DB.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		tx:       tx,
		writable: writable,
	}, nil
}

// Close the engine and underlying Bolt database.
func (e *Engine) Close() error {
	return e.DB.Close()
}

// A Transaction uses Bolt's transactions.
type Transaction struct {
	tx       *bolt.Tx
	writable bool
}

// Rollback the transaction. Can be used safely after commit.
func (t *Transaction) Rollback() error {
	err := t.tx.Rollback()
	if err != nil && err != bolt.ErrTxClosed {
		return err
	}

	return nil
}

// Commit the transaction.
func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

// Table returns a table by name. The table uses a Bolt bucket.
func (t *Transaction) Table(name string, codec record.Codec) (table.Table, error) {
	bname := []byte(name)
	b := t.tx.Bucket(bname)
	if b == nil {
		return nil, engine.ErrTableNotFound
	}

	return &Table{
		bucket: b,
		codec:  codec,
		tx:     t.tx,
		name:   bname,
	}, nil
}

// CreateTable creates a bolt bucket and returns a table.
// If the table already exists, returns engine.ErrTableAlreadyExists.
func (t *Transaction) CreateTable(name string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := t.tx.CreateBucket([]byte(name))
	if err == bolt.ErrBucketExists {
		return engine.ErrTableAlreadyExists
	}

	return err
}

// DropTable deletes the underlying bucket.
func (t *Transaction) DropTable(name string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	err := t.tx.DeleteBucket([]byte(name))
	if err == bolt.ErrBucketNotFound {
		return engine.ErrTableNotFound
	}

	return err
}

// CreateIndex creates an index in a sub bucket of the table bucket.
func (t *Transaction) CreateIndex(table, fieldName string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	b := t.tx.Bucket([]byte(table))
	if b == nil {
		return engine.ErrTableNotFound
	}

	bb, err := b.CreateBucketIfNotExists([]byte(indexBucketName))
	if err != nil {
		return err
	}

	_, err = bb.CreateBucket([]byte(fieldName))
	if err == bolt.ErrBucketExists {
		return engine.ErrIndexAlreadyExists
	}

	return err
}

// Index returns an index by name.
func (t *Transaction) Index(table, fieldName string) (index.Index, error) {
	b := t.tx.Bucket([]byte(table))
	if b == nil {
		return nil, engine.ErrTableNotFound
	}

	bb := b.Bucket([]byte(indexBucketName))
	if bb == nil {
		return nil, engine.ErrIndexNotFound
	}

	ib := bb.Bucket([]byte(fieldName))
	if ib == nil {
		return nil, engine.ErrIndexNotFound
	}

	return &Index{
		b: ib,
	}, nil
}

// Indexes lists all the indexes of this table.
func (t *Transaction) Indexes(table string) (map[string]index.Index, error) {
	b := t.tx.Bucket([]byte(table))
	if b == nil {
		return nil, engine.ErrTableNotFound
	}

	m := make(map[string]index.Index)

	bb := b.Bucket([]byte(indexBucketName))
	if bb == nil {
		return nil, nil
	}

	err := bb.ForEach(func(k, _ []byte) error {
		m[string(k)] = &Index{
			b: bb.Bucket(k),
		}

		return nil
	})

	return m, err
}

// DropIndex drops an index by name, removing its corresponding bucket.
func (t *Transaction) DropIndex(table, fieldName string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	b := t.tx.Bucket([]byte(table))
	if b == nil {
		return engine.ErrTableNotFound
	}

	bb := b.Bucket([]byte(indexBucketName))
	if bb == nil {
		return engine.ErrIndexNotFound
	}

	err := bb.DeleteBucket([]byte(fieldName))
	if err == bolt.ErrBucketNotFound {
		return engine.ErrIndexNotFound
	}

	return err
}

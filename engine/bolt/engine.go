package bolt

import (
	"os"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

type Engine struct {
	DB *bolt.DB
}

func NewEngine(path string, mode os.FileMode, opts *bolt.Options) (*Engine, error) {
	db, err := bolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}

	return &Engine{
		DB: db,
	}, nil
}

func (e *Engine) Begin(writable bool) (engine.Transaction, error) {
	tx, err := e.DB.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		tx: tx,
	}, nil
}

func (e *Engine) Close() error {
	return e.DB.Close()
}

type Transaction struct {
	tx *bolt.Tx
}

func (t *Transaction) Rollback() error {
	return t.tx.Rollback()
}

func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

func (t *Transaction) Table(name string) (table.Table, error) {
	b := t.tx.Bucket([]byte(name))
	if b == nil {
		return nil, engine.ErrNotFound
	}

	return &Table{
		Bucket: b,
	}, nil
}

func (t *Transaction) CreateTable(name string) (table.Table, error) {
	b, err := t.tx.CreateBucket([]byte(name))
	if err != nil {
		return nil, err
	}

	return &Table{
		Bucket: b,
	}, nil
}

func (t *Transaction) CreateIndex(table, name string) (index.Index, error) {
	b := t.tx.Bucket([]byte(name))
	if b == nil {
		return nil, engine.ErrNotFound
	}

	ib, err := b.CreateBucket([]byte(name))
	if err != nil {
		return nil, err
	}

	return &Index{
		b: ib,
	}, nil
}

func (t *Transaction) Index(table, name string) (index.Index, error) {
	b := t.tx.Bucket([]byte(name))
	if b == nil {
		return nil, engine.ErrNotFound
	}

	ib := b.Bucket([]byte(name))
	if ib == nil {
		return nil, engine.ErrNotFound
	}

	return &Index{
		b: ib,
	}, nil
}

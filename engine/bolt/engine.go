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
		tx:       tx,
		writable: writable,
	}, nil
}

func (e *Engine) Close() error {
	return e.DB.Close()
}

type Transaction struct {
	tx       *bolt.Tx
	writable bool
}

func (t *Transaction) Rollback() error {
	err := t.tx.Rollback()
	if err != nil && err != bolt.ErrTxClosed {
		return err
	}

	return nil
}

func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

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

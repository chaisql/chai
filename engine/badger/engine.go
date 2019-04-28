package badger

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/asdine/genji/engine"
	"github.com/dgraph-io/badger"
)

const (
	separator     byte = 0x1F
	tableListName      = "__genji.table"
)

type Engine struct {
	DB *badger.DB
}

func NewEngine(opts badger.Options) (*Engine, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Engine{
		DB: db,
	}, nil
}

func (e *Engine) Begin(writable bool) (engine.Transaction, error) {
	_ = e.DB.NewTransaction(writable)

	return nil, nil
}

func (e *Engine) Close() error {
	return e.DB.Close()
}

type Transaction struct {
	tx       *badger.Txn
	writable bool
}

func (t *Transaction) Rollback() error {
	t.tx.Discard()

	return nil
}

func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

func (t *Transaction) CreateTable(name string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	if idx := strings.IndexByte(name, separator); idx != -1 {
		return fmt.Errorf("table name contains forbidden character at pos %d", idx)
	}

	var buf bytes.Buffer
	buf.Grow(len(tableListName) + 1 + len(name))
	buf.WriteString(tableListName)
	buf.WriteByte(separator)
	buf.WriteString(name)

	_, err := t.tx.Get(buf.Bytes())
	if err == nil {
		return engine.ErrTableAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return err
	}

	return t.tx.Set(buf.Bytes(), nil)
}

package badger

import (
	"github.com/asdine/genji/engine"
	"github.com/dgraph-io/badger"
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
	tx := e.DB.NewTransaction(writable)

	return &Transaction{
		tx:       tx,
		writable: writable,
	}, nil
}

func (e *Engine) Close() error {
	return e.DB.Close()
}

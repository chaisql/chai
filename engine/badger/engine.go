package badger

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/dgraph-io/badger"
)

const (
	separator     byte = 0x1F
	tableListName      = "__genji.table"
	tablePrefix        = 't'
	indexListName      = "__genji.index"
	indexPrefix        = 'i'
)

type Engine struct {
	DB   *badger.DB
	seqs []*badger.Sequence
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
	txn := e.DB.NewTransaction(writable)

	return &Transaction{
		ng:       e,
		txn:      txn,
		writable: writable,
	}, nil
}

func (e *Engine) Close() error {
	for _, seq := range e.seqs {
		_ = seq.Release()
	}

	return e.DB.Close()
}

type Transaction struct {
	ng       *Engine
	txn      *badger.Txn
	writable bool

	discarded bool
}

func (t *Transaction) Rollback() error {
	t.txn.Discard()

	t.discarded = true
	return nil
}

func (t *Transaction) Commit() error {
	if t.discarded {
		return badger.ErrDiscardedTxn
	}

	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	t.discarded = true
	return t.txn.Commit()
}

func (t *Transaction) CreateTable(name string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	if idx := strings.IndexByte(name, separator); idx != -1 {
		return fmt.Errorf("table name contains forbidden character at pos %d", idx)
	}

	key := makeTableKey(name)
	_, err := t.txn.Get(key)
	if err == nil {
		return engine.ErrTableAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return err
	}

	return t.txn.Set(key, nil)
}

func (t *Transaction) Table(name string, codec record.Codec) (table.Table, error) {
	key := makeTableKey(name)

	_, err := t.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, engine.ErrTableNotFound
		}

		return nil, err
	}

	pkey := makeTablePrefixKey(name)
	seq, err := t.ng.DB.GetSequence(pkey, 512)
	if err != nil {
		return nil, err
	}

	t.ng.seqs = append(t.ng.seqs, seq)

	return &Table{
		txn:      t.txn,
		prefix:   pkey,
		writable: t.writable,
		seq:      seq,
		codec:    codec,
	}, nil
}

func makeTableKey(name string) []byte {
	var buf bytes.Buffer
	buf.Grow(len(tableListName) + 1 + len(name))
	buf.WriteString(tableListName)
	buf.WriteByte(separator)
	buf.WriteString(name)

	return buf.Bytes()
}

func makeIndexKey(table, field string) []byte {
	var buf bytes.Buffer
	buf.Grow(len(indexListName) + 1 + len(table) + 1 + len(field))
	buf.WriteString(indexListName)
	buf.WriteByte(separator)
	buf.WriteString(table)
	buf.WriteByte(separator)
	buf.WriteString(field)

	return buf.Bytes()
}

func makeTablePrefixKey(name string) []byte {
	prefix := make([]byte, 0, len(name)+3)
	prefix = append(prefix, tablePrefix)
	prefix = append(prefix, separator)
	prefix = append(prefix, name...)
	prefix = append(prefix, separator)

	return prefix
}

func makeIndexPrefixKey(table, field string) []byte {
	prefix := make([]byte, 0, len(table)+len(field)+4)
	prefix = append(prefix, indexPrefix)
	prefix = append(prefix, separator)
	prefix = append(prefix, table...)
	prefix = append(prefix, separator)
	prefix = append(prefix, field...)
	prefix = append(prefix, separator)

	return prefix
}

func (t *Transaction) CreateIndex(table, fieldName string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	key := makeTableKey(table)

	_, err := t.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return engine.ErrTableNotFound
		}

		return err
	}

	if idx := strings.IndexByte(fieldName, separator); idx != -1 {
		return fmt.Errorf("index name contains forbidden character at pos %d", idx)
	}

	key = makeIndexKey(table, fieldName)
	_, err = t.txn.Get(key)
	if err == nil {
		return engine.ErrIndexAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return err
	}

	return t.txn.Set(key, nil)
}

func (t *Transaction) Index(table, fieldName string) (index.Index, error) {
	return nil, nil
}

func (t *Transaction) Indexes(table string) (map[string]index.Index, error) {
	return nil, nil
}

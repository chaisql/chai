package database

import (
	"errors"
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/index"
)

var (
	internalPrefix     = "__genji_"
	tableInfoStoreName = internalPrefix + "tables"
	indexStoreName     = internalPrefix + "indexes"
)

// Transaction represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Transaction is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Transaction struct {
	id       int64
	db       *Database
	Tx       engine.Transaction
	writable bool

	tableInfoStore *tableInfoStore
	indexStore     *indexStore
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Transaction) Rollback() error {
	if tx.writable {
		tx.tableInfoStore.rollback(tx)
	}
	return tx.Tx.Rollback()
}

// Commit the transaction.
func (tx *Transaction) Commit() error {
	if tx.writable {
		tx.tableInfoStore.commit(tx)
	}
	return tx.Tx.Commit()
}

// Writable indicates if the transaction is writable or not.
func (tx *Transaction) Writable() bool {
	return tx.writable
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx *Transaction) CreateTable(name string, info *TableInfo) error {
	if strings.HasPrefix(name, internalPrefix) {
		return fmt.Errorf("table name must not start with %s", internalPrefix)
	}

	if info == nil {
		info = new(TableInfo)
	}

	info.tableName = name
	info.storeID = tx.tableInfoStore.generateStoreID()
	err := tx.tableInfoStore.Insert(tx, name, info)
	if err != nil {
		return err
	}

	err = tx.Tx.CreateStore(info.storeID)
	if err != nil {
		return fmt.Errorf("failed to create table %q: %w", name, err)
	}

	return nil
}

// GetTable returns a table by name. The table instance is only valid for the lifetime of the transaction.
func (tx *Transaction) GetTable(name string) (*Table, error) {
	ti, err := tx.tableInfoStore.Get(tx, name)
	if err != nil {
		return nil, err
	}

	s, err := tx.Tx.GetStore(ti.storeID)
	if err != nil {
		return nil, err
	}

	return &Table{
		tx:        tx,
		Store:     s,
		name:      name,
		infoStore: tx.tableInfoStore,
	}, nil
}

// RenameTable renames a table.
// If it doesn't exist, it returns ErrTableNotFound.
func (tx *Transaction) RenameTable(oldName, newName string) error {
	ti, err := tx.tableInfoStore.Get(tx, oldName)
	if err != nil {
		return err
	}

	if ti.readOnly {
		return errors.New("cannot write to read-only table")
	}

	ti.tableName = newName
	// Insert the TableInfo keyed by the newName name.
	err = tx.tableInfoStore.Insert(tx, newName, ti)
	if err != nil {
		return err
	}

	// Update the indexes.
	idxs, err := tx.ListIndexes()
	if err != nil {
		return err
	}
	for _, idx := range idxs {
		if idx.TableName == oldName {
			idx.TableName = newName
			err = tx.indexStore.Replace(idx.IndexName, *idx)
			if err != nil {
				return err
			}
		}
	}

	// Delete the old reference from the tableInfoStore.
	return tx.tableInfoStore.Delete(tx, oldName)
}

// DropTable deletes a table from the database.
func (tx *Transaction) DropTable(name string) error {
	ti, err := tx.tableInfoStore.Get(tx, name)
	if err != nil {
		return err
	}

	if ti.readOnly {
		return errors.New("cannot write to read-only table")
	}

	it := tx.indexStore.st.NewIterator(engine.IteratorConfig{})

	var buf msgpack.EncodedDocument
	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		var opts IndexConfig
		buf, err = item.ValueCopy(buf)
		if err != nil {
			it.Close()
			return err
		}

		err = opts.ScanDocument(&buf)
		if err != nil {
			it.Close()
			return err
		}

		// Remove only indexes associated with the target table.
		if opts.TableName != name {
			continue
		}

		err = tx.DropIndex(opts.IndexName)
		if err != nil {
			it.Close()
			return err
		}
	}
	err = it.Close()
	if err != nil {
		return err
	}

	err = tx.tableInfoStore.Delete(tx, name)
	if err != nil {
		return err
	}

	return tx.Tx.DropStore(ti.storeID)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrIndexAlreadyExists.
func (tx *Transaction) CreateIndex(opts IndexConfig) error {
	_, err := tx.GetTable(opts.TableName)
	if err != nil {
		return err
	}

	return tx.indexStore.Insert(opts)
}

// GetIndex returns an index by name.
func (tx *Transaction) GetIndex(name string) (*Index, error) {
	opts, err := tx.indexStore.Get(name)
	if err != nil {
		return nil, err
	}

	var idx index.Index
	if opts.Unique {
		idx = index.NewUniqueIndex(tx.Tx, opts.IndexName)
	} else {
		idx = index.NewListIndex(tx.Tx, opts.IndexName)
	}

	return &Index{
		Index: idx,
		Opts:  *opts,
	}, nil
}

// DropIndex deletes an index from the database.
func (tx *Transaction) DropIndex(name string) error {
	opts, err := tx.indexStore.Get(name)
	if err != nil {
		return err
	}
	err = tx.indexStore.Delete(name)
	if err != nil {
		return err
	}

	var idx index.Index
	if opts.Unique {
		idx = index.NewUniqueIndex(tx.Tx, opts.IndexName)
	} else {
		idx = index.NewListIndex(tx.Tx, opts.IndexName)
	}

	return idx.Truncate()
}

// ListIndexes lists all indexes.
func (tx *Transaction) ListIndexes() ([]*IndexConfig, error) {
	return tx.indexStore.ListAll()
}

// ReIndex truncates and recreates selected index from scratch.
func (tx *Transaction) ReIndex(indexName string) error {
	idx, err := tx.GetIndex(indexName)
	if err != nil {
		return err
	}

	tb, err := tx.GetTable(idx.Opts.TableName)
	if err != nil {
		return err
	}

	err = idx.Truncate()
	if err != nil {
		return err
	}

	return tb.Iterate(func(d document.Document) error {
		v, err := idx.Opts.Path.GetValue(d)
		if err != nil {
			return err
		}

		return idx.Set(v, d.(document.Keyer).Key())
	})
}

// ReIndexAll truncates and recreates all indexes of the database from scratch.
func (tx *Transaction) ReIndexAll() error {
	var indexes []string

	it := tx.indexStore.st.NewIterator(engine.IteratorConfig{})
	for it.Seek(nil); it.Valid(); it.Next() {
		indexes = append(indexes, string(it.Item().Key()))
	}
	err := it.Close()
	if err != nil {
		return err
	}

	for _, indexName := range indexes {
		err = tx.ReIndex(indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *Transaction) getIndexStore() (*indexStore, error) {
	st, err := tx.Tx.GetStore([]byte(indexStoreName))
	if err != nil {
		return nil, err
	}
	return &indexStore{
		st: st,
	}, nil
}

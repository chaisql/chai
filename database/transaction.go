package database

import (
	"errors"
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
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
	db       *Database
	tx       engine.Transaction
	writable bool
	// if set to true, this transaction is attached to the database
	attached bool

	tableInfoStore *tableInfoStore
	indexStore     *indexStore
}

// DB returns the underlying database that created the transaction.
func (tx *Transaction) DB() *Database {
	return tx.db
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Transaction) Rollback() error {
	err := tx.tx.Rollback()
	if err != nil {
		return err
	}

	if tx.attached {
		tx.db.attachedTxMu.Lock()
		defer tx.db.attachedTxMu.Unlock()

		if tx.db.attachedTransaction != nil {
			tx.db.attachedTransaction = nil
		}
	}

	return nil
}

// Commit the transaction.
func (tx *Transaction) Commit() error {
	err := tx.tx.Commit()
	if err != nil {
		return err
	}

	if tx.attached {
		tx.db.attachedTxMu.Lock()
		defer tx.db.attachedTxMu.Unlock()

		if tx.db.attachedTransaction != nil {
			tx.db.attachedTransaction = nil
		}
	}

	return nil

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
	err := tx.tableInfoStore.Insert(tx, name, info)
	if err != nil {
		return err
	}

	err = tx.tx.CreateStore(info.storeName)
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

	s, err := tx.tx.GetStore(ti.storeName)
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

// AddField adds a field constraint to a table.
func (tx *Transaction) AddField(tableName string, fc FieldConstraint) error {
	info, err := tx.tableInfoStore.Get(tx, tableName)
	if err != nil {
		return err
	}

	for _, field := range info.FieldConstraints {
		if field.Path.IsEqual(fc.Path) {
			return fmt.Errorf("field %q already exists", fc.Path.String())
		}
		if field.IsPrimaryKey && fc.IsPrimaryKey {
			return fmt.Errorf(
				"multiple primary keys are not allowed (%q is primary key)",
				field.Path.String(),
			)
		}
	}

	info.FieldConstraints = append(info.FieldConstraints, fc)

	return tx.tableInfoStore.Replace(tx, tableName, info)
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

	it := tx.indexStore.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var buf []byte
	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		buf, err = item.ValueCopy(buf)
		if err != nil {
			return err
		}

		var opts IndexConfig
		err = opts.ScanDocument(tx.db.Codec.NewDocument(buf))
		if err != nil {
			return err
		}

		// Remove only indexes associated with the target table.
		if opts.TableName != name {
			continue
		}

		err = tx.DropIndex(opts.IndexName)
		if err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}

	err = tx.tableInfoStore.Delete(tx, name)
	if err != nil {
		return err
	}

	return tx.tx.DropStore(ti.storeName)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrIndexAlreadyExists.
func (tx *Transaction) CreateIndex(opts IndexConfig) error {
	t, err := tx.GetTable(opts.TableName)
	if err != nil {
		return err
	}

	info, err := t.Info()
	if err != nil {
		return err
	}

	// if the index is created on a field on which we know the type,
	// create a typed index.
	for _, fc := range info.FieldConstraints {
		if fc.Path.IsEqual(opts.Path) {
			if fc.Type != 0 {
				opts.Type = fc.Type
			}

			break
		}
	}

	return tx.indexStore.Insert(opts)
}

// GetIndex returns an index by name.
func (tx *Transaction) GetIndex(name string) (*Index, error) {
	opts, err := tx.indexStore.Get(name)
	if err != nil {
		return nil, err
	}

	idx := index.New(tx.tx, opts.IndexName, index.Options{
		Unique: opts.Unique,
		Type:   opts.Type,
	})

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

	idx := index.New(tx.tx, opts.IndexName, index.Options{
		Unique: opts.Unique,
		Type:   opts.Type,
	})

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
		v, err := idx.Opts.Path.GetValueFromDocument(d)
		if err == document.ErrFieldNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return idx.Set(v, d.(document.Keyer).RawKey())
	})
}

// allIndexNames returns a list of all index names in index store.
func (tx *Transaction) allIndexNames() ([]string, error) {
	it := tx.indexStore.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var indexes []string
	for it.Seek(nil); it.Valid(); it.Next() {
		indexes = append(indexes, string(it.Item().Key()))
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return indexes, nil
}

// ReIndexAll truncates and recreates all indexes of the database from scratch.
func (tx *Transaction) ReIndexAll() error {
	indexes, err := tx.allIndexNames()
	if err != nil {
		return err
	}

	for _, indexName := range indexes {
		err := tx.ReIndex(indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *Transaction) getTableInfoStore() (*tableInfoStore, error) {
	st, err := tx.tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return nil, err
	}
	return &tableInfoStore{
		st: st,
		db: tx.db,
	}, nil
}

func (tx *Transaction) getIndexStore() (*indexStore, error) {
	st, err := tx.tx.GetStore([]byte(indexStoreName))
	if err != nil {
		return nil, err
	}
	return &indexStore{
		st: st,
		db: tx.db,
	}, nil
}

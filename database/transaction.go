package database

import (
	"context"
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
	id       int64
	db       *Database
	tx       engine.Transaction
	writable bool

	tableInfoStore *tableInfoStore
	indexStore     *indexStore
}

// DB returns the underlying database that created the transaction.
func (tx *Transaction) DB() *Database {
	return tx.db
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Transaction) Rollback() error {
	tx.db.attachedTxMu.Lock()
	defer tx.db.attachedTxMu.Unlock()

	if tx.writable {
		tx.tableInfoStore.rollback(tx)
	}

	err := tx.tx.Rollback()
	if err != nil {
		return err
	}

	if tx.db.attachedTransaction != nil {
		tx.db.attachedTransaction = nil
	}

	return nil
}

// Commit the transaction.
func (tx *Transaction) Commit() error {
	tx.db.attachedTxMu.Lock()
	defer tx.db.attachedTxMu.Unlock()

	if tx.writable {
		tx.tableInfoStore.commit(tx)
	}

	err := tx.tx.Commit()
	if err != nil {
		return err
	}

	if tx.db.attachedTransaction != nil {
		tx.db.attachedTransaction = nil
	}

	return nil

}

// Writable indicates if the transaction is writable or not.
func (tx *Transaction) Writable() bool {
	return tx.writable
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx *Transaction) CreateTable(ctx context.Context, name string, info *TableInfo) error {
	if strings.HasPrefix(name, internalPrefix) {
		return fmt.Errorf("table name must not start with %s", internalPrefix)
	}

	if info == nil {
		info = new(TableInfo)
	}

	info.tableName = name
	err := tx.tableInfoStore.Insert(ctx, tx, name, info)
	if err != nil {
		return err
	}

	err = tx.tx.CreateStore(ctx, info.storeName)
	if err != nil {
		return fmt.Errorf("failed to create table %q: %w", name, err)
	}

	return nil
}

// GetTable returns a table by name. The table instance is only valid for the lifetime of the transaction.
func (tx *Transaction) GetTable(ctx context.Context, name string) (*Table, error) {
	ti, err := tx.tableInfoStore.Get(ctx, tx, name)
	if err != nil {
		return nil, err
	}

	s, err := tx.tx.GetStore(ctx, ti.storeName)
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
func (tx *Transaction) RenameTable(ctx context.Context, oldName, newName string) error {
	ti, err := tx.tableInfoStore.Get(ctx, tx, oldName)
	if err != nil {
		return err
	}

	if ti.readOnly {
		return errors.New("cannot write to read-only table")
	}

	ti.tableName = newName
	// Insert the TableInfo keyed by the newName name.
	err = tx.tableInfoStore.Insert(ctx, tx, newName, ti)
	if err != nil {
		return err
	}

	// Update the indexes.
	idxs, err := tx.ListIndexes(ctx)
	if err != nil {
		return err
	}
	for _, idx := range idxs {
		if idx.TableName == oldName {
			idx.TableName = newName
			err = tx.indexStore.Replace(ctx, idx.IndexName, *idx)
			if err != nil {
				return err
			}
		}
	}

	// Delete the old reference from the tableInfoStore.
	return tx.tableInfoStore.Delete(ctx, tx, oldName)
}

// DropTable deletes a table from the database.
func (tx *Transaction) DropTable(ctx context.Context, name string) error {
	ti, err := tx.tableInfoStore.Get(ctx, tx, name)
	if err != nil {
		return err
	}

	if ti.readOnly {
		return errors.New("cannot write to read-only table")
	}

	it := tx.indexStore.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var buf []byte
	for it.Seek(ctx, nil); it.Valid(); it.Next(ctx) {
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

		err = tx.DropIndex(ctx, opts.IndexName)
		if err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}

	err = tx.tableInfoStore.Delete(ctx, tx, name)
	if err != nil {
		return err
	}

	return tx.tx.DropStore(ctx, ti.storeName)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrIndexAlreadyExists.
func (tx *Transaction) CreateIndex(ctx context.Context, opts IndexConfig) error {
	t, err := tx.GetTable(ctx, opts.TableName)
	if err != nil {
		return err
	}

	info, err := t.Info(ctx)
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

	return tx.indexStore.Insert(ctx, opts)
}

// GetIndex returns an index by name.
func (tx *Transaction) GetIndex(ctx context.Context, name string) (*Index, error) {
	opts, err := tx.indexStore.Get(ctx, name)
	if err != nil {
		return nil, err
	}

	idx := index.NewIndex(tx.tx, opts.IndexName, index.Options{
		Unique: opts.Unique,
		Type:   opts.Type,
	})

	return &Index{
		Index: idx,
		Opts:  *opts,
	}, nil
}

// DropIndex deletes an index from the database.
func (tx *Transaction) DropIndex(ctx context.Context, name string) error {
	opts, err := tx.indexStore.Get(ctx, name)
	if err != nil {
		return err
	}
	err = tx.indexStore.Delete(ctx, name)
	if err != nil {
		return err
	}

	idx := index.NewIndex(tx.tx, opts.IndexName, index.Options{
		Unique: opts.Unique,
		Type:   opts.Type,
	})

	return idx.Truncate(ctx)
}

// ListIndexes lists all indexes.
func (tx *Transaction) ListIndexes(ctx context.Context) ([]*IndexConfig, error) {
	return tx.indexStore.ListAll(ctx)
}

// ReIndex truncates and recreates selected index from scratch.
func (tx *Transaction) ReIndex(ctx context.Context, indexName string) error {
	idx, err := tx.GetIndex(ctx, indexName)
	if err != nil {
		return err
	}

	tb, err := tx.GetTable(ctx, idx.Opts.TableName)
	if err != nil {
		return err
	}

	err = idx.Truncate(ctx)
	if err != nil {
		return err
	}

	return tb.Iterate(ctx, func(d document.Document) error {
		v, err := idx.Opts.Path.GetValue(d)
		if err == document.ErrFieldNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return idx.Set(ctx, v, d.(document.Keyer).Key())
	})
}

// ReIndexAll truncates and recreates all indexes of the database from scratch.
func (tx *Transaction) ReIndexAll(ctx context.Context) error {
	it := tx.indexStore.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var indexes []string
	for it.Seek(ctx, nil); it.Valid(); it.Next(ctx) {
		indexes = append(indexes, string(it.Item().Key()))
	}
	if err := it.Err(); err != nil {
		return err
	}

	for _, indexName := range indexes {
		err := tx.ReIndex(ctx, indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *Transaction) getIndexStore(ctx context.Context) (*indexStore, error) {
	st, err := tx.tx.GetStore(ctx, []byte(indexStoreName))
	if err != nil {
		return nil, err
	}
	return &indexStore{
		st: st,
		db: tx.db,
	}, nil
}

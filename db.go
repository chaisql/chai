package genji

import (
	"bytes"
	"database/sql"
	"strings"
	"sync"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
	"github.com/pkg/errors"
)

var (
	separator            byte = 0x1F
	tableConfigStoreName      = "__genji.tables"
	indexStoreName            = "__genji.indexes"
	indexPrefix               = "i"
)

// Open creates a Genji database and wraps it around a *sql.DB instance.
func Open(ng engine.Engine) (*sql.DB, error) {
	db, err := New(ng)
	if err != nil {
		return nil, err
	}

	return OpenDB(db)
}

// OpenDB connects to an existing database instance and returns a *sql.DB.
func OpenDB(db *DB) (*sql.DB, error) {
	return sql.OpenDB(newConnector(db)), nil
}

// DB represents a collection of tables stored in the underlying engine.
// DB differs from the engine in that it provides automatic indexing
// and database administration methods.
// DB is safe for concurrent use unless the given engine isn't.
type DB struct {
	ng engine.Engine

	mu sync.Mutex
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*DB, error) {
	db := DB{
		ng: ng,
	}

	ntx, err := db.ng.Begin(true)
	if err != nil {
		return nil, err
	}
	defer ntx.Rollback()

	_, err = ntx.Store(tableConfigStoreName)
	if err == engine.ErrStoreNotFound {
		err = ntx.CreateStore(tableConfigStoreName)
	}
	if err != nil {
		return nil, err
	}

	_, err = ntx.Store(indexStoreName)
	if err == engine.ErrStoreNotFound {
		err = ntx.CreateStore(indexStoreName)
	}
	if err != nil {
		return nil, err
	}

	err = ntx.Commit()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Close the underlying engine.
func (db *DB) Close() error {
	return db.ng.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *DB) Begin(writable bool) (*Tx, error) {
	ntx, err := db.ng.Begin(writable)
	if err != nil {
		return nil, err
	}

	tx := Tx{
		db:       db,
		tx:       ntx,
		writable: writable,
	}

	tx.tcfgStore, err = tx.getTableConfigStore()
	if err != nil {
		return nil, err
	}

	tx.indexStore, err = tx.getIndexStore()
	if err != nil {
		return nil, err
	}

	return &tx, nil
}

// View starts a read only transaction, runs fn and automatically rolls it back.
func (db *DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

// Update starts a read-write transaction, runs fn and automatically commits it.
func (db *DB) Update(fn func(tx *Tx) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = fn(tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Exec a query against the database without returning the result.
func (db *DB) Exec(q string, args ...interface{}) error {
	res, err := db.Query(q, args...)
	if err != nil {
		return err
	}

	return res.Close()
}

// Query the database and return the result.
// The returned result must always be closed after usage.
func (db *DB) Query(q string, args ...interface{}) (*Result, error) {
	pq, err := parseQuery(q)
	if err != nil {
		return nil, err
	}

	return pq.Run(db, argsToNamedValues(args))
}

// ViewTable starts a read only transaction, fetches the selected table, calls fn with that table
// and automatically rolls back the transaction.
func (db *DB) ViewTable(tableName string, fn func(*Tx, *Table) error) error {
	return db.View(func(tx *Tx) error {
		tb, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		return fn(tx, tb)
	})
}

// UpdateTable starts a read/write transaction, fetches the selected table, calls fn with that table
// and automatically commits the transaction.
// If fn returns an error, the transaction is rolled back.
func (db *DB) UpdateTable(tableName string, fn func(*Tx, *Table) error) error {
	return db.Update(func(tx *Tx) error {
		tb, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		return fn(tx, tb)
	})
}

// Tx represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Tx is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Tx struct {
	db         *DB
	tx         engine.Transaction
	writable   bool
	tcfgStore  *tableConfigStore
	indexStore *indexStore
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

// Commit the transaction.
func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

// Writable indicates if the transaction is writable or not.
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Promote rollsback a read-only transaction and begins a read-write transaction transparently.
// It returns an error if the current transaction is already writable.
func (tx *Tx) Promote() error {
	if tx.writable {
		return errors.New("can't promote a writable transaction")
	}

	err := tx.Rollback()
	if err != nil {
		return err
	}

	newTx, err := tx.db.Begin(true)
	if err != nil {
		return err
	}

	*tx = *newTx
	return nil
}

// Query the database withing the transaction and returns the result.
// Closing the returned result after usage is not mandatory.
func (tx *Tx) Query(q string, args ...interface{}) (*Result, error) {
	pq, err := parseQuery(q)
	if err != nil {
		return nil, err
	}

	return pq.Exec(tx, argsToNamedValues(args), false)
}

// Exec a query against the database within tx and without returning the result.
func (tx *Tx) Exec(q string, args ...interface{}) error {
	res, err := tx.Query(q, args...)
	if err != nil {
		return err
	}

	return res.Close()
}

// TableConfig holds the configuration of a table
type TableConfig struct {
	PrimaryKeyName string
	PrimaryKeyType value.Type

	lastKey int64
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx Tx) CreateTable(name string, cfg *TableConfig) error {
	if cfg == nil {
		cfg = new(TableConfig)
	}
	err := tx.tcfgStore.Insert(name, *cfg)
	if err != nil {
		return err
	}

	err = tx.tx.CreateStore(name)
	if err != nil {
		return errors.Wrapf(err, "failed to create table %q", name)
	}

	return nil
}

// GetTable returns a table by name. The table instance is only valid for the lifetime of the transaction.
func (tx Tx) GetTable(name string) (*Table, error) {
	_, err := tx.tcfgStore.Get(name)
	if err != nil {
		return nil, err
	}

	s, err := tx.tx.Store(name)
	if err != nil {
		return nil, err
	}

	return &Table{
		tx:       &tx,
		store:    s,
		name:     name,
		cfgStore: tx.tcfgStore,
	}, nil
}

// DropTable deletes a table from the database.
func (tx Tx) DropTable(name string) error {
	err := tx.tcfgStore.Delete(name)
	if err != nil {
		return err
	}

	return tx.tx.DropStore(name)
}

// ListTables lists all the tables.
func (tx Tx) ListTables() ([]string, error) {
	stores, err := tx.tx.ListStores("")
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0, len(stores))
	idxPrefix := indexPrefix + string([]byte{separator})

	for _, st := range stores {
		if st == indexStoreName || st == tableConfigStoreName {
			continue
		}
		if strings.HasPrefix(st, idxPrefix) {
			continue
		}

		tables = append(tables, st)
	}

	return tables, nil
}

func buildIndexName(name string) string {
	var b strings.Builder
	b.WriteString(indexPrefix)
	b.WriteByte(separator)
	b.WriteString(name)

	return b.String()
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx Tx) CreateIndex(opts index.Options) error {
	_, err := tx.GetTable(opts.TableName)
	if err != nil {
		return err
	}

	idxOpts := indexOptions{
		IndexName: opts.IndexName,
		TableName: opts.TableName,
		FieldName: opts.FieldName,
		Unique:    opts.Unique,
	}

	return tx.indexStore.Insert(idxOpts)
}

// GetIndex returns an index by name.
func (tx Tx) GetIndex(name string) (*Index, error) {
	opts, err := tx.indexStore.Get(name)
	if err != nil {
		return nil, err
	}

	return &Index{
		Index: index.New(tx.tx, index.Options{
			IndexName: opts.IndexName,
			TableName: opts.TableName,
			FieldName: opts.FieldName,
			Unique:    opts.Unique,
		}),
		IndexName: opts.IndexName,
		TableName: opts.TableName,
		FieldName: opts.FieldName,
		Unique:    opts.Unique,
	}, nil
}

// DropIndex deletes an index from the database.
func (tx Tx) DropIndex(name string) error {
	opts, err := tx.indexStore.Get(name)
	if err != nil {
		return err
	}
	err = tx.indexStore.Delete(name)
	if err != nil {
		return err
	}

	return index.New(tx.tx, index.Options{
		IndexName: opts.IndexName,
		TableName: opts.TableName,
		FieldName: opts.FieldName,
		Unique:    opts.Unique,
	}).Truncate()
}

// ReIndex truncates and recreates selected index from scratch.
func (tx Tx) ReIndex(indexName string) error {
	idx, err := tx.GetIndex(indexName)
	if err != nil {
		return err
	}

	tb, err := tx.GetTable(idx.TableName)
	if err != nil {
		return err
	}

	err = idx.Truncate()
	if err != nil {
		return err
	}

	return tb.Iterate(func(r record.Record) error {
		f, err := r.GetField(idx.FieldName)
		if err != nil {
			return err
		}

		return idx.Set(f.Value, r.(record.Keyer).Key())
	})
}

// ReIndexAll truncates and recreates all indexes of the database from scratch.
func (tx Tx) ReIndexAll() error {
	return tx.indexStore.st.AscendGreaterOrEqual(nil, func(k, v []byte) error {
		var opts indexOptions
		err := opts.ScanRecord(record.EncodedRecord(v))
		if err != nil {
			return err
		}

		idx := index.New(tx.tx, index.Options{
			IndexName: opts.IndexName,
			TableName: opts.TableName,
			FieldName: opts.FieldName,
			Unique:    opts.Unique,
		})

		tb, err := tx.GetTable(opts.TableName)
		if err != nil {
			return err
		}

		err = idx.Truncate()
		if err != nil {
			return err
		}

		return tb.Iterate(func(r record.Record) error {
			f, err := r.GetField(opts.FieldName)
			if err != nil {
				return err
			}

			return idx.Set(f.Value, r.(record.Keyer).Key())
		})
	})
}

// A Table represents a collection of records.
type Table struct {
	tx       *Tx
	store    engine.Store
	name     string
	cfgStore *tableConfigStore
}

type encodedRecordWithKey struct {
	record.EncodedRecord

	key []byte
}

func (e encodedRecordWithKey) Key() []byte {
	return e.key
}

// Iterate goes through all the records of the table and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (t *Table) Iterate(fn func(r record.Record) error) error {
	// To avoid unnecessary allocations, we create the slice once and reuse it
	// at each call of the fn method.
	// Since the AscendGreaterOrEqual is never supposed to call the callback concurrently
	// we can assume that it's thread safe.
	// TODO(asdine) Add a mutex if proven necessary
	var r encodedRecordWithKey

	return t.store.AscendGreaterOrEqual(nil, func(k, v []byte) error {
		r.EncodedRecord = v
		r.key = k
		// r must be passed as pointer, not value, because passing a value to an interface
		// requires an allocation, while it doesn't for a pointer.
		return fn(&r)
	})
}

// GetRecord returns one record by key.
func (t *Table) GetRecord(key []byte) (record.Record, error) {
	v, err := t.store.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return nil, ErrRecordNotFound
		}
		return nil, errors.Wrapf(err, "failed to fetch record %q", key)
	}

	var r encodedRecordWithKey
	r.EncodedRecord = record.EncodedRecord(v)
	r.key = key
	return &r, err
}

func (t *Table) generateKey(r record.Record) ([]byte, error) {
	cfg, err := t.cfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	var key []byte
	if cfg.PrimaryKeyName != "" {
		f, err := r.GetField(cfg.PrimaryKeyName)
		if err != nil {
			return nil, err
		}
		v, err := f.DecodeTo(cfg.PrimaryKeyType)
		if err != nil {
			return nil, err
		}
		return v.Data, nil
	}

	t.tx.db.mu.Lock()
	defer t.tx.db.mu.Unlock()

	cfg, err = t.cfgStore.Get(t.name)
	if err != nil {
		return nil, err
	}

	cfg.lastKey++
	key = value.NewInt64(cfg.lastKey).Data
	err = t.cfgStore.Replace(t.name, cfg)
	if err != nil {
		return nil, err
	}

	return key, nil
}

// Insert the record into the table.
// If a primary key has been specified during the table creation, the field is expected to be present
// in the given record.
// If no primary key has been selected, a monotonic autoincremented integer key will be generated.
func (t *Table) Insert(r record.Record) ([]byte, error) {
	key, err := t.generateKey(r)
	if err != nil {
		return nil, err
	}

	_, err = t.store.Get(key)
	if err == nil {
		return nil, ErrDuplicateRecord
	}

	v, err := record.Encode(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode record")
	}

	err = t.store.Put(key, v)
	if err != nil {
		return nil, err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		f, err := r.GetField(idx.FieldName)
		if err != nil {
			continue
		}

		err = idx.Set(f.Value, key)
		if err != nil {
			if err == index.ErrDuplicate {
				return nil, ErrDuplicateRecord
			}

			return nil, err
		}
	}

	return key, nil
}

// Delete a record by key.
// Indexes are automatically updated.
func (t *Table) Delete(key []byte) error {
	r, err := t.GetRecord(key)
	if err != nil {
		return err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		f, err := r.GetField(idx.FieldName)
		if err != nil {
			return err
		}

		err = idx.Delete(f.Value, key)
		if err != nil {
			return err
		}
	}

	return t.store.Delete(key)
}

// Replace a record by key.
// An error is returned if the key doesn't exist.
// Indexes are automatically updated.
func (t *Table) Replace(key []byte, r record.Record) error {
	indexes, err := t.Indexes()
	if err != nil {
		return err
	}

	return t.replace(indexes, key, r)
}

func (t *Table) replace(indexes map[string]Index, key []byte, r record.Record) error {
	// make sure key exists
	old, err := t.GetRecord(key)
	if err != nil {
		return err
	}

	// remove key from indexes
	for _, idx := range indexes {
		f, err := old.GetField(idx.FieldName)
		if err != nil {
			return err
		}

		err = idx.Delete(f.Value, key)
		if err != nil {
			return err
		}
	}

	// encode new record
	v, err := record.Encode(r)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	// replace old record with new record
	err = t.store.Put(key, v)
	if err != nil {
		return err
	}

	// update indexes
	for _, idx := range indexes {
		f, err := r.GetField(idx.FieldName)
		if err != nil {
			continue
		}

		err = idx.Set(f.Value, key)
		if err != nil {
			return err
		}
	}

	return err
}

// Truncate deletes all the records from the table.
func (t *Table) Truncate() error {
	return t.store.Truncate()
}

// TableName returns the name of the table.
func (t *Table) TableName() string {
	return t.name
}

// Indexes returns a map of all the indexes of a table.
func (t *Table) Indexes() (map[string]Index, error) {
	s, err := t.tx.tx.Store(indexStoreName)
	if err != nil {
		return nil, err
	}

	tb := Table{
		tx:    t.tx,
		store: s,
		name:  indexStoreName,
	}

	tableName := []byte(t.name)
	indexes := make(map[string]Index)

	err = record.NewStream(&tb).
		Filter(func(r record.Record) (bool, error) {
			f, err := r.GetField("TableName")
			if err != nil {
				return false, err
			}

			return bytes.Equal(f.Data, tableName), nil
		}).
		Iterate(func(r record.Record) error {
			var opts indexOptions
			err := opts.ScanRecord(r)
			if err != nil {
				return err
			}

			indexes[opts.FieldName] = Index{
				Index: index.New(t.tx.tx, index.Options{
					IndexName: opts.IndexName,
					TableName: opts.TableName,
					FieldName: opts.FieldName,
					Unique:    opts.Unique,
				}),
				IndexName: opts.IndexName,
				TableName: opts.TableName,
				FieldName: opts.FieldName,
				Unique:    opts.Unique,
			}

			return nil
		})
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

type indexOptions struct {
	IndexName string
	TableName string
	FieldName string
	Unique    bool
}

// Field implements the field method of the record.Record interface.
func (i *indexOptions) GetField(name string) (record.Field, error) {
	switch name {
	case "IndexName":
		return record.NewStringField("IndexName", i.IndexName), nil
	case "TableName":
		return record.NewStringField("TableName", i.TableName), nil
	case "FieldName":
		return record.NewStringField("FieldName", i.FieldName), nil
	case "Unique":
		return record.NewBoolField("Unique", i.Unique), nil
	}

	return record.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (i *indexOptions) Iterate(fn func(record.Field) error) error {
	var err error
	var f record.Field

	f, _ = i.GetField("IndexName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("TableName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("FieldName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("Unique")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (i *indexOptions) ScanRecord(rec record.Record) error {
	return rec.Iterate(func(f record.Field) error {
		var err error

		switch f.Name {
		case "IndexName":
			i.IndexName, err = value.DecodeString(f.Data)
		case "TableName":
			i.TableName, err = value.DecodeString(f.Data)
		case "FieldName":
			i.FieldName, err = value.DecodeString(f.Data)
		case "Unique":
			i.Unique, err = value.DecodeBool(f.Data)
		}
		return err
	})
}

// Index of a table field. Contains information about
// the index configuration and provides methods to manipulate the index.
type Index struct {
	index.Index

	IndexName string
	TableName string
	FieldName string
	Unique    bool
}

type indexStore struct {
	st engine.Store
}

func (tx *Tx) getIndexStore() (*indexStore, error) {
	st, err := tx.tx.Store(indexStoreName)
	if err != nil {
		return nil, err
	}
	return &indexStore{
		st: st,
	}, nil
}

func (t *indexStore) Insert(cfg indexOptions) error {
	key := []byte(buildIndexName(cfg.IndexName))
	_, err := t.st.Get(key)
	if err == nil {
		return ErrIndexAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	v, err := record.Encode(&cfg)
	if err != nil {
		return err
	}

	return t.st.Put(key, v)
}

func (t *indexStore) Get(indexName string) (*indexOptions, error) {
	key := []byte(buildIndexName(indexName))
	v, err := t.st.Get(key)
	if err == engine.ErrKeyNotFound {
		return nil, ErrIndexNotFound
	}
	if err != nil {
		return nil, err
	}

	var idxopts indexOptions
	err = idxopts.ScanRecord(record.EncodedRecord(v))
	if err != nil {
		return nil, err
	}

	return &idxopts, nil
}

func (t *indexStore) Delete(indexName string) error {
	key := []byte(buildIndexName(indexName))
	err := t.st.Delete(key)
	if err == engine.ErrKeyNotFound {
		return ErrIndexNotFound
	}
	return err
}

type tableConfigStore struct {
	st engine.Store
}

func (tx *Tx) getTableConfigStore() (*tableConfigStore, error) {
	st, err := tx.tx.Store(tableConfigStoreName)
	if err != nil {
		return nil, err
	}
	return &tableConfigStore{
		st: st,
	}, nil
}

func (t *tableConfigStore) Insert(tableName string, cfg TableConfig) error {
	key := []byte(tableName)
	_, err := t.st.Get(key)
	if err == nil {
		return ErrTableAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	var fb record.FieldBuffer
	fb.Add(record.NewStringField("PrimaryKeyName", cfg.PrimaryKeyName))
	fb.Add(record.NewUint8Field("PrimaryKeyType", uint8(cfg.PrimaryKeyType)))
	fb.Add(record.NewInt64Field("lastKey", cfg.lastKey))

	v, err := record.Encode(&fb)
	if err != nil {
		return err
	}

	return t.st.Put(key, v)
}

func (t *tableConfigStore) Replace(tableName string, cfg *TableConfig) error {
	key := []byte(tableName)
	_, err := t.st.Get(key)
	if err == engine.ErrKeyNotFound {
		return ErrTableNotFound
	}
	if err != nil {
		return err
	}

	var fb record.FieldBuffer
	fb.Add(record.NewStringField("PrimaryKeyName", cfg.PrimaryKeyName))
	fb.Add(record.NewUint8Field("PrimaryKeyType", uint8(cfg.PrimaryKeyType)))
	fb.Add(record.NewInt64Field("lastKey", cfg.lastKey))

	v, err := record.Encode(&fb)
	if err != nil {
		return err
	}
	return t.st.Put(key, v)
}

func (t *tableConfigStore) Get(tableName string) (*TableConfig, error) {
	key := []byte(tableName)
	v, err := t.st.Get(key)
	if err == engine.ErrKeyNotFound {
		return nil, ErrTableNotFound
	}
	if err != nil {
		return nil, err
	}

	var cfg TableConfig

	r := record.EncodedRecord(v)

	f, err := r.GetField("PrimaryKeyName")
	if err != nil {
		return nil, err
	}
	cfg.PrimaryKeyName, err = f.DecodeToString()
	if err != nil {
		return nil, err
	}
	f, err = r.GetField("PrimaryKeyType")
	if err != nil {
		return nil, err
	}
	tp, err := f.DecodeToUint8()
	if err != nil {
		return nil, err
	}
	cfg.PrimaryKeyType = value.Type(tp)

	f, err = r.GetField("lastKey")
	if err != nil {
		return nil, err
	}
	cfg.lastKey, err = f.DecodeToInt64()
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (t *tableConfigStore) Delete(tableName string) error {
	key := []byte(tableName)
	err := t.st.Delete(key)
	if err == engine.ErrKeyNotFound {
		return ErrTableNotFound
	}
	return err
}

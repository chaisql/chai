package database

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/index"
)

// FieldConstraint describes constraints on a particular field.
type FieldConstraint struct {
	Path         document.ValuePath
	Type         document.ValueType
	IsPrimaryKey bool
	IsNotNull    bool
}

// ToDocument returns a document from f.
func (f *FieldConstraint) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("path", document.NewArrayValue(valuePathToArray(f.Path)))
	buf.Add("type", document.NewIntegerValue(int64(f.Type)))
	buf.Add("is_primary_key", document.NewBoolValue(f.IsPrimaryKey))
	buf.Add("is_not_null", document.NewBoolValue(f.IsNotNull))
	return buf
}

// ScanDocument implements the document.Scanner interface.
func (f *FieldConstraint) ScanDocument(d document.Document) error {
	v, err := d.GetByField("path")
	if err != nil {
		return err
	}
	f.Path, err = arrayToValuePath(v)
	if err != nil {
		return err
	}

	v, err = d.GetByField("type")
	if err != nil {
		return err
	}
	tp := v.V.(int64)
	f.Type = document.ValueType(tp)

	v, err = d.GetByField("is_primary_key")
	if err != nil {
		return err
	}
	f.IsPrimaryKey = v.V.(bool)

	v, err = d.GetByField("is_not_null")
	if err != nil {
		return err
	}
	f.IsNotNull = v.V.(bool)
	return nil
}

type TableInfo struct {
	tableName string
	// storeID is used as a key to reference a table.
	storeID  []byte
	readOnly bool
	// if non-zero, this tableInfo has been created during the current transaction.
	// it will be removed if the transaction is rolled back or set to false if its commited.
	transactionID int64

	FieldConstraints []FieldConstraint
}

// GetPrimaryKey returns the field constraint of the primary key.
// Returns nil if there is no primary key.
func (ti *TableInfo) GetPrimaryKey() *FieldConstraint {
	for _, f := range ti.FieldConstraints {
		if f.IsPrimaryKey {
			return &f
		}
	}

	return nil
}

func (ti *TableInfo) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("table_name", document.NewTextValue(ti.tableName))
	buf.Add("store_id", document.NewBlobValue(ti.storeID))

	vbuf := document.NewValueBuffer()
	for _, fc := range ti.FieldConstraints {
		vbuf = vbuf.Append(document.NewDocumentValue(fc.ToDocument()))
	}

	buf.Add("field_constraints", document.NewArrayValue(vbuf))

	buf.Add("read_only", document.NewBoolValue(ti.readOnly))
	return buf
}

func (ti *TableInfo) ScanDocument(d document.Document) error {
	v, err := d.GetByField("table_name")
	if err != nil {
		return err
	}
	ti.tableName = v.V.(string)

	v, err = d.GetByField("store_id")
	if err != nil {
		return err
	}
	ti.storeID = make([]byte, len(v.V.([]byte)))
	copy(ti.storeID, v.V.([]byte))

	v, err = d.GetByField("field_constraints")
	if err != nil {
		return err
	}
	ar := v.V.(document.Array)

	l, err := document.ArrayLength(ar)
	if err != nil {
		return err
	}

	ti.FieldConstraints = make([]FieldConstraint, l)

	err = ar.Iterate(func(i int, value document.Value) error {
		return ti.FieldConstraints[i].ScanDocument(v.V.(document.Document))
	})
	if err != nil {
		return err
	}

	v, err = d.GetByField("read_only")
	if err != nil {
		return err
	}

	ti.readOnly = v.V.(bool)
	return nil
}

// tableInfoStore manages table information.
// It loads table information during database startup
// and holds it in memory.
type tableInfoStore struct {
	// tableInfos contains information about all the tables
	tableInfos map[string]TableInfo

	mu sync.RWMutex
}

func newTableInfoStore(tx engine.Transaction) (*tableInfoStore, error) {
	var ts tableInfoStore

	err := ts.loadAllTableInfo(tx)
	if err != nil {
		return nil, err
	}

	return &ts, nil
}

// Insert a new tableInfo for the given table name.
func (t *tableInfoStore) Insert(tx *Transaction, tableName string, info *TableInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.tableInfos[tableName]
	if ok {
		// TODO(asdine): if a table already exists but is uncommited,
		// there is a chance the other transaction will be rolled back.
		// Instead of returning an error, wait until the other transaction is
		// either commited or rolled back.
		// If it is commited, return an error here
		// If not, create the table in this transaction.
		return ErrTableAlreadyExists
	}

	v, err := msgpack.EncodeDocument(info.ToDocument())
	if err != nil {
		return err
	}

	st, err := tx.Tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return err
	}

	err = st.Put([]byte(tableName), v)
	if err != nil {
		return err
	}

	info.transactionID = tx.id
	t.tableInfos[tableName] = *info
	return nil
}

func (t *tableInfoStore) Get(tx *Transaction, tableName string) (*TableInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, ok := t.tableInfos[tableName]
	if !ok {
		return nil, ErrTableNotFound
	}

	if info.transactionID != 0 && info.transactionID != tx.id {
		return nil, ErrTableNotFound
	}

	return &info, nil
}

func (t *tableInfoStore) Delete(tx *Transaction, tableName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.tableInfos[tableName]
	if !ok {
		return ErrTableNotFound
	}

	if info.transactionID != 0 && info.transactionID != tx.id {
		return ErrTableNotFound
	}

	st, err := tx.Tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return err
	}

	key := []byte(tableName)
	err = st.Delete(key)
	if err == engine.ErrKeyNotFound {
		return ErrTableNotFound
	}
	if err != nil {
		return err
	}

	delete(t.tableInfos, tableName)

	return nil
}

func (t *tableInfoStore) loadAllTableInfo(tx engine.Transaction) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, err := tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return err
	}

	it := st.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	t.tableInfos = make(map[string]TableInfo)
	var b []byte
	for it.Seek(nil); it.Valid(); it.Next() {
		itm := it.Item()
		b, err = itm.ValueCopy(b)
		if err != nil {
			return err
		}

		var ti TableInfo
		err = ti.ScanDocument(msgpack.EncodedDocument(b))
		if err != nil {
			return err
		}

		t.tableInfos[string(itm.Key())] = ti
	}

	t.tableInfos[tableInfoStoreName] = TableInfo{
		storeID:  []byte(tableInfoStoreName),
		readOnly: true,
	}
	return nil
}

// remove all tableInfo whose transaction id is equal to the given transacrion id.
// this is called when a read/write transaction is being rolled back.
func (t *tableInfoStore) rollback(tx *Transaction) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for k, info := range t.tableInfos {
		if info.transactionID == tx.id {
			delete(t.tableInfos, k)
		}
	}
}

// set all the tableInfo created by this transaction to 0.
// this is called when a read/write transaction is being commited.
func (t *tableInfoStore) commit(tx *Transaction) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for k := range t.tableInfos {
		if t.tableInfos[k].transactionID == tx.id {
			info := t.tableInfos[k]
			info.transactionID = 0
			t.tableInfos[k] = info
		}
	}
}

// ListTables lists all the tables. It ignores tables created by
// other transactions that haven't been commited yet.
// The returned slice is lexicographically ordered.
func (t *tableInfoStore) ListTables(tx *Transaction) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	names := make([]string, 0, len(t.tableInfos))
	for k := range t.tableInfos {
		if t.tableInfos[k].transactionID != 0 && t.tableInfos[k].transactionID != tx.id {
			continue
		}

		if strings.HasPrefix(k, internalPrefix) {
			continue
		}

		names = append(names, k)
	}

	sort.Strings(names)

	return names
}

// GetTableInfo returns a copy of all the table information.
func (t *tableInfoStore) GetTableInfo() map[string]TableInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	ti := make(map[string]TableInfo, len(t.tableInfos))
	for k, v := range t.tableInfos {
		ti[k] = v
	}

	return ti
}

// generateStoreID generates an ID used as a key to reference a table.
// The first 4 bytes represent the timestamp in second and the last-2 bytes are
// randomly generated.
func (t *tableInfoStore) generateStoreID() []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var found bool = true
	var id [6]byte
	for found {
		binary.BigEndian.PutUint32(id[:], uint32(time.Now().Unix()))
		if _, err := rand.Reader.Read(id[4:]); err != nil {
			panic(fmt.Errorf("cannot generate random number: %v;", err))
		}

		found = false
		for _, ti := range t.tableInfos {
			if bytes.Equal(ti.storeID, id[:]) {
				// A store with this id already exists.
				// Let's generate a new one.
				found = true
				break
			}
		}
	}

	return id[:]
}

// IndexConfig holds the configuration of an index.
type IndexConfig struct {
	TableName string
	IndexName string
	Path      document.ValuePath

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool
}

// ToDocument creates a document from an IndexConfig.
func (i *IndexConfig) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("unique", document.NewBoolValue(i.Unique))
	buf.Add("indexname", document.NewTextValue(i.IndexName))
	buf.Add("tablename", document.NewTextValue(i.TableName))
	buf.Add("path", document.NewArrayValue(valuePathToArray(i.Path)))
	return buf
}

// ScanDocument implements the document.Scanner interface.
func (i *IndexConfig) ScanDocument(d document.Document) error {
	v, err := d.GetByField("unique")
	if err != nil {
		return err
	}
	i.Unique = v.V.(bool)

	v, err = d.GetByField("indexname")
	if err != nil {
		return err
	}
	i.IndexName = string(v.V.(string))

	v, err = d.GetByField("tablename")
	if err != nil {
		return err
	}
	i.TableName = string(v.V.(string))

	v, err = d.GetByField("path")
	if err != nil {
		return err
	}
	i.Path, err = arrayToValuePath(v)
	return err
}

// Index of a table field. Contains information about
// the index configuration and provides methods to manipulate the index.
type Index struct {
	index.Index
	Opts IndexConfig
}

type indexStore struct {
	st engine.Store
}

func (t *indexStore) Insert(cfg IndexConfig) error {
	key := []byte(cfg.IndexName)
	_, err := t.st.Get(key)
	if err == nil {
		return ErrIndexAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	v, err := msgpack.EncodeDocument(cfg.ToDocument())
	if err != nil {
		return err
	}

	return t.st.Put(key, v)
}

func (t *indexStore) Get(indexName string) (*IndexConfig, error) {
	key := []byte(indexName)
	v, err := t.st.Get(key)
	if err == engine.ErrKeyNotFound {
		return nil, ErrIndexNotFound
	}
	if err != nil {
		return nil, err
	}

	var idxopts IndexConfig
	err = idxopts.ScanDocument(msgpack.EncodedDocument(v))
	if err != nil {
		return nil, err
	}

	return &idxopts, nil
}

func (t *indexStore) Replace(indexName string, cfg IndexConfig) error {
	v, err := msgpack.EncodeDocument(cfg.ToDocument())
	if err != nil {
		return err
	}

	return t.st.Put([]byte(indexName), v)
}

func (t *indexStore) Delete(indexName string) error {
	key := []byte(indexName)
	err := t.st.Delete(key)
	if err == engine.ErrKeyNotFound {
		return ErrIndexNotFound
	}
	return err
}

func (t *indexStore) ListAll() ([]*IndexConfig, error) {
	var idxList []*IndexConfig
	it := t.st.NewIterator(engine.IteratorConfig{})

	var buf msgpack.EncodedDocument
	var err error
	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		var opts IndexConfig
		buf, err = item.ValueCopy(buf)
		if err != nil {
			it.Close()
			return nil, err
		}

		err = opts.ScanDocument(&buf)
		if err != nil {
			it.Close()
			return nil, err
		}

		idxList = append(idxList, &opts)
	}
	err = it.Close()
	if err != nil {
		return nil, err
	}

	return idxList, nil
}

func arrayToValuePath(v document.Value) (document.ValuePath, error) {
	var path document.ValuePath

	err := v.V.(document.Array).Iterate(func(_ int, value document.Value) error {
		path = append(path, value.V.(string))
		return nil
	})

	return path, err
}

func valuePathToArray(path document.ValuePath) document.Array {
	abuf := document.NewValueBuffer()
	for _, p := range path {
		abuf = abuf.Append(document.NewTextValue(p))
	}

	return abuf
}

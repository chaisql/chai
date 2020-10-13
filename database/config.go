package database

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/index"
)

const storePrefix = 't'

// FieldConstraint describes constraints on a particular field.
type FieldConstraint struct {
	Path         document.ValuePath
	Type         document.ValueType
	IsPrimaryKey bool
	IsNotNull    bool
	DefaultValue document.Value
}

func (f *FieldConstraint) HasDefaultValue() bool {
	return f.DefaultValue.Type != 0
}

// ToDocument returns a document from f.
func (f *FieldConstraint) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("path", document.NewArrayValue(valuePathToArray(f.Path)))
	buf.Add("type", document.NewIntegerValue(int64(f.Type)))
	buf.Add("is_primary_key", document.NewBoolValue(f.IsPrimaryKey))
	buf.Add("is_not_null", document.NewBoolValue(f.IsNotNull))
	if f.HasDefaultValue() {
		buf.Add("default_value", f.DefaultValue)
	}
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

	v, err = d.GetByField("default_value")
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if err == nil {
		f.DefaultValue = v
	}

	return nil
}

// TableInfo contains information about a table.
type TableInfo struct {
	// name of the table.
	tableName string
	// name of the store associated with the table.
	storeName []byte
	readOnly  bool
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

// ToDocument turns ti into a document.
func (ti *TableInfo) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("table_name", document.NewTextValue(ti.tableName))
	buf.Add("store_name", document.NewBlobValue(ti.storeName))

	vbuf := document.NewValueBuffer()
	for _, fc := range ti.FieldConstraints {
		vbuf = vbuf.Append(document.NewDocumentValue(fc.ToDocument()))
	}

	buf.Add("field_constraints", document.NewArrayValue(vbuf))

	buf.Add("read_only", document.NewBoolValue(ti.readOnly))
	return buf
}

// ScanDocument decodes d into ti.
func (ti *TableInfo) ScanDocument(d document.Document) error {
	v, err := d.GetByField("table_name")
	if err != nil {
		return err
	}
	ti.tableName = v.V.(string)

	v, err = d.GetByField("store_name")
	if err != nil {
		return err
	}
	ti.storeName = make([]byte, len(v.V.([]byte)))
	copy(ti.storeName, v.V.([]byte))

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
		return ti.FieldConstraints[i].ScanDocument(value.V.(document.Document))
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
	db *Database
	// tableInfos contains information about all the tables
	tableInfos map[string]TableInfo

	mu sync.RWMutex
}

func newTableInfoStore(db *Database, tx engine.Transaction) (*tableInfoStore, error) {
	ts := tableInfoStore{
		db: db,
	}

	err := ts.loadAllTableInfo(tx)
	if err != nil {
		return nil, err
	}

	return &ts, nil
}

// Insert a new tableInfo for the given table name.
// If info.storeName is nil, it generates one and stores it in info.
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

	st, err := tx.tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return err
	}

	if info.storeName == nil {
		seq, err := st.NextSequence()
		if err != nil {
			return err
		}
		buf := make([]byte, binary.MaxVarintLen64+1)
		buf[0] = storePrefix
		n := binary.PutUvarint(buf[1:], seq)
		info.storeName = buf[:n+1]
	}

	var buf bytes.Buffer
	err = t.db.Codec.NewEncoder(&buf).EncodeDocument(info.ToDocument())
	if err != nil {
		return err
	}

	err = st.Put([]byte(tableName), buf.Bytes())
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
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	}

	if info.transactionID != 0 && info.transactionID != tx.id {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	}

	return &info, nil
}

func (t *tableInfoStore) Delete(tx *Transaction, tableName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.tableInfos[tableName]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	}

	if info.transactionID != 0 && info.transactionID != tx.id {
		return fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	}

	st, err := tx.tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return err
	}

	key := []byte(tableName)
	err = st.Delete(key)
	if err == engine.ErrKeyNotFound {
		return fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	}
	if err != nil {
		return err
	}

	delete(t.tableInfos, tableName)

	return nil
}

// modifyTable modifies TableInfo using given callback.
func (t *tableInfoStore) modifyTable(tx *Transaction, tableName string, f func(*TableInfo) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.tableInfos[tableName]
	if !ok {
		return ErrTableNotFound
	}

	if info.transactionID != 0 && info.transactionID != tx.id {
		return ErrTableNotFound
	}

	err := f(&info)
	if err != nil {
		return err
	}

	st, err := tx.tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	err = t.db.Codec.NewEncoder(&buf).EncodeDocument(info.ToDocument())
	if err != nil {
		return err
	}

	key := []byte(tableName)
	err = st.Delete(key)
	if err != nil {
		return err
	}

	err = st.Put(key, buf.Bytes())
	if err != nil {
		return err
	}

	info.transactionID = tx.id
	t.tableInfos[tableName] = info

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
		err = ti.ScanDocument(t.db.Codec.NewDocument(b))
		if err != nil {
			return err
		}

		t.tableInfos[string(itm.Key())] = ti
	}

	t.tableInfos[tableInfoStoreName] = TableInfo{
		storeName: []byte(tableInfoStoreName),
		readOnly:  true,
		FieldConstraints: []FieldConstraint{
			{
				Path: document.ValuePath{
					document.ValuePathFragment{
						FieldName: "table_name",
					},
				},
				IsPrimaryKey: true,
			},
		},
	}

	t.tableInfos[indexStoreName] = TableInfo{
		storeName: []byte(indexStoreName),
		readOnly:  true,
		FieldConstraints: []FieldConstraint{
			{
				Path: document.ValuePath{
					document.ValuePathFragment{
						FieldName: "index_name",
					},
				},
				IsPrimaryKey: true,
			},
		},
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

// IndexConfig holds the configuration of an index.
type IndexConfig struct {
	TableName string
	IndexName string
	Path      document.ValuePath

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	// If set, the index is typed and only accepts that type
	Type document.ValueType
}

// ToDocument creates a document from an IndexConfig.
func (i *IndexConfig) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("unique", document.NewBoolValue(i.Unique))
	buf.Add("index_name", document.NewTextValue(i.IndexName))
	buf.Add("table_name", document.NewTextValue(i.TableName))
	buf.Add("path", document.NewArrayValue(valuePathToArray(i.Path)))
	if i.Type != 0 {
		buf.Add("type", document.NewIntegerValue(int64(i.Type)))
	}
	return buf
}

// ScanDocument implements the document.Scanner interface.
func (i *IndexConfig) ScanDocument(d document.Document) error {
	v, err := d.GetByField("unique")
	if err != nil {
		return err
	}
	i.Unique = v.V.(bool)

	v, err = d.GetByField("index_name")
	if err != nil {
		return err
	}
	i.IndexName = string(v.V.(string))

	v, err = d.GetByField("table_name")
	if err != nil {
		return err
	}
	i.TableName = string(v.V.(string))

	v, err = d.GetByField("path")
	if err != nil {
		return err
	}
	i.Path, err = arrayToValuePath(v)
	if err != nil {
		return err
	}

	v, err = d.GetByField("type")
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if err == nil {
		i.Type = document.ValueType(v.V.(int64))
	}

	return nil
}

// Index of a table field. Contains information about
// the index configuration and provides methods to manipulate the index.
type Index struct {
	*index.Index
	Opts IndexConfig
}

type indexStore struct {
	db *Database
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

	var buf bytes.Buffer
	err = t.db.Codec.NewEncoder(&buf).EncodeDocument(cfg.ToDocument())
	if err != nil {
		return err
	}

	return t.st.Put(key, buf.Bytes())
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
	err = idxopts.ScanDocument(t.db.Codec.NewDocument(v))
	if err != nil {
		return nil, err
	}

	return &idxopts, nil
}

func (t *indexStore) Replace(indexName string, cfg IndexConfig) error {
	var buf bytes.Buffer
	err := t.db.Codec.NewEncoder(&buf).EncodeDocument(cfg.ToDocument())
	if err != nil {
		return err
	}

	return t.st.Put([]byte(indexName), buf.Bytes())
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

	var buf []byte
	var err error
	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		var opts IndexConfig
		buf, err = item.ValueCopy(buf)
		if err != nil {
			it.Close()
			return nil, err
		}

		err = opts.ScanDocument(t.db.Codec.NewDocument(buf))
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
		if value.Type == document.TextValue {
			path = append(path, document.ValuePathFragment{FieldName: value.V.(string)})
		} else {
			path = append(path, document.ValuePathFragment{ArrayIndex: int(value.V.(int64))})
		}
		return nil
	})

	return path, err
}

func valuePathToArray(path document.ValuePath) document.Array {
	abuf := document.NewValueBuffer()
	for _, p := range path {
		if p.FieldName != "" {
			abuf = abuf.Append(document.NewTextValue(p.FieldName))
		} else {
			abuf = abuf.Append(document.NewIntegerValue(int64(p.ArrayIndex)))
		}
	}

	return abuf
}

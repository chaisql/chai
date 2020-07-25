package database

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
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
	tp, err := v.ConvertToInt64()
	if err != nil {
		return err
	}
	f.Type = document.ValueType(tp)

	v, err = d.GetByField("is_primary_key")
	if err != nil {
		return err
	}
	f.IsPrimaryKey, err = v.ConvertToBool()
	if err != nil {
		return err
	}

	v, err = d.GetByField("is_not_null")
	if err != nil {
		return err
	}
	f.IsNotNull, err = v.ConvertToBool()
	return err
}

type TableInfo struct {
	// storeID is a generated ID that acts as a key to reference a table.
	// The first-4 bytes represents the timestamp in second and the last-2 bytes are
	// randomly generated.
	storeID [6]byte

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

	buf.Add("storeID", document.NewBlobValue(ti.storeID[:]))

	vbuf := document.NewValueBuffer()
	for _, fc := range ti.FieldConstraints {
		vbuf = vbuf.Append(document.NewDocumentValue(fc.ToDocument()))
	}

	buf.Add("field_constraints", document.NewArrayValue(vbuf))

	return buf
}

func (ti *TableInfo) ScanDocument(d document.Document) error {
	v, err := d.GetByField("storeID")
	if err != nil {
		return err
	}
	b, err := v.ConvertToBytes()
	if err != nil {
		return err
	}
	copy(ti.storeID[:], b)

	v, err = d.GetByField("field_constraints")
	if err != nil {
		return err
	}
	ar, err := v.ConvertToArray()
	if err != nil {
		return err
	}

	l, err := document.ArrayLength(ar)
	if err != nil {
		return err
	}

	ti.FieldConstraints = make([]FieldConstraint, l)

	return ar.Iterate(func(i int, value document.Value) error {
		doc, err := value.ConvertToDocument()
		if err != nil {
			return err
		}
		return ti.FieldConstraints[i].ScanDocument(doc)
	})
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
	ts := tableInfoStore{
		tableInfos: make(map[string]TableInfo),
	}

	err := ts.loadAllTableInfo(tx)
	if err != nil {
		return nil, err
	}

	return &ts, nil
}

// Insert a new tableInfo for the given table name.
// It automatically generates a unique storeID for that table.
func (t *tableInfoStore) Insert(tx engine.Transaction, tableName string, info *TableInfo) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.tableInfos[tableName]
	if ok {
		return nil, ErrTableAlreadyExists
	}

	var found bool = true
	var id [6]byte
	for found {
		id = generateStoreID()

		found = false
		for _, ti := range t.tableInfos {
			if ti.storeID == id {
				// A store with this id already exists.
				// Let's generate a new one.
				found = true
				break
			}
		}
	}
	info.storeID = id

	v, err := msgpack.EncodeDocument(info.ToDocument())
	if err != nil {
		return nil, err
	}

	st, err := tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		return nil, err
	}

	err = st.Put([]byte(tableName), v)
	if err != nil {
		return nil, err
	}

	t.tableInfos[tableName] = *info
	return info.storeID[:], err
}

func (t *tableInfoStore) Get(tableName string) (*TableInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, ok := t.tableInfos[tableName]
	if !ok {
		return nil, ErrTableNotFound
	}

	return &info, nil
}

func (t *tableInfoStore) Delete(tx engine.Transaction, tableName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, err := tx.GetStore([]byte(tableInfoStoreName))
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

	return err
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

	return nil
}

// ListTables lists all the tables.
// The returned slice is lexicographically ordered.
func (t *tableInfoStore) ListTables() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	names := make([]string, len(t.tableInfos))
	var i int
	for k := range t.tableInfos {
		names[i] = k
		i++
	}

	sort.Strings(names)

	return names
}

func generateStoreID() [6]byte {
	var id [6]byte

	binary.BigEndian.PutUint32(id[:], uint32(time.Now().Unix()))
	if _, err := rand.Reader.Read(id[4:]); err != nil {
		panic(fmt.Errorf("cannot generate random number: %v;", err))
	}

	return id
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
	i.Unique, err = v.ConvertToBool()
	if err != nil {
		return err
	}

	v, err = d.GetByField("indexname")
	if err != nil {
		return err
	}
	i.IndexName, err = v.ConvertToString()
	if err != nil {
		return err
	}

	v, err = d.GetByField("tablename")
	if err != nil {
		return err
	}
	i.TableName, err = v.ConvertToString()
	if err != nil {
		return err
	}

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
	ar, err := v.ConvertToArray()
	if err != nil {
		return nil, err
	}

	var path document.ValuePath

	err = ar.Iterate(func(_ int, value document.Value) error {
		p, err := value.ConvertToString()
		if err != nil {
			return err
		}

		path = append(path, p)
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

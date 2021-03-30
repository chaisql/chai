package database

import (
	"bytes"
	"encoding/binary"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/stringutil"
)

const storePrefix = 't'

// TableInfo contains information about a table.
type TableInfo struct {
	// name of the table.
	tableName string
	// name of the store associated with the table.
	storeName []byte
	readOnly  bool

	FieldConstraints FieldConstraints
}

// GetPrimaryKey returns the field constraint of the primary key.
// Returns nil if there is no primary key.
func (ti *TableInfo) GetPrimaryKey() *FieldConstraint {
	for _, f := range ti.FieldConstraints {
		if f.IsPrimaryKey {
			return f
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

	ti.FieldConstraints = make([]*FieldConstraint, l)

	err = ar.Iterate(func(i int, value document.Value) error {
		var fc FieldConstraint
		err := fc.ScanDocument(value.V.(document.Document))
		if err != nil {
			return err
		}

		ti.FieldConstraints[i] = &fc
		return nil
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

// Clone creates another tableInfo with the same values.
func (ti *TableInfo) Clone() *TableInfo {
	cp := *ti
	cp.FieldConstraints = nil
	cp.FieldConstraints = append(cp.FieldConstraints, ti.FieldConstraints...)
	return &cp
}

// tableStore manages table information.
// It loads table information during database startup
// and holds it in memory.
type tableStore struct {
	db *Database
	st engine.Store
}

// List all tables.
func (t *tableStore) ListAll() ([]*TableInfo, error) {
	it := t.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var list []*TableInfo
	var buf []byte
	var err error

	for it.Seek(nil); it.Valid(); it.Next() {
		itm := it.Item()
		buf, err = itm.ValueCopy(buf)
		if err != nil {
			return nil, err
		}

		var ti TableInfo
		err = ti.ScanDocument(t.db.Codec.NewDocument(buf))
		if err != nil {
			return nil, err
		}

		list = append(list, &ti)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return list, nil
}

// Insert a new tableInfo for the given table name.
// If info.storeName is nil, it generates one and stores it in info.
func (t *tableStore) Insert(tx *Transaction, tableName string, info *TableInfo) error {
	tblName := []byte(tableName)

	_, err := t.st.Get(tblName)
	if err == nil {
		return ErrTableAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	if info.storeName == nil {
		seq, err := t.st.NextSequence()
		if err != nil {
			return err
		}
		buf := make([]byte, binary.MaxVarintLen64+1)
		buf[0] = storePrefix
		n := binary.PutUvarint(buf[1:], seq)
		info.storeName = buf[:n+1]
	}

	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(info.ToDocument())
	if err != nil {
		return err
	}

	err = t.st.Put([]byte(tableName), buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (t *tableStore) Delete(tx *Transaction, tableName string) error {
	err := t.st.Delete([]byte(tableName))
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return stringutil.Errorf("%w: %q", ErrTableNotFound, tableName)
		}

		return err
	}

	return nil
}

// Replace replaces tableName table information with the new info.
func (t *tableStore) Replace(tx *Transaction, tableName string, info *TableInfo) error {
	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err := enc.EncodeDocument(info.ToDocument())
	if err != nil {
		return err
	}

	tbName := []byte(tableName)
	_, err = t.st.Get(tbName)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return stringutil.Errorf("%w: %q", ErrTableNotFound, tableName)
		}

		return err
	}

	return t.st.Put(tbName, buf.Bytes())
}

// IndexInfo holds the configuration of an index.
type IndexInfo struct {
	TableName string
	IndexName string
	Paths     []document.Path

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	// If set, the index is typed and only accepts values of those types	.
	Types []document.ValueType
}

// ToDocument creates a document from an IndexConfig.
func (i *IndexInfo) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("unique", document.NewBoolValue(i.Unique))
	buf.Add("index_name", document.NewTextValue(i.IndexName))
	buf.Add("table_name", document.NewTextValue(i.TableName))

	vb := document.NewValueBuffer()
	for _, path := range i.Paths {
		vb.Append(document.NewArrayValue(pathToArray(path)))
	}

	buf.Add("paths", document.NewArrayValue(vb))
	if i.Types != nil {
		types := make([]document.Value, 0, len(i.Types))
		for _, typ := range i.Types {
			types = append(types, document.NewIntegerValue(int64(typ)))
		}
		buf.Add("types", document.NewArrayValue(document.NewValueBuffer(types...)))
	}
	return buf
}

// ScanDocument implements the document.Scanner interface.
func (i *IndexInfo) ScanDocument(d document.Document) error {
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

	v, err = d.GetByField("paths")
	if err != nil {
		return err
	}

	i.Paths = nil
	err = v.V.(document.Array).Iterate(func(ii int, pval document.Value) error {
		p, err := arrayToPath(pval.V.(document.Array))
		if err != nil {
			return err
		}

		i.Paths = append(i.Paths, p)
		return nil
	})

	if err != nil {
		return err
	}

	v, err = d.GetByField("types")
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}

	if err == nil {
		i.Types = nil
		err = v.V.(document.Array).Iterate(func(ii int, tval document.Value) error {
			i.Types = append(i.Types, document.ValueType(tval.V.(int64)))
			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// Clone returns a copy of the index information.
func (i IndexInfo) Clone() *IndexInfo {
	c := i

	c.Paths = make([]document.Path, len(i.Paths))
	for i, p := range i.Paths {
		c.Paths[i] = p.Clone()
	}

	c.Types = make([]document.ValueType, len(i.Types))
	copy(c.Types, i.Types)

	return &c
}

type indexStore struct {
	db *Database
	st engine.Store
}

func (t *indexStore) Insert(cfg *IndexInfo) error {
	key := []byte(cfg.IndexName)
	_, err := t.st.Get(key)
	if err == nil {
		return ErrIndexAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(cfg.ToDocument())
	if err != nil {
		return err
	}

	return t.st.Put(key, buf.Bytes())
}

func (t *indexStore) Get(indexName string) (*IndexInfo, error) {
	key := []byte(indexName)
	v, err := t.st.Get(key)
	if err == engine.ErrKeyNotFound {
		return nil, ErrIndexNotFound
	}
	if err != nil {
		return nil, err
	}

	var idxopts IndexInfo
	err = idxopts.ScanDocument(t.db.Codec.NewDocument(v))
	if err != nil {
		return nil, err
	}

	return &idxopts, nil
}

func (t *indexStore) Replace(indexName string, cfg IndexInfo) error {
	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err := enc.EncodeDocument(cfg.ToDocument())
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

func (t *indexStore) ListAll() ([]*IndexInfo, error) {
	it := t.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var idxList []*IndexInfo
	var buf []byte
	var err error
	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		buf, err = item.ValueCopy(buf)
		if err != nil {
			return nil, err
		}

		var opts IndexInfo
		err = opts.ScanDocument(t.db.Codec.NewDocument(buf))
		if err != nil {
			return nil, err
		}

		idxList = append(idxList, &opts)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return idxList, nil
}

type Indexes []*Index

func (i Indexes) GetIndex(name string) *Index {
	for _, idx := range i {
		if idx.Info.IndexName == name {
			return idx
		}
	}

	return nil
}

func (i Indexes) GetIndexByPath(p document.Path) *Index {
	for _, idx := range i {
		if idx.Info.Paths[0].IsEqual(p) {
			return idx
		}
	}

	return nil
}

func arrayToPath(a document.Array) (document.Path, error) {
	var path document.Path

	err := a.Iterate(func(_ int, value document.Value) error {
		if value.Type == document.TextValue {
			path = append(path, document.PathFragment{FieldName: value.V.(string)})
		} else {
			path = append(path, document.PathFragment{ArrayIndex: int(value.V.(int64))})
		}
		return nil
	})

	return path, err
}

func pathToArray(path document.Path) document.Array {
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

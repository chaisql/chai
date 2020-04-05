package database

import (
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
)

// TableConfig holds the configuration of a table
type TableConfig struct {
	FieldConstraints []FieldConstraint

	LastKey int64
}

// ToDocument returns a document from t.
func (t *TableConfig) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	vbuf := document.NewValueBuffer()
	for _, fc := range t.FieldConstraints {
		vbuf = vbuf.Append(document.NewDocumentValue(fc.ToDocument()))
	}

	buf.Add("field_constraints", document.NewArrayValue(vbuf))
	buf.Add("last_key", document.NewInt64Value(t.LastKey))
	return buf
}

// ScanDocument implements the document.Scanner interface.
func (t *TableConfig) ScanDocument(d document.Document) error {
	v, err := d.GetByField("field_constraints")
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

	t.FieldConstraints = make([]FieldConstraint, l)

	err = ar.Iterate(func(i int, value document.Value) error {
		doc, err := value.ConvertToDocument()
		if err != nil {
			return err
		}
		return t.FieldConstraints[i].ScanDocument(doc)
	})
	if err != nil {
		return err
	}

	v, err = d.GetByField("last_key")
	if err != nil {
		return err
	}
	t.LastKey, err = v.ConvertToInt64()
	return err
}

// GetPrimaryKey returns the field constraint of the primary key.
// Returns nil if there is no primary key.
func (t TableConfig) GetPrimaryKey() *FieldConstraint {
	for _, f := range t.FieldConstraints {
		if f.IsPrimaryKey {
			return &f
		}
	}

	return nil
}

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
	buf.Add("type", document.NewIntValue(int(f.Type)))
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

type tableConfigStore struct {
	st engine.Store
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

	v, err := encoding.EncodeDocument(cfg.ToDocument())
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

	v, err := encoding.EncodeDocument(cfg.ToDocument())
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

	err = cfg.ScanDocument(encoding.EncodedDocument(v))
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

// IndexConfig holds the configuration of an index.
type IndexConfig struct {
	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	IndexName string
	TableName string
	Path      document.ValuePath
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
	i.IndexName, err = v.ConvertToText()
	if err != nil {
		return err
	}

	v, err = d.GetByField("tablename")
	if err != nil {
		return err
	}
	i.TableName, err = v.ConvertToText()
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

	IndexName string
	TableName string
	Path      document.ValuePath
	Unique    bool
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

	v, err := encoding.EncodeDocument(cfg.ToDocument())
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
	err = idxopts.ScanDocument(encoding.EncodedDocument(v))
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

func arrayToValuePath(v document.Value) (document.ValuePath, error) {
	ar, err := v.ConvertToArray()
	if err != nil {
		return nil, err
	}

	var path document.ValuePath

	err = ar.Iterate(func(_ int, value document.Value) error {
		p, err := value.ConvertToText()
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

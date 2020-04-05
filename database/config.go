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

	doc, err := document.NewFromStruct(cfg)
	if err != nil {
		return err
	}

	v, err := encoding.EncodeDocument(doc)
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

	doc, err := document.NewFromStruct(cfg)
	if err != nil {
		return err
	}

	v, err := encoding.EncodeDocument(doc)
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

	err = document.StructScan(encoding.EncodedDocument(v), &cfg)
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
	err = document.StructScan(encoding.EncodedDocument(v), &idxopts)
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

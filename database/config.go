package database

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/value"
)


// TableConfig holds the configuration of a table
type TableConfig struct {
	PrimaryKeyName string
	PrimaryKeyType value.Type

	lastKey int64
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

	var fb document.FieldBuffer
	fb.Add(document.NewStringField("PrimaryKeyName", cfg.PrimaryKeyName))
	fb.Add(document.NewUint8Field("PrimaryKeyType", uint8(cfg.PrimaryKeyType)))
	fb.Add(document.NewInt64Field("lastKey", cfg.lastKey))

	v, err := document.Encode(&fb)
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

	var fb document.FieldBuffer
	fb.Add(document.NewStringField("PrimaryKeyName", cfg.PrimaryKeyName))
	fb.Add(document.NewUint8Field("PrimaryKeyType", uint8(cfg.PrimaryKeyType)))
	fb.Add(document.NewInt64Field("lastKey", cfg.lastKey))

	v, err := document.Encode(&fb)
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

	r := document.EncodedRecord(v)

	f, err := r.GetValueByName("PrimaryKeyName")
	if err != nil {
		return nil, err
	}
	cfg.PrimaryKeyName, err = f.DecodeToString()
	if err != nil {
		return nil, err
	}
	f, err = r.GetValueByName("PrimaryKeyType")
	if err != nil {
		return nil, err
	}
	tp, err := f.DecodeToUint8()
	if err != nil {
		return nil, err
	}
	cfg.PrimaryKeyType = value.Type(tp)

	f, err = r.GetValueByName("lastKey")
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

func (t *indexStore) Insert(cfg indexOptions) error {
	key := []byte(buildIndexName(cfg.IndexName))
	_, err := t.st.Get(key)
	if err == nil {
		return ErrIndexAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	v, err := document.Encode(&cfg)
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
	err = idxopts.ScanRecord(document.EncodedRecord(v))
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

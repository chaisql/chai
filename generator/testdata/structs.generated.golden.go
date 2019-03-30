package testdata

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// Field implements the field method of the record.Record interface.
func (b *Basic) Field(name string) (field.Field, error) {
	switch name {
	case "A":
		return field.Field{
			Name: "A",
			Type: field.String,
			Data: []byte(b.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(b.B),
		}, nil
	case "C":
		return field.Field{
			Name: "C",
			Type: field.Int64,
			Data: field.EncodeInt64(b.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.Int64,
			Data: field.EncodeInt64(b.D),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (b *Basic) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, _ = b.Field("A")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = b.Field("B")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = b.Field("C")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = b.Field("D")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (b *Basic) ScanRecord(rec record.Record) error {
	var f field.Field
	var err error

	f, err = rec.Field("A")
	if err == nil {
		b.A = string(f.Data)
	}

	f, err = rec.Field("B")
	if err == nil {
		b.B, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
	}

	f, err = rec.Field("C")
	if err == nil {
		b.C, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
	}

	f, err = rec.Field("D")
	if err == nil {
		b.D, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

// BasicStore manages the table. It provides several typed helpers
// that simplify common operations.
type BasicStore struct {
	store *genji.StaticStore
}

// NewBasicStore creates a BasicStore.
func NewBasicStore(db *genji.DB) *BasicStore {
	schema := record.Schema{
		TableName: "Basic",
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &BasicStore{store: genji.NewStaticStore(db, "Basic", schema)}
}

// NewBasicStoreWithTx creates a BasicStore valid for the lifetime of the given transaction.
func NewBasicStoreWithTx(tx *genji.Tx) *BasicStore {
	schema := record.Schema{
		TableName: "Basic",
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &BasicStore{store: genji.NewStaticStoreWithTx(tx, "Basic", schema)}
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (b *BasicStore) Init() error {
	return b.store.Init()
}

// Insert a record in the table and return the primary key.
func (b *BasicStore) Insert(record *Basic) (rowid []byte, err error) {
	return b.store.Insert(record)
}

// Get a record using its primary key.
func (b *BasicStore) Get(rowid []byte) (*Basic, error) {
	var record Basic

	return &record, b.store.Get(rowid, &record)
}

// Field implements the field method of the record.Record interface.
func (b *basic) Field(name string) (field.Field, error) {
	switch name {
	case "A":
		return field.Field{
			Name: "A",
			Type: field.String,
			Data: []byte(b.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(b.B),
		}, nil
	case "C":
		return field.Field{
			Name: "C",
			Type: field.Int64,
			Data: field.EncodeInt64(b.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.Int64,
			Data: field.EncodeInt64(b.D),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (b *basic) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, _ = b.Field("A")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = b.Field("B")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = b.Field("C")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = b.Field("D")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (b *basic) ScanRecord(rec record.Record) error {
	var f field.Field
	var err error

	f, err = rec.Field("A")
	if err == nil {
		b.A = string(f.Data)
	}

	f, err = rec.Field("B")
	if err == nil {
		b.B, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
	}

	f, err = rec.Field("C")
	if err == nil {
		b.C, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
	}

	f, err = rec.Field("D")
	if err == nil {
		b.D, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

// basicStore manages the table. It provides several typed helpers
// that simplify common operations.
type basicStore struct {
	store *genji.StaticStore
}

// newBasicStore creates a basicStore.
func newBasicStore(db *genji.DB) *basicStore {
	schema := record.Schema{
		TableName: "basic",
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &basicStore{store: genji.NewStaticStore(db, "basic", schema)}
}

// newBasicStoreWithTx creates a basicStore valid for the lifetime of the given transaction.
func newBasicStoreWithTx(tx *genji.Tx) *basicStore {
	schema := record.Schema{
		TableName: "basic",
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &basicStore{store: genji.NewStaticStoreWithTx(tx, "basic", schema)}
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (b *basicStore) Init() error {
	return b.store.Init()
}

// Insert a record in the table and return the primary key.
func (b *basicStore) Insert(record *basic) (rowid []byte, err error) {
	return b.store.Insert(record)
}

// Get a record using its primary key.
func (b *basicStore) Get(rowid []byte) (*basic, error) {
	var record basic

	return &record, b.store.Get(rowid, &record)
}

// Field implements the field method of the record.Record interface.
func (p *Pk) Field(name string) (field.Field, error) {
	switch name {
	case "A":
		return field.Field{
			Name: "A",
			Type: field.String,
			Data: []byte(p.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(p.B),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (p *Pk) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, _ = p.Field("A")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = p.Field("B")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (p *Pk) ScanRecord(rec record.Record) error {
	var f field.Field
	var err error

	f, err = rec.Field("A")
	if err == nil {
		p.A = string(f.Data)
	}

	f, err = rec.Field("B")
	if err == nil {
		p.B, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

// Pk returns the primary key. It implements the table.Pker interface.
func (p *Pk) Pk() ([]byte, error) {
	return field.EncodeInt64(p.B), nil
}

// PkStore manages the table. It provides several typed helpers
// that simplify common operations.
type PkStore struct {
	store *genji.StaticStore
}

// NewPkStore creates a PkStore.
func NewPkStore(db *genji.DB) *PkStore {
	schema := record.Schema{
		TableName: "Pk",
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	return &PkStore{store: genji.NewStaticStore(db, "Pk", schema)}
}

// NewPkStoreWithTx creates a PkStore valid for the lifetime of the given transaction.
func NewPkStoreWithTx(tx *genji.Tx) *PkStore {
	schema := record.Schema{
		TableName: "Pk",
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	return &PkStore{store: genji.NewStaticStoreWithTx(tx, "Pk", schema)}
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (p *PkStore) Init() error {
	return p.store.Init()
}

// Insert a record in the table and return the primary key.
func (p *PkStore) Insert(record *Pk) (err error) {
	_, err = p.store.Insert(record)
	return err
}

// Get a record using its primary key.
func (p *PkStore) Get(pk int64) (*Pk, error) {
	var record Pk
	rowid := field.EncodeInt64(pk)

	return &record, p.store.Get(rowid, &record)
}

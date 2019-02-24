package testdata

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/table"
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

// BasicSelector provides helpers for selecting fields from the Basic structure.
type BasicSelector struct{}

// NewBasicSelector creates a BasicSelector.
func NewBasicSelector() BasicSelector {
	return BasicSelector{}
}

// A returns a string selector.
func (BasicSelector) A() query.StrField {
	return query.NewStrField("A")
}

// B returns an int64 selector.
func (BasicSelector) B() query.Int64Field {
	return query.NewInt64Field("B")
}

// C returns an int64 selector.
func (BasicSelector) C() query.Int64Field {
	return query.NewInt64Field("C")
}

// D returns an int64 selector.
func (BasicSelector) D() query.Int64Field {
	return query.NewInt64Field("D")
}

// BasicTable manages the table. It provides several typed helpers
// that simplify common operations.
type BasicTable struct {
	tx *genji.Tx
	t  table.Table
}

// NewBasicTable creates a BasicTable valid for the lifetime of the given transaction.
func NewBasicTable(tx *genji.Tx) *BasicTable {
	return &BasicTable{
		tx: tx,
	}
}

func (b *BasicTable) ensureTable() error {
	if b.t != nil {
		return nil
	}

	var err error

	b.t, err = b.tx.Table("Basic")

	return err
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (b *BasicTable) Init() error {
	var err error

	b.t, err = b.tx.CreateTable("Basic")
	if err == engine.ErrTableAlreadyExists {
		return nil
	}

	return err
}

// Insert a record in the table and return the primary key.
func (b *BasicTable) Insert(record *Basic) (rowid []byte, err error) {
	err = b.ensureTable()
	if err != nil {
		return
	}
	return b.t.Insert(record)
}

// Get a record using its primary key.
func (b *BasicTable) Get(rowid []byte) (record *Basic, err error) {
	err = b.ensureTable()
	if err != nil {
		return
	}

	rec, err := b.t.Record(rowid)
	if err != nil {
		return
	}

	record = new(Basic)

	var f field.Field

	f, err = rec.Field("A")
	if err != nil {
		return
	}
	record.A = string(f.Data)

	f, err = rec.Field("B")
	if err != nil {
		return
	}
	record.B, err = field.DecodeInt64(f.Data)

	f, err = rec.Field("C")
	if err != nil {
		return
	}
	record.C, err = field.DecodeInt64(f.Data)

	f, err = rec.Field("D")
	if err != nil {
		return
	}
	record.D, err = field.DecodeInt64(f.Data)

	return
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

// basicSelector provides helpers for selecting fields from the basic structure.
type basicSelector struct{}

// newBasicSelector creates a basicSelector.
func newBasicSelector() basicSelector {
	return basicSelector{}
}

// A returns a string selector.
func (basicSelector) A() query.StrField {
	return query.NewStrField("A")
}

// B returns an int64 selector.
func (basicSelector) B() query.Int64Field {
	return query.NewInt64Field("B")
}

// C returns an int64 selector.
func (basicSelector) C() query.Int64Field {
	return query.NewInt64Field("C")
}

// D returns an int64 selector.
func (basicSelector) D() query.Int64Field {
	return query.NewInt64Field("D")
}

// basicTable manages the table. It provides several typed helpers
// that simplify common operations.
type basicTable struct {
	tx *genji.Tx
	t  table.Table
}

// newBasicTable creates a basicTable valid for the lifetime of the given transaction.
func newBasicTable(tx *genji.Tx) *basicTable {
	return &basicTable{
		tx: tx,
	}
}

func (b *basicTable) ensureTable() error {
	if b.t != nil {
		return nil
	}

	var err error

	b.t, err = b.tx.Table("basic")

	return err
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (b *basicTable) Init() error {
	var err error

	b.t, err = b.tx.CreateTable("basic")
	if err == engine.ErrTableAlreadyExists {
		return nil
	}

	return err
}

// Insert a record in the table and return the primary key.
func (b *basicTable) Insert(record *basic) (rowid []byte, err error) {
	err = b.ensureTable()
	if err != nil {
		return
	}
	return b.t.Insert(record)
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

// Pk returns the primary key. It implements the table.Pker interface.
func (p *Pk) Pk() ([]byte, error) {
	return field.EncodeInt64(p.B), nil
}

// PkSelector provides helpers for selecting fields from the Pk structure.
type PkSelector struct{}

// NewPkSelector creates a PkSelector.
func NewPkSelector() PkSelector {
	return PkSelector{}
}

// A returns a string selector.
func (PkSelector) A() query.StrField {
	return query.NewStrField("A")
}

// B returns an int64 selector.
func (PkSelector) B() query.Int64Field {
	return query.NewInt64Field("B")
}

// PkTable manages the table. It provides several typed helpers
// that simplify common operations.
type PkTable struct {
	tx *genji.Tx
	t  table.Table
}

// NewPkTable creates a PkTable valid for the lifetime of the given transaction.
func NewPkTable(tx *genji.Tx) *PkTable {
	return &PkTable{
		tx: tx,
	}
}

func (p *PkTable) ensureTable() error {
	if p.t != nil {
		return nil
	}

	var err error

	p.t, err = p.tx.Table("Pk")

	return err
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (p *PkTable) Init() error {
	var err error

	p.t, err = p.tx.CreateTable("Pk")
	if err == engine.ErrTableAlreadyExists {
		return nil
	}

	return err
}

// Insert a record in the table and return the primary key.
func (p *PkTable) Insert(record *Pk) (err error) {
	err = p.ensureTable()
	if err != nil {
		return
	}
	_, err = p.t.Insert(record)
	return
}

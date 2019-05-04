package testdata

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
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
	*genji.Store
}

// NewBasicStore creates a BasicStore.
func NewBasicStore(db *genji.DB) *BasicStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &BasicStore{Store: genji.NewStore(db, "Basic", &schema)}
}

// NewBasicStoreWithTx creates a BasicStore valid for the lifetime of the given transaction.
func NewBasicStoreWithTx(tx *genji.Tx) *BasicStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &BasicStore{Store: genji.NewStoreWithTx(tx, "Basic", &schema)}
}

// Insert a record in the table and return the primary key.
func (b *BasicStore) Insert(record *Basic) (rowid []byte, err error) {
	return b.Store.Insert(record)
}

// Get a record using its primary key.
func (b *BasicStore) Get(rowid []byte) (*Basic, error) {
	var record Basic

	return &record, b.Store.Get(rowid, &record)
}

// List records from the specified offset. If the limit is equal to -1, it returns all records after the selected offset.
func (b *BasicStore) List(offset, limit int) ([]Basic, error) {
	size := limit
	if size == -1 {
		size = 0
	}
	list := make([]Basic, 0, size)
	err := b.Store.List(offset, limit, func(rowid []byte, r record.Record) error {
		var record Basic
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}
		list = append(list, record)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (b *BasicStore) Replace(rowid []byte, record *Basic) error {
	return b.Store.Replace(rowid, record)
}

// BasicQuerySelector provides helpers for selecting fields from the Basic structure.
type BasicQuerySelector struct {
	A query.StrField
	B query.Int64Field
	C query.Int64Field
	D query.Int64Field
}

// NewBasicQuerySelector creates a BasicQuerySelector.
func NewBasicQuerySelector() BasicQuerySelector {
	return BasicQuerySelector{
		A: query.NewStrField("A"),
		B: query.NewInt64Field("B"),
		C: query.NewInt64Field("C"),
		D: query.NewInt64Field("D"),
	}
}

// Table returns a query.TableSelector for Basic.
func (*BasicQuerySelector) Table() query.TableSelector {
	return query.Table("Basic")
}

// All returns a list of all selectors for Basic.
func (s *BasicQuerySelector) All() []query.FieldSelector {
	return []query.FieldSelector{
		s.A,
		s.B,
		s.C,
		s.D,
	}
}

// BasicResult can be used to store the result of queries.
// Selected fields must map the Basic fields.
type BasicResult []Basic

// ScanTable iterates over table.Reader and stores all the records in the slice.
func (b *BasicResult) ScanTable(tr table.Reader) error {
	return tr.Iterate(func(_ []byte, r record.Record) error {
		var record Basic
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}

		*b = append(*b, record)
		return nil
	})
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
	*genji.Store
}

// newBasicStore creates a basicStore.
func newBasicStore(db *genji.DB) *basicStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &basicStore{Store: genji.NewStore(db, "basic", &schema)}
}

// newBasicStoreWithTx creates a basicStore valid for the lifetime of the given transaction.
func newBasicStoreWithTx(tx *genji.Tx) *basicStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
			{Name: "C", Type: field.Int64},
			{Name: "D", Type: field.Int64},
		},
	}

	return &basicStore{Store: genji.NewStoreWithTx(tx, "basic", &schema)}
}

// Insert a record in the table and return the primary key.
func (b *basicStore) Insert(record *basic) (rowid []byte, err error) {
	return b.Store.Insert(record)
}

// Get a record using its primary key.
func (b *basicStore) Get(rowid []byte) (*basic, error) {
	var record basic

	return &record, b.Store.Get(rowid, &record)
}

// List records from the specified offset. If the limit is equal to -1, it returns all records after the selected offset.
func (b *basicStore) List(offset, limit int) ([]basic, error) {
	size := limit
	if size == -1 {
		size = 0
	}
	list := make([]basic, 0, size)
	err := b.Store.List(offset, limit, func(rowid []byte, r record.Record) error {
		var record basic
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}
		list = append(list, record)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (b *basicStore) Replace(rowid []byte, record *basic) error {
	return b.Store.Replace(rowid, record)
}

// basicQuerySelector provides helpers for selecting fields from the basic structure.
type basicQuerySelector struct {
	A query.StrField
	B query.Int64Field
	C query.Int64Field
	D query.Int64Field
}

// newbasicQuerySelector creates a basicQuerySelector.
func newBasicQuerySelector() basicQuerySelector {
	return basicQuerySelector{
		A: query.NewStrField("A"),
		B: query.NewInt64Field("B"),
		C: query.NewInt64Field("C"),
		D: query.NewInt64Field("D"),
	}
}

// Table returns a query.TableSelector for basic.
func (*basicQuerySelector) Table() query.TableSelector {
	return query.Table("basic")
}

// All returns a list of all selectors for basic.
func (s *basicQuerySelector) All() []query.FieldSelector {
	return []query.FieldSelector{
		s.A,
		s.B,
		s.C,
		s.D,
	}
}

// basicResult can be used to store the result of queries.
// Selected fields must map the basic fields.
type basicResult []basic

// ScanTable iterates over table.Reader and stores all the records in the slice.
func (b *basicResult) ScanTable(tr table.Reader) error {
	return tr.Iterate(func(_ []byte, r record.Record) error {
		var record basic
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}

		*b = append(*b, record)
		return nil
	})
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
	*genji.Store
}

// NewPkStore creates a PkStore.
func NewPkStore(db *genji.DB) *PkStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	return &PkStore{Store: genji.NewStore(db, "Pk", &schema)}
}

// NewPkStoreWithTx creates a PkStore valid for the lifetime of the given transaction.
func NewPkStoreWithTx(tx *genji.Tx) *PkStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	return &PkStore{Store: genji.NewStoreWithTx(tx, "Pk", &schema)}
}

// Insert a record in the table and return the primary key.
func (p *PkStore) Insert(record *Pk) (err error) {
	_, err = p.Store.Insert(record)
	return err
}

// Get a record using its primary key.
func (p *PkStore) Get(pk int64) (*Pk, error) {
	var record Pk
	rowid := field.EncodeInt64(pk)

	return &record, p.Store.Get(rowid, &record)
}

// Delete a record using its primary key.
func (p *PkStore) Delete(pk int64) error {
	rowid := field.EncodeInt64(pk)
	return p.Store.Delete(rowid)
}

// List records from the specified offset. If the limit is equal to -1, it returns all records after the selected offset.
func (p *PkStore) List(offset, limit int) ([]Pk, error) {
	size := limit
	if size == -1 {
		size = 0
	}
	list := make([]Pk, 0, size)
	err := p.Store.List(offset, limit, func(rowid []byte, r record.Record) error {
		var record Pk
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}
		list = append(list, record)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (p *PkStore) Replace(pk int64, record *Pk) error {
	rowid := field.EncodeInt64(pk)
	if record.B == 0 && record.B != pk {
		record.B = pk
	}
	return p.Store.Replace(rowid, record)
}

// PkQuerySelector provides helpers for selecting fields from the Pk structure.
type PkQuerySelector struct {
	A query.StrField
	B query.Int64Field
}

// NewPkQuerySelector creates a PkQuerySelector.
func NewPkQuerySelector() PkQuerySelector {
	return PkQuerySelector{
		A: query.NewStrField("A"),
		B: query.NewInt64Field("B"),
	}
}

// Table returns a query.TableSelector for Pk.
func (*PkQuerySelector) Table() query.TableSelector {
	return query.Table("Pk")
}

// All returns a list of all selectors for Pk.
func (s *PkQuerySelector) All() []query.FieldSelector {
	return []query.FieldSelector{
		s.A,
		s.B,
	}
}

// PkResult can be used to store the result of queries.
// Selected fields must map the Pk fields.
type PkResult []Pk

// ScanTable iterates over table.Reader and stores all the records in the slice.
func (p *PkResult) ScanTable(tr table.Reader) error {
	return tr.Iterate(func(_ []byte, r record.Record) error {
		var record Pk
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}

		*p = append(*p, record)
		return nil
	})
}

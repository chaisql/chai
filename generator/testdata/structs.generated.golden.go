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
			Data: field.EncodeString(b.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Int,
			Data: field.EncodeInt(b.B),
		}, nil
	case "C":
		return field.Field{
			Name: "C",
			Type: field.Int32,
			Data: field.EncodeInt32(b.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.Int32,
			Data: field.EncodeInt32(b.D),
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
	return rec.Iterate(func(f field.Field) error {
		var err error

		switch f.Name {
		case "A":
			b.A, err = field.DecodeString(f.Data)
		case "B":
			b.B, err = field.DecodeInt(f.Data)
		case "C":
			b.C, err = field.DecodeInt32(f.Data)
		case "D":
			b.D, err = field.DecodeInt32(f.Data)
		}
		return err
	})
}

// BasicStore manages the table. It provides several typed helpers
// that simplify common operations.
type BasicStore struct {
	*genji.Store
}

// NewBasicStore creates a BasicStore.
func NewBasicStore(db *genji.DB) *BasicStore {
	var schema *record.Schema
	schema = &record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int},
			{Name: "C", Type: field.Int32},
			{Name: "D", Type: field.Int32},
		},
	}

	var indexes []string

	return &BasicStore{Store: genji.NewStore(db, "Basic", schema, indexes)}
}

// NewBasicStoreWithTx creates a BasicStore valid for the lifetime of the given transaction.
func NewBasicStoreWithTx(tx *genji.Tx) *BasicStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int},
			{Name: "C", Type: field.Int32},
			{Name: "D", Type: field.Int32},
		},
	}

	var indexes []string

	return &BasicStore{Store: genji.NewStoreWithTx(tx, "Basic", &schema, indexes)}
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

// Delete a record using its primary key.
func (b *BasicStore) Delete(rowid []byte) error {
	return b.Store.Delete(rowid)
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

// Replace the selected record by the given one.
func (b *BasicStore) Replace(rowid []byte, record *Basic) error {
	return b.Store.Replace(rowid, record)
}

// BasicQuerySelector provides helpers for selecting fields from the Basic structure.
type BasicQuerySelector struct {
	A query.StringField
	B query.IntField
	C query.Int32Field
	D query.Int32Field
}

// NewBasicQuerySelector creates a BasicQuerySelector.
func NewBasicQuerySelector() BasicQuerySelector {
	return BasicQuerySelector{
		A: query.NewStringField("A"),
		B: query.NewIntField("B"),
		C: query.NewInt32Field("C"),
		D: query.NewInt32Field("D"),
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
			Type: field.Bytes,
			Data: field.EncodeBytes(b.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Uint16,
			Data: field.EncodeUint16(b.B),
		}, nil
	case "C":
		return field.Field{
			Name: "C",
			Type: field.Float32,
			Data: field.EncodeFloat32(b.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.Float32,
			Data: field.EncodeFloat32(b.D),
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
	return rec.Iterate(func(f field.Field) error {
		var err error

		switch f.Name {
		case "A":
			b.A, err = field.DecodeBytes(f.Data)
		case "B":
			b.B, err = field.DecodeUint16(f.Data)
		case "C":
			b.C, err = field.DecodeFloat32(f.Data)
		case "D":
			b.D, err = field.DecodeFloat32(f.Data)
		}
		return err
	})
}

// basicStore manages the table. It provides several typed helpers
// that simplify common operations.
type basicStore struct {
	*genji.Store
}

// newBasicStore creates a basicStore.
func newBasicStore(db *genji.DB) *basicStore {
	var schema *record.Schema
	schema = &record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.Bytes},
			{Name: "B", Type: field.Uint16},
			{Name: "C", Type: field.Float32},
			{Name: "D", Type: field.Float32},
		},
	}

	var indexes []string

	return &basicStore{Store: genji.NewStore(db, "basic", schema, indexes)}
}

// newBasicStoreWithTx creates a basicStore valid for the lifetime of the given transaction.
func newBasicStoreWithTx(tx *genji.Tx) *basicStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.Bytes},
			{Name: "B", Type: field.Uint16},
			{Name: "C", Type: field.Float32},
			{Name: "D", Type: field.Float32},
		},
	}

	var indexes []string

	return &basicStore{Store: genji.NewStoreWithTx(tx, "basic", &schema, indexes)}
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

// Delete a record using its primary key.
func (b *basicStore) Delete(rowid []byte) error {
	return b.Store.Delete(rowid)
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

// Replace the selected record by the given one.
func (b *basicStore) Replace(rowid []byte, record *basic) error {
	return b.Store.Replace(rowid, record)
}

// basicQuerySelector provides helpers for selecting fields from the basic structure.
type basicQuerySelector struct {
	A query.BytesField
	B query.Uint16Field
	C query.Float32Field
	D query.Float32Field
}

// newbasicQuerySelector creates a basicQuerySelector.
func newBasicQuerySelector() basicQuerySelector {
	return basicQuerySelector{
		A: query.NewBytesField("A"),
		B: query.NewUint16Field("B"),
		C: query.NewFloat32Field("C"),
		D: query.NewFloat32Field("D"),
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
			Data: field.EncodeString(p.A),
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
	return rec.Iterate(func(f field.Field) error {
		var err error

		switch f.Name {
		case "A":
			p.A, err = field.DecodeString(f.Data)
		case "B":
			p.B, err = field.DecodeInt64(f.Data)
		}
		return err
	})
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
	var schema *record.Schema
	schema = &record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	var indexes []string

	return &PkStore{Store: genji.NewStore(db, "Pk", schema, indexes)}
}

// NewPkStoreWithTx creates a PkStore valid for the lifetime of the given transaction.
func NewPkStoreWithTx(tx *genji.Tx) *PkStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	var indexes []string

	return &PkStore{Store: genji.NewStoreWithTx(tx, "Pk", &schema, indexes)}
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

// Replace the selected record by the given one.
func (p *PkStore) Replace(pk int64, record *Pk) error {
	rowid := field.EncodeInt64(pk)
	if record.B != pk {
		record.B = pk
	}
	return p.Store.Replace(rowid, record)
}

// PkQuerySelector provides helpers for selecting fields from the Pk structure.
type PkQuerySelector struct {
	A query.StringField
	B query.Int64Field
}

// NewPkQuerySelector creates a PkQuerySelector.
func NewPkQuerySelector() PkQuerySelector {
	return PkQuerySelector{
		A: query.NewStringField("A"),
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

// Field implements the field method of the record.Record interface.
func (i *Indexed) Field(name string) (field.Field, error) {
	switch name {
	case "A":
		return field.Field{
			Name: "A",
			Type: field.String,
			Data: field.EncodeString(i.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(i.B),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (i *Indexed) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, _ = i.Field("A")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.Field("B")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (i *Indexed) ScanRecord(rec record.Record) error {
	return rec.Iterate(func(f field.Field) error {
		var err error

		switch f.Name {
		case "A":
			i.A, err = field.DecodeString(f.Data)
		case "B":
			i.B, err = field.DecodeInt64(f.Data)
		}
		return err
	})
}

// IndexedStore manages the table. It provides several typed helpers
// that simplify common operations.
type IndexedStore struct {
	*genji.Store
}

// NewIndexedStore creates a IndexedStore.
func NewIndexedStore(db *genji.DB) *IndexedStore {
	var schema *record.Schema
	schema = &record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	var indexes []string
	indexes = append(indexes, "A")

	return &IndexedStore{Store: genji.NewStore(db, "Indexed", schema, indexes)}
}

// NewIndexedStoreWithTx creates a IndexedStore valid for the lifetime of the given transaction.
func NewIndexedStoreWithTx(tx *genji.Tx) *IndexedStore {
	schema := record.Schema{
		Fields: []field.Field{
			{Name: "A", Type: field.String},
			{Name: "B", Type: field.Int64},
		},
	}

	var indexes []string

	indexes = append(indexes, "A")

	return &IndexedStore{Store: genji.NewStoreWithTx(tx, "Indexed", &schema, indexes)}
}

// Insert a record in the table and return the primary key.
func (i *IndexedStore) Insert(record *Indexed) (rowid []byte, err error) {
	return i.Store.Insert(record)
}

// Get a record using its primary key.
func (i *IndexedStore) Get(rowid []byte) (*Indexed, error) {
	var record Indexed

	return &record, i.Store.Get(rowid, &record)
}

// Delete a record using its primary key.
func (i *IndexedStore) Delete(rowid []byte) error {
	return i.Store.Delete(rowid)
}

// List records from the specified offset. If the limit is equal to -1, it returns all records after the selected offset.
func (i *IndexedStore) List(offset, limit int) ([]Indexed, error) {
	size := limit
	if size == -1 {
		size = 0
	}
	list := make([]Indexed, 0, size)
	err := i.Store.List(offset, limit, func(rowid []byte, r record.Record) error {
		var record Indexed
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

// Replace the selected record by the given one.
func (i *IndexedStore) Replace(rowid []byte, record *Indexed) error {
	return i.Store.Replace(rowid, record)
}

// IndexedQuerySelector provides helpers for selecting fields from the Indexed structure.
type IndexedQuerySelector struct {
	A query.StringField
	B query.Int64Field
}

// NewIndexedQuerySelector creates a IndexedQuerySelector.
func NewIndexedQuerySelector() IndexedQuerySelector {
	return IndexedQuerySelector{
		A: query.NewStringField("A"),
		B: query.NewInt64Field("B"),
	}
}

// Table returns a query.TableSelector for Indexed.
func (*IndexedQuerySelector) Table() query.TableSelector {
	return query.Table("Indexed")
}

// All returns a list of all selectors for Indexed.
func (s *IndexedQuerySelector) All() []query.FieldSelector {
	return []query.FieldSelector{
		s.A,
		s.B,
	}
}

// IndexedResult can be used to store the result of queries.
// Selected fields must map the Indexed fields.
type IndexedResult []Indexed

// ScanTable iterates over table.Reader and stores all the records in the slice.
func (i *IndexedResult) ScanTable(tr table.Reader) error {
	return tr.Iterate(func(_ []byte, r record.Record) error {
		var record Indexed
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}

		*i = append(*i, record)
		return nil
	})
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (s *Sample) ScanRecord(rec record.Record) error {
	return rec.Iterate(func(f field.Field) error {
		var err error

		switch f.Name {
		case "A":
			s.A, err = field.DecodeString(f.Data)
		case "B":
			s.B, err = field.DecodeInt64(f.Data)
		}
		return err
	})
}

// SampleResult can be used to store the result of queries.
// Selected fields must map the Sample fields.
type SampleResult []Sample

// ScanTable iterates over table.Reader and stores all the records in the slice.
func (s *SampleResult) ScanTable(tr table.Reader) error {
	return tr.Iterate(func(_ []byte, r record.Record) error {
		var record Sample
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}

		*s = append(*s, record)
		return nil
	})
}

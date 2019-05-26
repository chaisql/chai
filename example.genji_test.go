package genji_test

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// Field implements the field method of the record.Record interface.
func (u *User) Field(name string) (field.Field, error) {
	switch name {
	case "ID":
		return field.Field{
			Name: "ID",
			Type: field.Int64,
			Data: field.EncodeInt64(u.ID),
		}, nil
	case "Name":
		return field.Field{
			Name: "Name",
			Type: field.String,
			Data: field.EncodeString(u.Name),
		}, nil
	case "Age":
		return field.Field{
			Name: "Age",
			Type: field.Uint32,
			Data: field.EncodeUint32(u.Age),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (u *User) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, _ = u.Field("ID")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = u.Field("Name")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = u.Field("Age")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (u *User) ScanRecord(rec record.Record) error {
	return rec.Iterate(func(f field.Field) error {
		var err error

		switch f.Name {
		case "ID":
			u.ID, err = field.DecodeInt64(f.Data)
		case "Name":
			u.Name, err = field.DecodeString(f.Data)
		case "Age":
			u.Age, err = field.DecodeUint32(f.Data)
		}
		return err
	})
}

// Pk returns the primary key. It implements the table.Pker interface.
func (u *User) Pk() ([]byte, error) {
	return field.EncodeInt64(u.ID), nil
}

// UserStore manages the table. It provides several typed helpers
// that simplify common operations.
type UserStore struct {
	*genji.Store
}

// NewUserStore creates a UserStore.
func NewUserStore(db *genji.DB) *UserStore {
	var schema *record.Schema

	var indexes []string
	indexes = append(indexes, "Name")

	return &UserStore{Store: genji.NewStore(db, "User", schema, indexes)}
}

// NewUserStoreWithTx creates a UserStore valid for the lifetime of the given transaction.
func NewUserStoreWithTx(tx *genji.Tx) *UserStore {
	var schema *record.Schema

	var indexes []string

	indexes = append(indexes, "Name")

	return &UserStore{Store: genji.NewStoreWithTx(tx, "User", schema, indexes)}
}

// Insert a record in the table and return the primary key.
func (u *UserStore) Insert(record *User) (err error) {
	_, err = u.Store.Insert(record)
	return err
}

// Get a record using its primary key.
func (u *UserStore) Get(pk int64) (*User, error) {
	var record User
	rowid := field.EncodeInt64(pk)

	return &record, u.Store.Get(rowid, &record)
}

// Delete a record using its primary key.
func (u *UserStore) Delete(pk int64) error {
	rowid := field.EncodeInt64(pk)
	return u.Store.Delete(rowid)
}

// List records from the specified offset. If the limit is equal to -1, it returns all records after the selected offset.
func (u *UserStore) List(offset, limit int) ([]User, error) {
	size := limit
	if size == -1 {
		size = 0
	}
	list := make([]User, 0, size)
	err := u.Store.List(offset, limit, func(rowid []byte, r record.Record) error {
		var record User
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
func (u *UserStore) Replace(pk int64, record *User) error {
	rowid := field.EncodeInt64(pk)
	if record.ID != pk {
		record.ID = pk
	}
	return u.Store.Replace(rowid, record)
}

// UserQuerySelector provides helpers for selecting fields from the User structure.
type UserQuerySelector struct {
	ID   query.Int64Field
	Name query.StringField
	Age  query.Uint32Field
}

// NewUserQuerySelector creates a UserQuerySelector.
func NewUserQuerySelector() UserQuerySelector {
	return UserQuerySelector{
		ID:   query.NewInt64Field("ID"),
		Name: query.NewStringField("Name"),
		Age:  query.NewUint32Field("Age"),
	}
}

// Table returns a query.TableSelector for User.
func (*UserQuerySelector) Table() query.TableSelector {
	return query.Table("User")
}

// All returns a list of all selectors for User.
func (s *UserQuerySelector) All() []query.FieldSelector {
	return []query.FieldSelector{
		s.ID,
		s.Name,
		s.Age,
	}
}

// UserResult can be used to store the result of queries.
// Selected fields must map the User fields.
type UserResult []User

// ScanTable iterates over table.Reader and stores all the records in the slice.
func (u *UserResult) ScanTable(tr table.Reader) error {
	return tr.Iterate(func(_ []byte, r record.Record) error {
		var record User
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}

		*u = append(*u, record)
		return nil
	})
}

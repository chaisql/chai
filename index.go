package genji

import (
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/pkg/errors"
)

type indexOptions struct {
	TableName string
	FieldName string
	Unique    bool
}

func (i *indexOptions) PrimaryKey() ([]byte, error) {
	return []byte(buildIndexName(i.TableName, i.FieldName)), nil
}

// Field implements the field method of the record.Record interface.
func (i *indexOptions) GetField(name string) (field.Field, error) {
	switch name {
	case "Unique":
		return field.NewBool("Unique", i.Unique), nil
	case "TableName":
		return field.NewString("TableName", i.TableName), nil
	case "FieldName":
		return field.NewString("FieldName", i.FieldName), nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (i *indexOptions) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, _ = i.GetField("Unique")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("TableName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetField("FieldName")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func (i *indexOptions) ScanRecord(rec record.Record) error {
	return rec.Iterate(func(f field.Field) error {
		var err error

		switch f.Name {
		case "Unique":
			i.Unique, err = field.DecodeBool(f.Data)
		case "TableName":
			i.TableName, err = field.DecodeString(f.Data)
		case "FieldName":
			i.FieldName, err = field.DecodeString(f.Data)
		}
		return err
	})
}

func readIndexOptions(tx *Tx, indexName string) (*indexOptions, error) {
	it, err := tx.GetTable(indexTable)
	if err != nil {
		return nil, err
	}

	r, err := it.GetRecord([]byte(indexName))
	if err != nil {
		if err == table.ErrRecordNotFound {
			return nil, ErrIndexNotFound
		}

		return nil, err
	}
	var idxopts indexOptions
	err = idxopts.ScanRecord(r)
	if err != nil {
		return nil, err
	}

	return &idxopts, nil
}

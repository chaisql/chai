package generator

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// Field implements the field method of the record.Record interface.
func (r *RecordTest) Field(name string) (*field.Field, error) {
	switch name {
	case "A":
		return &field.Field{
			Name: "A",
			Type: field.String,
			Data: []byte(r.A),
		}, nil
	case "B":
		return &field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(r.B),
		}, nil
	case "C":
		return &field.Field{
			Name: "C",
			Type: field.Int64,
			Data: field.EncodeInt64(r.C),
		}, nil
	case "D":
		return &field.Field{
			Name: "D",
			Type: field.Int64,
			Data: field.EncodeInt64(r.D),
		}, nil
	}

	return nil, errors.New("unknown field")
}

func (r *RecordTest) Cursor() record.Cursor {
	return &recordTestCursor{
		RecordTest: r,
		i:          -1,
	}
}

type recordTestCursor struct {
	RecordTest *RecordTest
	i          int
}

func (c *recordTestCursor) Next() bool {
	if c.i+2 > 4 {
		return false
	}

	c.i++
	return true
}

func (c *recordTestCursor) Field() (*field.Field, error) {
	switch c.i {
	case 0:
		return c.RecordTest.Field("A")
	case 1:
		return c.RecordTest.Field("B")
	case 2:
		return c.RecordTest.Field("C")
	case 3:
		return c.RecordTest.Field("D")
	}

	return nil, errors.New("cursor has no more fields")
}

func (c *recordTestCursor) Err() error {
	return nil
}

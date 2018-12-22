package generator

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// Field implements the field method of the record.Record interface.
func (r *RecordTest) Field(name string) (field.Field, error) {
	switch name {
	case "A":
		return field.Field{
			Name: "A",
			Type: field.String,
			Data: []byte(r.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(r.B),
		}, nil
	case "C":
		return field.Field{
			Name: "C",
			Type: field.Int64,
			Data: field.EncodeInt64(r.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.Int64,
			Data: field.EncodeInt64(r.D),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
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
	err        error
}

func (c *recordTestCursor) Next() bool {
	if c.i+2 > 4 {
		return false
	}

	c.i++
	return true
}

func (c *recordTestCursor) Field() field.Field {
	switch c.i {
	case 0:
		f, _ := c.RecordTest.Field("A")
		return f
	case 1:
		f, _ := c.RecordTest.Field("B")
		return f
	case 2:
		f, _ := c.RecordTest.Field("C")
		return f
	case 3:
		f, _ := c.RecordTest.Field("D")
		return f
	}

	c.err = errors.New("no more fields")
	return field.Field{}
}

func (c *recordTestCursor) Err() error {
	return c.err
}

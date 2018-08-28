package generator

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// Field implements the field method of the record.Record interface.
func (s *StructTest) Field(name string) (*field.Field, error) {
	switch name {
	case "A":
		return &field.Field{
			Name: "A",
			Type: field.String,
			Data: []byte(s.A),
		}, nil
	case "B":
		return &field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(s.B),
		}, nil
	case "C":
		return &field.Field{
			Name: "C",
			Type: field.Int64,
			Data: field.EncodeInt64(s.C),
		}, nil
	case "D":
		return &field.Field{
			Name: "D",
			Type: field.Int64,
			Data: field.EncodeInt64(s.D),
		}, nil
	}

	return nil, errors.New("unknown field")
}

func (s *StructTest) Cursor() record.Cursor {
	return &structTestCursor{
		StructTest: s,
		i:          -1,
	}
}

type structTestCursor struct {
	StructTest *StructTest
	i          int
}

func (c *structTestCursor) Next() bool {
	if c.i+2 > 4 {
		return false
	}

	c.i++
	return true
}

func (c *structTestCursor) Field() (*field.Field, error) {
	switch c.i {
	case 0:
		return c.StructTest.Field("A")
	case 1:
		return c.StructTest.Field("B")
	case 2:
		return c.StructTest.Field("C")
	case 3:
		return c.StructTest.Field("D")
	}

	return nil, errors.New("cursor has no more fields")
}

func (c *structTestCursor) Err() error {
	return nil
}

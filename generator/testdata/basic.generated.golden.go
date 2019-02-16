package testdata

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
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
			Type: field.String,
			Data: []byte(b.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.String,
			Data: []byte(b.D),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Cursor creates a cursor for scanning records.
func (b *Basic) Cursor() record.Cursor {
	return &basicCursor{
		Basic: b,
		i:     -1,
	}
}

type basicCursor struct {
	Basic *Basic
	i     int
	err   error
}

func (c *basicCursor) Next() bool {
	if c.i+2 > 4 {
		return false
	}

	c.i++
	return true
}

func (c *basicCursor) Field() field.Field {
	switch c.i {
	case 0:
		f, _ := c.Basic.Field("A")
		return f
	case 1:
		f, _ := c.Basic.Field("B")
		return f
	case 2:
		f, _ := c.Basic.Field("C")
		return f
	case 3:
		f, _ := c.Basic.Field("D")
		return f
	}

	c.err = errors.New("no more fields")
	return field.Field{}
}

func (c *basicCursor) Err() error {
	return c.err
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

// C returns a string selector.
func (BasicSelector) C() query.StrField {
	return query.NewStrField("C")
}

// D returns a string selector.
func (BasicSelector) D() query.StrField {
	return query.NewStrField("D")
}

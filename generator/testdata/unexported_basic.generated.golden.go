package testdata

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
)

// Field implements the field method of the record.Record interface.
func (u *unexportedBasic) Field(name string) (field.Field, error) {
	switch name {
	case "A":
		return field.Field{
			Name: "A",
			Type: field.String,
			Data: []byte(u.A),
		}, nil
	case "B":
		return field.Field{
			Name: "B",
			Type: field.Int64,
			Data: field.EncodeInt64(u.B),
		}, nil
	case "C":
		return field.Field{
			Name: "C",
			Type: field.Int64,
			Data: field.EncodeInt64(u.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.Int64,
			Data: field.EncodeInt64(u.D),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Cursor creates a cursor for scanning records.
func (u *unexportedBasic) Cursor() record.Cursor {
	return &unexportedBasicCursor{
		unexportedBasic: u,
		i:               -1,
	}
}

type unexportedBasicCursor struct {
	unexportedBasic *unexportedBasic
	i               int
	err             error
}

func (c *unexportedBasicCursor) Next() bool {
	if c.i+2 > 4 {
		return false
	}

	c.i++
	return true
}

func (c *unexportedBasicCursor) Field() field.Field {
	switch c.i {
	case 0:
		f, _ := c.unexportedBasic.Field("A")
		return f
	case 1:
		f, _ := c.unexportedBasic.Field("B")
		return f
	case 2:
		f, _ := c.unexportedBasic.Field("C")
		return f
	case 3:
		f, _ := c.unexportedBasic.Field("D")
		return f
	}

	c.err = errors.New("no more fields")
	return field.Field{}
}

func (c *unexportedBasicCursor) Err() error {
	return c.err
}

// unexportedBasicSelector provides helpers for selecting fields from the unexportedBasic structure.
type unexportedBasicSelector struct{}

// newunexportedBasicSelector creates a unexportedBasicSelector.
func newUnexportedBasicSelector() unexportedBasicSelector {
	return unexportedBasicSelector{}
}

// A returns a string selector.
func (unexportedBasicSelector) A() query.StrField {
	return query.NewStrField("A")
}

// B returns an int64 selector.
func (unexportedBasicSelector) B() query.Int64Field {
	return query.NewInt64Field("B")
}

// C returns an int64 selector.
func (unexportedBasicSelector) C() query.Int64Field {
	return query.NewInt64Field("C")
}

// D returns an int64 selector.
func (unexportedBasicSelector) D() query.Int64Field {
	return query.NewInt64Field("D")
}

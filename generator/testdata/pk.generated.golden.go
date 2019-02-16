package testdata

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
)

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

// Cursor creates a cursor for scanning records.
func (p *Pk) Cursor() record.Cursor {
	return &pkCursor{
		Pk: p,
		i:  -1,
	}
}

type pkCursor struct {
	Pk  *Pk
	i   int
	err error
}

func (c *pkCursor) Next() bool {
	if c.i+2 > 2 {
		return false
	}

	c.i++
	return true
}

func (c *pkCursor) Field() field.Field {
	switch c.i {
	case 0:
		f, _ := c.Pk.Field("A")
		return f
	case 1:
		f, _ := c.Pk.Field("B")
		return f
	}

	c.err = errors.New("no more fields")
	return field.Field{}
}

func (c *pkCursor) Err() error {
	return c.err
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

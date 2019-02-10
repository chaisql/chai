package testdata

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// Field implements the field method of the record.Record interface.
func (u *User) Field(name string) (field.Field, error) {
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
			Type: field.String,
			Data: []byte(u.C),
		}, nil
	case "D":
		return field.Field{
			Name: "D",
			Type: field.String,
			Data: []byte(u.D),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Cursor creates a cursor for scanning records.
func (u *User) Cursor() record.Cursor {
	return &userCursor{
		User: u,
		i:    -1,
	}
}

type userCursor struct {
	User *User
	i    int
	err  error
}

func (c *userCursor) Next() bool {
	if c.i+2 > 4 {
		return false
	}

	c.i++
	return true
}

func (c *userCursor) Field() field.Field {
	switch c.i {
	case 0:
		f, _ := c.User.Field("A")
		return f
	case 1:
		f, _ := c.User.Field("B")
		return f
	case 2:
		f, _ := c.User.Field("C")
		return f
	case 3:
		f, _ := c.User.Field("D")
		return f
	}

	c.err = errors.New("no more fields")
	return field.Field{}
}

func (c *userCursor) Err() error {
	return c.err
}

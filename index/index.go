package index

import "github.com/asdine/genji/field"

type Index interface {
	Cursor() Cursor
	Set(f field.Field) error
}

type Cursor interface {
	Next() (value field.Field, rowid []byte)
	Prev() (value field.Field, rowid []byte)
	Seek(seek field.Field) (value field.Field, rowid []byte)
}

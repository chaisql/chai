package index

type Index interface {
	Cursor() Cursor
	Set(d []byte) error
}

type Cursor interface {
	First() (value []byte, rowid []byte, err error)
	Last() (value []byte, rowid []byte, err error)
	Next() (value []byte, rowid []byte, err error)
	Prev() (value []byte, rowid []byte, err error)
	Seek(seek []byte) (value []byte, rowid []byte, err error)
}

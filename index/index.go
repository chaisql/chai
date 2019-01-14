package index

type Index interface {
	Cursor() Cursor
	Set(value []byte, rowid []byte) error
}

type Cursor interface {
	First() (value []byte, rowid []byte)
	Last() (value []byte, rowid []byte)
	Next() (value []byte, rowid []byte)
	Prev() (value []byte, rowid []byte)
	Seek(seek []byte) (value []byte, rowid []byte)
}

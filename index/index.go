package index

type Index interface {
	Cursor() Cursor
}

type Cursor interface {
	First() (rowid []byte, value []byte)
	Last() (rowid []byte, value []byte)
	Next() (rowid []byte, value []byte)
	Prev() (rowid []byte, value []byte)
	Seek(seek []byte) (rowid []byte, value []byte)
}

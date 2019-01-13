package bolt

import (
	"github.com/asdine/genji/field"
	bolt "github.com/etcd-io/bbolt"
)

type Index struct {
	b *bolt.Bucket
}

func (i *Index) Set(f field.Field, rowid []byte) error {
	b, err := i.b.CreateBucketIfNotExists(f.Data)
	if err != nil {
		return err
	}

	return b.Put(rowid, nil)
}

// type Cursor struct {
// 	c *bolt.Cursor
// }

// func (c *Cursor) Next() (value field.Field, rowid []byte) {

// 	return nil, nil
// }

// func (c *Cursor) Prev() (value field.Field, rowid []byte) {
// 	return nil, nil
// }

// func (c *Cursor) Seek(seek field.Field) (value field.Field, rowid []byte) {
// 	c.c.Seek()
// 	return nil, nil
// }

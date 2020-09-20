package index

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/key"
)

const (
	// storePrefix is the prefix used to name the index stores.
	storePrefix = "i"
)

var valueTypes = []document.ValueType{
	document.NullValue,
	document.BoolValue,
	document.DoubleValue,
	document.DurationValue,
	document.TextValue,
	document.BlobValue,
	document.ArrayValue,
	document.DocumentValue,
}

var (
	// ErrDuplicate is returned when a value is already associated with a key
	ErrDuplicate = errors.New("duplicate")
)

// An Index associates encoded values with keys.
// It is sorted by value following the lexicographic order.
type Index struct {
	Unique bool

	tx        engine.Transaction
	storeName []byte
}

// Options of the index.
type Options struct {
	Unique bool
}

// NewIndex creates an index that associates a value with a list of keys.
func NewIndex(tx engine.Transaction, idxName string, opts Options) *Index {
	return &Index{
		tx:        tx,
		storeName: append([]byte(storePrefix), idxName...),
		Unique:    opts.Unique,
	}
}

var errStop = errors.New("stop")

// Set associates a value with a key. If Unique is set to false, it is
// possible to associate multiple keys for the same value
// but a key can be associated to only one value.
func (idx *Index) Set(v document.Value, k []byte) error {
	var err error

	if len(k) == 0 {
		return errors.New("cannot index value without a key")
	}

	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	// encode the value we are going to use as a key
	buf, err := key.AppendValue(nil, v)
	if err != nil {
		return err
	}

	// lookup for an already existing value in the index.
	// every value ends with a zero byte.
	lookupKey := append(buf, 0)

	_, err = st.Get(lookupKey)
	switch err {
	case nil:
		// the value already exists
		// if this is a unique index, return an error
		if idx.Unique {
			return ErrDuplicate
		}

		// the value already exists
		// add a prefix to that value
		seq, err := st.NextSequence()
		if err != nil {
			return err
		}
		vbuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(vbuf, seq)
		buf = append(buf, vbuf[:n]...)
		// duplicated values always end with the size of the varint
		buf = append(buf, byte(n))
	case engine.ErrKeyNotFound:
		// the value doesn't exist
		// use the lookup as value
		buf = lookupKey
	default:
		return err
	}

	return st.Put(buf, k)
}

// Delete all the references to the key from the index.
func (idx *Index) Delete(v document.Value, k []byte) error {
	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	var toDelete []byte
	var buf []byte
	err = iterate(st, v, false, func(item engine.Item) error {
		buf, err = item.ValueCopy(buf[:0])
		if err != nil {
			return err
		}
		if bytes.Equal(buf, k) {
			toDelete = item.Key()
			return errStop
		}

		return nil
	})
	if err != errStop && err != nil {
		return err
	}

	if toDelete != nil {
		return st.Delete(toDelete)
	}

	return engine.ErrKeyNotFound
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is empty, starts from the beginning.
func (idx *Index) AscendGreaterOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(pivot, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is empty, starts from the end.
func (idx *Index) DescendLessOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(pivot, true, fn)
}

func (idx *Index) iterateOnStore(pivot document.Value, reverse bool, fn func(val, key []byte, isEqual bool) error) error {
	st, err := idx.tx.GetStore(idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}
	if st == nil {
		return nil
	}

	var enc []byte
	if pivot.V != nil {
		enc, err = key.AppendValue(nil, pivot)
		if err != nil {
			return err
		}
	}

	var buf []byte
	return iterate(st, pivot, reverse, func(item engine.Item) error {
		var err error

		k := item.Key()

		// the last byte of the key is the size of the varint.
		// if that byte is 0, it means that key is not duplicated.
		n := k[len(k)-1]
		k = k[:len(k)-int(n)-1]

		buf, err = item.ValueCopy(buf[:0])
		if err != nil {
			return err
		}

		return fn(k, buf, bytes.Equal(k, enc))
	})
}

// Truncate deletes all the index data.
func (idx *Index) Truncate() error {
	err := idx.tx.DropStore(idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}

	return nil
}

func getOrCreateStore(tx engine.Transaction, name []byte) (engine.Store, error) {
	st, err := tx.GetStore(name)
	if err == nil {
		return st, nil
	}

	if err != engine.ErrStoreNotFound {
		return nil, err
	}

	err = tx.CreateStore(name)
	if err != nil {
		return nil, err
	}

	return tx.GetStore(name)
}

func iterate(st engine.Store, pivot document.Value, reverse bool, fn func(item engine.Item) error) error {
	var seek []byte
	var err error

	if pivot.V != nil {
		seek, err = key.AppendValue(nil, pivot)
		if err != nil {
			return err
		}

		if reverse {
			seek = append(seek, 0xFF)
		}
	}

	if pivot.Type == document.IntegerValue {
		pivot.Type = document.DoubleValue
	}

	if pivot.Type != 0 && pivot.V == nil {
		seek = []byte{byte(pivot.Type)}

		if reverse {
			seek = append(seek, 0xFF)
		}
	}

	it := st.NewIterator(engine.IteratorConfig{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		itm := it.Item()

		if pivot.Type != 0 && itm.Key()[0] != byte(pivot.Type) {
			return nil
		}

		err := fn(itm)
		if err != nil {
			return err
		}
	}

	return nil
}

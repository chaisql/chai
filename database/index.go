package database

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
)

const (
	// indexStorePrefix is the prefix used to name the index stores.
	indexStorePrefix = "i"
)

var (
	// ErrIndexDuplicateValue is returned when a value is already associated with a key
	ErrIndexDuplicateValue = errors.New("duplicate")
)

// An Index associates encoded values with keys.
// It is sorted by value following the lexicographic order.
type Index struct {
	Info *IndexInfo

	tx        engine.Transaction
	storeName []byte
}

// NewIndex creates an index that associates a value with a list of keys.
func NewIndex(tx engine.Transaction, idxName string, opts *IndexInfo) *Index {
	return &Index{
		tx:        tx,
		storeName: append([]byte(indexStorePrefix), idxName...),
		Info:      opts,
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

	if idx.Info.Type != 0 && idx.Info.Type != v.Type {
		return fmt.Errorf("cannot index value of type %s in %s index", v.Type, idx.Info.Type)
	}

	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	// encode the value we are going to use as a key
	buf, err := idx.EncodeValue(v)
	if err != nil {
		return err
	}

	// lookup for an already existing value in the index.
	var lookupKey = buf

	// every value of a non-unique index ends with a byte that starts at zero.
	if !idx.Info.Unique {
		lookupKey = append(lookupKey, 0)
	}

	_, err = st.Get(lookupKey)
	switch err {
	case nil:
		// the value already exists
		// if this is a unique index, return an error
		if idx.Info.Unique {
			return ErrIndexDuplicateValue
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
	err = idx.iterate(st, v, false, func(item engine.Item) error {
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
func (idx *Index) AscendGreaterOrEqual(pivot document.Value, fn func(val, key []byte) error) error {
	return idx.iterateOnStore(pivot, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is empty, starts from the end.
func (idx *Index) DescendLessOrEqual(pivot document.Value, fn func(val, key []byte) error) error {
	return idx.iterateOnStore(pivot, true, fn)
}

func (idx *Index) iterateOnStore(pivot document.Value, reverse bool, fn func(val, key []byte) error) error {
	// if index and pivot are typed but not of the same type
	// return no result
	if idx.Info.Type != 0 && pivot.Type != 0 && idx.Info.Type != pivot.Type {
		return nil
	}

	st, err := idx.tx.GetStore(idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}
	if st == nil {
		return nil
	}

	var buf []byte
	return idx.iterate(st, pivot, reverse, func(item engine.Item) error {
		var err error

		k := item.Key()

		// the last byte of the key of a non-unique index is the size of the varint.
		// if that byte is 0, it means that key is not duplicated.
		if !idx.Info.Unique {
			n := k[len(k)-1]
			k = k[:len(k)-int(n)-1]
		}

		buf, err = item.ValueCopy(buf[:0])
		if err != nil {
			return err
		}

		return fn(k, buf)
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

// EncodeValue encodes the value we are going to use as a key,
// If the index is typed, encode the value without expecting
// the presence of other types.
// Ff not, encode so that order is preserved regardless of the type.
func (idx *Index) EncodeValue(v document.Value) ([]byte, error) {
	if idx.Info.Type != 0 {
		return v.MarshalBinary()
	}

	var err error
	if v.Type == document.IntegerValue {
		if v.V == nil {
			v.Type = document.DoubleValue
		} else {
			v, err = v.CastAsDouble()
			if err != nil {
				return nil, err
			}
		}
	}

	var buf bytes.Buffer
	err = document.NewValueEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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

func (idx *Index) iterate(st engine.Store, pivot document.Value, reverse bool, fn func(item engine.Item) error) error {
	var seek []byte
	var err error

	if idx.Info.Type == 0 && pivot.Type == document.IntegerValue {
		if pivot.V == nil {
			pivot.Type = document.DoubleValue
		} else {
			pivot, err = pivot.CastAsDouble()
			if err != nil {
				return err
			}
		}
	}

	if pivot.V != nil {
		seek, err = idx.EncodeValue(pivot)
		if err != nil {
			return err
		}

		if reverse {
			seek = append(seek, 0xFF)
		}
	}

	if idx.Info.Type == 0 && pivot.Type != 0 && pivot.V == nil {
		seek = []byte{byte(pivot.Type)}

		if reverse {
			seek = append(seek, 0xFF)
		}
	}

	it := st.Iterator(engine.IteratorOptions{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		itm := it.Item()

		// if index is untyped and pivot is typed, only iterate on values with the same type as pivot
		if idx.Info.Type == 0 && pivot.Type != 0 && itm.Key()[0] != byte(pivot.Type) {
			return nil
		}

		err := fn(itm)
		if err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}

	return nil
}

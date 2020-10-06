package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

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
	Type   document.ValueType

	tx        engine.Transaction
	storeName []byte
}

// Options of the index.
type Options struct {
	Unique bool

	// If specified, the indexed expects only one type.
	Type document.ValueType
}

// NewIndex creates an index that associates a value with a list of keys.
func NewIndex(tx engine.Transaction, idxName string, opts Options) *Index {
	return &Index{
		tx:        tx,
		storeName: append([]byte(storePrefix), idxName...),
		Unique:    opts.Unique,
		Type:      opts.Type,
	}
}

var errStop = errors.New("stop")

// Set associates a value with a key. If Unique is set to false, it is
// possible to associate multiple keys for the same value
// but a key can be associated to only one value.
func (idx *Index) Set(ctx context.Context, v document.Value, k []byte) error {
	var err error

	if len(k) == 0 {
		return errors.New("cannot index value without a key")
	}

	if idx.Type != 0 && idx.Type != v.Type {
		return fmt.Errorf("cannot index value of type %s in %s index", v.Type, idx.Type)
	}

	st, err := getOrCreateStore(ctx, idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	// encode the value we are going to use as a key
	buf, err := idx.encodeValue(v)
	if err != nil {
		return err
	}

	// lookup for an already existing value in the index.
	var lookupKey = buf

	// every value of a non-unique index ends with a byte that starts at zero.
	if !idx.Unique {
		lookupKey = append(lookupKey, 0)
	}

	_, err = st.Get(ctx, lookupKey)
	switch err {
	case nil:
		// the value already exists
		// if this is a unique index, return an error
		if idx.Unique {
			return ErrDuplicate
		}

		// the value already exists
		// add a prefix to that value
		seq, err := st.NextSequence(ctx)
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

	return st.Put(ctx, buf, k)
}

// Delete all the references to the key from the index.
func (idx *Index) Delete(ctx context.Context, v document.Value, k []byte) error {
	st, err := getOrCreateStore(ctx, idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	var toDelete []byte
	var buf []byte
	err = idx.iterate(ctx, st, v, false, func(item engine.Item) error {
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
		return st.Delete(ctx, toDelete)
	}

	return engine.ErrKeyNotFound
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is empty, starts from the beginning.
func (idx *Index) AscendGreaterOrEqual(ctx context.Context, pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(ctx, pivot, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is empty, starts from the end.
func (idx *Index) DescendLessOrEqual(ctx context.Context, pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(ctx, pivot, true, fn)
}

func (idx *Index) iterateOnStore(ctx context.Context, pivot document.Value, reverse bool, fn func(val, key []byte, isEqual bool) error) error {
	if idx.Type != 0 && pivot.Type != 0 && idx.Type != pivot.Type {
		return nil
	}

	st, err := idx.tx.GetStore(ctx, idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}
	if st == nil {
		return nil
	}

	var enc []byte
	if pivot.V != nil {
		enc, err = idx.encodeValue(pivot)
		if err != nil {
			return err
		}
	}

	var buf []byte
	return idx.iterate(ctx, st, pivot, reverse, func(item engine.Item) error {
		var err error

		k := item.Key()

		// the last byte of the key of a non-unique index is the size of the varint.
		// if that byte is 0, it means that key is not duplicated.
		if !idx.Unique {
			n := k[len(k)-1]
			k = k[:len(k)-int(n)-1]
		}

		buf, err = item.ValueCopy(buf[:0])
		if err != nil {
			return err
		}

		return fn(k, buf, bytes.Equal(k, enc))
	})
}

// Truncate deletes all the index data.
func (idx *Index) Truncate(ctx context.Context) error {
	err := idx.tx.DropStore(ctx, idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}

	return nil
}

// encode the value we are going to use as a key
// if the index is typed, encode the value without expecting
// the presence of other types.
// if not, encode so that order is preserved regardless of the type.
func (idx *Index) encodeValue(v document.Value) (buf []byte, err error) {
	if idx.Type != 0 {
		buf, err = key.Append(buf, v.Type, v.V)
	} else {
		buf, err = key.AppendValue(buf, v)
	}
	return
}

func getOrCreateStore(ctx context.Context, tx engine.Transaction, name []byte) (engine.Store, error) {
	st, err := tx.GetStore(ctx, name)
	if err == nil {
		return st, nil
	}

	if err != engine.ErrStoreNotFound {
		return nil, err
	}

	err = tx.CreateStore(ctx, name)
	if err != nil {
		return nil, err
	}

	return tx.GetStore(ctx, name)
}

func (idx *Index) iterate(ctx context.Context, st engine.Store, pivot document.Value, reverse bool, fn func(item engine.Item) error) error {
	var seek []byte
	var err error

	if pivot.V != nil {
		seek, err = idx.encodeValue(pivot)
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

	it := st.Iterator(engine.IteratorOptions{Reverse: reverse})
	defer it.Close()

	for it.Seek(ctx, seek); it.Valid(); it.Next(ctx) {
		itm := it.Item()

		if idx.Type == 0 && pivot.Type != 0 && itm.Key()[0] != byte(pivot.Type) {
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

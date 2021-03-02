package database

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/stringutil"
)

const (
	// indexStorePrefix is the prefix used to name the index stores.
	indexStorePrefix = "i"
)

var (
	// ErrIndexDuplicateValue is returned when a value is already associated with a key
	ErrIndexDuplicateValue = errors.New("duplicate value")
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
	if opts == nil {
		opts = &IndexInfo{
			Types: []document.ValueType{document.ValueType(0)}
		}
	}

	if opts.Types == nil {
			opts.Types= []document.ValueType{document.ValueType(0)}
	}

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
func (idx *Index) Set(vs []document.Value, k []byte) error {
	var err error

	if len(k) == 0 {
		return errors.New("cannot index value without a key")
	}

	if len(vs) == 0 {
		return errors.New("cannot index without a value")
	}

	if len(vs) > len(idx.Types) {
		return errors.New("cannot index more values than what the index supports")
	}

	for i, typ := range idx.Types {
		// it is possible to set an index(a,b) on (a), it will be assumed that b is null in that case
		if typ != 0 && i < len(vs) && typ != vs[i].Type {
			// TODO use the full version to clarify the error
			return fmt.Errorf("cannot index value of type %s in %s index", vs[i].Type, typ)
		}
	}

	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	// encode the value we are going to use as a key
	buf, err := idx.EncodeValues(vs)
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
func (idx *Index) Delete(vs []document.Value, k []byte) error {
	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		// TODO, more precise error handling?
		return nil
	}

	var toDelete []byte
	var buf []byte
	err = idx.iterate(st, vs, false, func(item engine.Item) error {
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
func (idx *Index) AscendGreaterOrEqual(pivots []document.Value, fn func(val, key []byte) error) error {
	return idx.iterateOnStore(pivots, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is empty, starts from the end.
func (idx *Index) DescendLessOrEqual(pivots []document.Value, fn func(val, key []byte) error) error {
	return idx.iterateOnStore(pivots, true, fn)
}

func (idx *Index) iterateOnStore(pivots []document.Value, reverse bool, fn func(val, key []byte) error) error {
	if len(pivots) == 0 {
		return errors.New("cannot iterate without a pivot")
	}

	if len(pivots) > len(idx.Types) {
		return errors.New("cannot iterate with more values than what the index supports")
	}

	for i, typ := range idx.Types {
		// if index and pivot are typed but not of the same type
		// return no result
		if typ != 0 && pivots[i].Type != 0 && typ != pivots[i].Type {
			return nil
		}

	}

	st, err := idx.tx.GetStore(idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}
	if st == nil {
		return nil
	}

	var buf []byte
	return idx.iterate(st, pivots, reverse, func(item engine.Item) error {
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
// If not, encode so that order is preserved regardless of the type.
func (idx *Index) EncodeValue(v document.Value) ([]byte, error) {
	if idx.Types[0] != 0 {
		return v.MarshalBinary()
	}

	var err error
	var buf bytes.Buffer
	err = document.NewValueEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// TODO
func (idx *Index) EncodeValues(vs []document.Value) ([]byte, error) {
	buf := []byte{}

	for i, v := range vs {
		if idx.Types[i] != 0 {
			b, err := v.MarshalBinary()
			if err != nil {
				return nil, err
			}

			buf = append(buf, b...)
			continue
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

		var bbuf bytes.Buffer
		err = document.NewValueEncoder(&bbuf).Encode(v)
		if err != nil {
			return nil, err
		}
		b := bbuf.Bytes()
		buf = append(buf, b...)
	}

	return buf, nil
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

func (idx *Index) iterate(st engine.Store, pivots []document.Value, reverse bool, fn func(item engine.Item) error) error {
	var seek []byte
	var err error

	for i, typ := range idx.Types {
		if typ == 0 && pivots[i].Type == document.IntegerValue {
			if pivots[i].V == nil {
				pivots[i].Type = document.DoubleValue
			} else {
				pivots[i], err = pivots[i].CastAsDouble()
				if err != nil {
					return err
				}
			}
		}
	}

	if pivots[0].V != nil {
		seek, err = idx.EncodeValues(pivots)
		if err != nil {
			return err
		}

		if reverse {
			seek = append(seek, 0xFF)
		}
	} else {
		// this is pretty surely wrong as it does not allow to select t1t2 is there are values on t1
		buf := []byte{}
		for i, typ := range idx.Types {
			if typ == 0 && pivots[i].Type != 0 && pivots[i].V == nil {
				buf = append(buf, byte(pivots[i].Type))
			}
		}

		seek = buf
		if reverse {
			seek = append(seek, 0xFF)
		}
		// if idx.Type == 0 && pivot.Type != 0 && pivot.V == nil {
		// 	seek = []byte{byte(pivot.Type)}

		// 	if reverse {
		// 		seek = append(seek, 0xFF)
		// 	}
		// }
	}

	it := st.Iterator(engine.IteratorOptions{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		itm := it.Item()

		// if index is untyped and pivot is typed, only iterate on values with the same type as pivot
		// if idx.Type == 0 && pivot.Type != 0 && itm.Key()[0] != byte(pivot.Type) {
		// 	return nil
		// }

		// this is wrong and only handle the first type
		for i, typ := range idx.Types {
			if typ == 0 && pivots[i].Type != 0 && itm.Key()[0] != byte(pivots[i].Type) {
				return nil
			}
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

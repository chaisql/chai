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

	// untypedValue is the placeholder type for keys of an index which aren't typed.
	// CREATE TABLE foo;
	// CREATE INDEX idx_foo_a_b ON foo(a,b);
	// document.ValueType of a and b will be untypedValue.
	untypedValue = document.ValueType(0)
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
			Types: []document.ValueType{untypedValue},
		}
	}

	if opts.Types == nil {
		opts.Types = []document.ValueType{untypedValue}
	}

	return &Index{
		tx:        tx,
		storeName: append([]byte(indexStorePrefix), idxName...),
		Info:      opts,
	}
}

var errStop = errors.New("stop")

func (idx *Index) IsComposite() bool {
	return len(idx.Info.Types) > 1
}

func (idx *Index) Arity() int {
	return len(idx.Info.Types)
}

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

	if len(vs) != len(idx.Info.Types) {
		return fmt.Errorf("cannot index %d values on an index of arity %d", len(vs), len(idx.Info.Types))
	}

	for i, typ := range idx.Info.Types {
		// it is possible to set an index(a,b) on (a), it will be assumed that b is null in that case
		if typ != untypedValue && i < len(vs) && typ != vs[i].Type {
			// TODO use the full version to clarify the error
			return fmt.Errorf("cannot index value of type %s in %s index", vs[i].Type, typ)
		}
	}

	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	// encode the value we are going to use as a key
	var buf []byte
	if len(vs) > 1 {
		wrappedVs := document.NewValueBuffer(vs...)
		buf, err = idx.EncodeValue(document.NewArrayValue(wrappedVs))
	} else {
		buf, err = idx.EncodeValue(vs[0])
	}

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

// validatePivots returns an error when the pivots are unsuitable for the index:
// - no pivots at all
// - having pivots length superior to the index arity
// - having the first pivot without a value when the subsequent ones do have values
func (idx *Index) validatePivots(pivots []document.Value) error {
	if len(pivots) == 0 {
		return errors.New("cannot iterate without a pivot")
	}

	if len(pivots) > idx.Arity() {
		// TODO panic
		return errors.New("cannot iterate with a pivot whose size is superior to the index arity")
	}

	if idx.IsComposite() {
		if pivots[0].V == nil {
			return errors.New("cannot iterate on a composite index with a pivot whose first item has no value")
		}

		previousPivotHasValue := true
		for _, p := range pivots[1:] {
			if previousPivotHasValue {
				previousPivotHasValue = p.V != nil
			} else {
				return errors.New("cannot iterate on a composite index with a pivot that has holes")
			}
		}
	}

	return nil
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
	err := idx.validatePivots(pivots)
	if err != nil {
		return err
	}

	for i, typ := range idx.Info.Types {
		// if index and pivot are typed but not of the same type
		// return no result
		//
		// don't try to check in case we have less pivots than values
		if i >= len(pivots) {
			break
		}

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
	if idx.IsComposite() {
		// v has been turned into an array of values being indexed
		// TODO add a check
		array := v.V.(*document.ValueBuffer)

		// in the case of one of the index keys being untyped and the corresponding
		// value being an integer, convert it into a double.
		err := array.Iterate(func(i int, vi document.Value) error {
			if idx.Info.Types[i] != untypedValue {
				return nil
			}

			var err error
			if vi.Type == document.IntegerValue {
				if vi.V == nil {
					vi.Type = document.DoubleValue
				} else {
					vi, err = vi.CastAsDouble()
					if err != nil {
						return err
					}
				}

				// update the value with its new type
				return array.Replace(i, vi)
			}

			return nil
		})

		if err != nil {
			return nil, err
		}

		// encode the array
		return v.MarshalBinary()
	}

	if idx.Info.Types[0] != 0 {
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

	for i, typ := range idx.Info.Types {
		if i < len(pivots) && typ == 0 && pivots[i].Type == document.IntegerValue {
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

	if idx.IsComposite() {
		// if we have n valueless and typeless pivots, we just iterate
		all := true
		for _, pivot := range pivots {
			if pivot.Type == 0 && pivot.V == nil {
				all = all && true
			} else {
				all = false
				break
			}
		}

		// we do have pivot values/types, so let's use them to seek in the index
		if !all {
			// TODO delete
			// if the first pivot is valueless but typed, we iterate but filter out the types we don't want
			// but just for the first pivot.
			if pivots[0].Type != 0 && pivots[0].V == nil {
				seek = []byte{byte(pivots[0].Type)}
			} else {
				vb := document.NewValueBuffer(pivots...)
				seek, err = idx.EncodeValue(document.NewArrayValue(vb))

				if err != nil {
					return err
				}
			}
		} else { // we don't, let's start at the beginning
			seek = []byte{}
		}

		if reverse {
			// if we are reverse on a pivot with less arity, we will get 30 255, which is lower than 31
			// and such will ignore all values. Let's drop the separator in that case
			if len(seek) > 0 {
				seek = append(seek[:len(seek)-1], 0xFF)
			} else {
				seek = append(seek, 0xFF)
			}

		}
	} else {
		if pivots[0].V != nil {
			seek, err = idx.EncodeValue(pivots[0])
			if err != nil {
				return err
			}

			if reverse {
				seek = append(seek, 0xFF)
			}
		} else {
			if idx.Info.Types[0] == untypedValue && pivots[0].Type != untypedValue && pivots[0].V == nil {
				seek = []byte{byte(pivots[0].Type)}

				if reverse {
					seek = append(seek, 0xFF)
				}
			}
		}
	}

	it := st.Iterator(engine.IteratorOptions{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		itm := it.Item()

		// if index is untyped and pivot is typed, only iterate on values with the same type as pivot
		if idx.IsComposite() {
			// for now, we only check the first element
			if idx.Info.Types[0] == 0 && pivots[0].Type != 0 && itm.Key()[0] != byte(pivots[0].Type) {
				return nil
			}
		} else {
			var typ document.ValueType
			if len(idx.Info.Types) > 0 {
				typ = idx.Info.Types[0]
			}

			if (typ == 0) && pivots[0].Type != 0 && itm.Key()[0] != byte(pivots[0].Type) {
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

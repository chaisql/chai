package database

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/stringutil"
)

const (
	// indexStorePrefix is the prefix used to name the index stores.
	indexStorePrefix = "i"
)

var (
	// ErrIndexDuplicateValue is returned when a value is already associated with a key
	ErrIndexDuplicateValue = errors.New("duplicate value")

	// ErrIndexWrongArity is returned when trying to index more values that what an
	// index supports.
	ErrIndexWrongArity = errors.New("wrong index arity")
)

// An Index associates encoded values with keys.
//
// The association is performed by encoding the values in a binary format that preserve
// ordering when compared lexicographically. For the implementation, see the binarysort
// package and the document.ValueEncoder.
type Index struct {
	Info *IndexInfo

	tx        engine.Transaction
	storeName []byte
}

// indexValueEncoder encodes a field based on its type; if a type is provided,
// the value is encoded as is, without any type information. Otherwise, the
// type is prepended to the value.
type indexValueEncoder struct {
	typ document.ValueType
	w   io.Writer
}

func (e *indexValueEncoder) EncodeValue(v document.Value) error {
	// if the index has no type constraint, encode the value with its type
	if e.typ.IsAny() {
		// prepend with the type
		_, err := e.w.Write([]byte{byte(v.Type)})
		if err != nil {
			return err
		}

		// marshal the value, if it exists, just return the type otherwise
		if v.V != nil {
			b, err := v.MarshalBinary()
			if err != nil {
				return err
			}

			_, err = e.w.Write(b)
			if err != nil {
				return err
			}
		}

		return nil
	}

	if v.Type != e.typ {
		if v.Type.IsAny() {
			v.Type = e.typ
		} else {
			// this should never happen, but if it does, something is very wrong
			panic("incompatible index type")
		}
	}

	if v.V == nil {
		return nil
	}

	// there is a type constraint, so a shorter form can be used as the type is always the same
	b, err := v.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = e.w.Write(b)
	return err
}

// NewIndex creates an index that associates values with a list of keys.
func NewIndex(tx engine.Transaction, idxName string, opts *IndexInfo) *Index {
	if opts == nil {
		opts = &IndexInfo{
			Types: []document.ValueType{document.AnyType},
		}
	}

	// if no types are provided, it implies that it's an index for single untyped values
	if opts.Types == nil {
		opts.Types = []document.ValueType{document.AnyType}
	}

	return &Index{
		tx:        tx,
		storeName: append([]byte(indexStorePrefix), idxName...),
		Info:      opts,
	}
}

var errStop = errors.New("stop")

// IsComposite returns true if the index is defined to operate on at least more than one value.
func (idx *Index) IsComposite() bool {
	return len(idx.Info.Types) > 1
}

// Arity returns how many values the index is operating on.
// For example, an index created with `CREATE INDEX idx_a_b ON foo (a, b)` has an arity of 2.
func (idx *Index) Arity() int {
	return len(idx.Info.Types)
}

// Set associates values with a key. If Unique is set to false, it is
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

	if len(vs) != idx.Arity() {
		return stringutil.Errorf("cannot index %d values on an index of arity %d", len(vs), len(idx.Info.Types))
	}

	for i, typ := range idx.Info.Types {
		if !typ.IsAny() && typ != vs[i].Type {
			return stringutil.Errorf("cannot index value of type %s in %s index", vs[i].Type, typ)
		}
	}

	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	// encode the value we are going to use as a key
	var buf []byte
	vb := document.NewValueBuffer(vs...)
	buf, err = idx.EncodeValueBuffer(vb)

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

type Pivot []document.Value

// validate panics when the pivot values are unsuitable for the index:
// - having pivot length superior to the index arity
// - having the first pivot element without a value when the subsequent ones do have values
func (pivot Pivot) validate(idx *Index) {
	if len(pivot) > idx.Arity() {
		panic("cannot iterate with a pivot whose size is superior to the index arity")
	}

	if idx.IsComposite() && !pivot.IsAny() {
		// it's acceptable for the last pivot element to just have a type and no value
		hasValue := true
		for _, p := range pivot {
			// if on the previous pivot we have a value
			if hasValue {
				hasValue = p.V != nil
			} else {
				panic("cannot iterate on a composite index with a pivot with both values and nil values")
			}
		}
	}
}

// IsAny return true if every value of the pivot is typed with AnyType
func (pivot Pivot) IsAny() bool {
	res := true
	for _, p := range pivot {
		res = res && p.Type.IsAny() && p.V == nil
		if !res {
			break
		}
	}

	return res
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot(s) is/are empty, starts from the beginning.
//
// Valid pivots are:
// - zero value pivot
//   - iterate on everything
// - n elements pivot (where n is the index arity) with each element having a value and a type
//   - iterate starting at the closest index value
//   - optionally, the last pivot element can have just a type and no value, which will scope the value of that element to that type
// - less than n elements pivot, with each element having a value and a type
//   - iterate starting at the closest index value, using the first known value for missing elements
//   - optionally, the last pivot element can have just a type and no value, which will scope the value of that element to that type
// - a single element with a type but nil value: will iterate on everything of that type
//
// Any other variation of a pivot are invalid and will panic.
func (idx *Index) AscendGreaterOrEqual(pivot Pivot, fn func(val, key []byte) error) error {
	return idx.iterateOnStore(pivot, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot(s) is/are empty, starts from the end.
//
// Valid pivots are:
// - zero value pivot
//   - iterate on everything
// - n elements pivot (where n is the index arity) with each element having a value and a type
//   - iterate starting at the closest index value
//   - optionally, the last pivot element can have just a type and no value, which will scope the value of that element to that type
// - less than n elements pivot, with each element having a value and a type
//   - iterate starting at the closest index value, using the last known value for missing elements
//   - optionally, the last pivot element can have just a type and no value, which will scope the value of that element to that type
// - a single element with a type but nil value: will iterate on everything of that type
//
// Any other variation of a pivot are invalid and will panic.
func (idx *Index) DescendLessOrEqual(pivot Pivot, fn func(val, key []byte) error) error {
	return idx.iterateOnStore(pivot, true, fn)
}

func (idx *Index) iterateOnStore(pivot Pivot, reverse bool, fn func(val, key []byte) error) error {
	pivot.validate(idx)

	// If index and pivot values are typed but not of the same type, return no results.
	for i, pv := range pivot {
		if !pv.Type.IsAny() && !idx.Info.Types[i].IsAny() && pv.Type != idx.Info.Types[i] {
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

// EncodeValueBuffer encodes the value buffer containing a single or
// multiple values being indexed into a byte array, keeping the
// order of the original values.
//
// The values are marshalled and separated with a document.ArrayValueDelim,
// *without* a trailing document.ArrayEnd, which enables to handle cases
// where only some of the values are being provided and still perform lookups
// (like index_foo_a_b_c and providing only a and b).
//
// See IndexValueEncoder for details about how the value themselves are encoded.
func (idx *Index) EncodeValueBuffer(vb *document.ValueBuffer) ([]byte, error) {
	if vb.Len() > idx.Arity() {
		return nil, ErrIndexWrongArity
	}

	var buf bytes.Buffer

	err := vb.Iterate(func(i int, value document.Value) error {
		enc := &indexValueEncoder{typ: idx.Info.Types[i], w: &buf}
		err := enc.EncodeValue(value)
		if err != nil {
			return err
		}

		// if it's not the last value, append the seperator
		if i < vb.Len()-1 {
			err = buf.WriteByte(document.ArrayValueDelim)
			if err != nil {
				return err
			}
		}

		return nil
	})

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

// buildSeek encodes the pivot values as binary in order to seek into the indexed data.
func (idx *Index) buildSeek(pivot Pivot, reverse bool) ([]byte, error) {
	var seek []byte
	var err error

	// if we have valueless and typeless pivot, we just iterate
	if pivot.IsAny() {
		return []byte{}, nil
	}

	// if the index is without type and the first pivot is valueless but typed, iterate but filter out the types we don't want,
	// but just for the first pivot; subsequent pivot values cannot be filtered this way.
	if idx.Info.Types[0].IsAny() && !pivot[0].Type.IsAny() && pivot[0].V == nil {
		seek = []byte{byte(pivot[0].Type)}

		if reverse {
			seek = append(seek, 0xFF)
		}

		return seek, nil
	}

	vb := document.NewValueBuffer(pivot...)
	seek, err = idx.EncodeValueBuffer(vb)

	if err != nil {
		return nil, err
	}

	if reverse {
		seek = append(seek, 0xFF)
	}

	return seek, nil
}

func (idx *Index) iterate(st engine.Store, pivot Pivot, reverse bool, fn func(item engine.Item) error) error {
	var err error

	seek, err := idx.buildSeek(pivot, reverse)
	if err != nil {
		return err
	}

	it := st.Iterator(engine.IteratorOptions{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		itm := it.Item()

		// If index is untyped and pivot first element is typed, only iterate on values with the same type as the first pivot
		if len(pivot) > 0 && idx.Info.Types[0].IsAny() && !pivot[0].Type.IsAny() && itm.Key()[0] != byte(pivot[0].Type) {
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

package database

import (
	"bytes"
	"encoding/binary"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
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
// package and the types.ValueEncoder.
type Index struct {
	Info *IndexInfo

	tx engine.Transaction
}

// NewIndex creates an index that associates values with a list of keys.
func NewIndex(tx engine.Transaction, idxName string, opts *IndexInfo) *Index {
	if opts == nil {
		opts = &IndexInfo{
			Types: []types.ValueType{types.AnyType},
		}
	}

	// if no types are provided, it implies that it's an index for single untyped values
	if opts.Types == nil {
		opts.Types = []types.ValueType{types.AnyType}
	}

	return &Index{
		tx:   tx,
		Info: opts,
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
//
// Values are stored in the index following the "index format".
// Every record is stored like this:
//   k: <encoded values><primary key>
//   v: length of the encoded value, as an unsigned varint
func (idx *Index) Set(vs []types.Value, pk []byte) error {
	if len(pk) == 0 {
		return errors.New("cannot index value without a key")
	}

	if len(vs) == 0 {
		return errors.New("cannot index without a value")
	}

	if len(vs) != idx.Arity() {
		return stringutil.Errorf("cannot index %d values on an index of arity %d", len(vs), len(idx.Info.Types))
	}

	for i, typ := range idx.Info.Types {
		if !typ.IsAny() && typ != vs[i].Type() {
			return stringutil.Errorf("cannot index value of type %s in %s index", vs[i].Type(), typ)
		}
	}

	st, err := getOrCreateStore(idx.tx, idx.Info.StoreName)
	if err != nil {
		return nil
	}

	var storeKey []byte

	// encode the value we are going to use as a key
	vb := document.NewValueBuffer(vs...)
	storeKey, err = idx.EncodeValueBuffer(vb)
	if err != nil {
		return err
	}

	// if the index is unique, we need to check if the value is already associated with the key
	if idx.Info.Unique {
		ok, _, err := idx.exists(st, storeKey)
		if err != nil {
			return err
		}
		if ok {
			return errors.Wrap(ErrIndexDuplicateValue)
		}
	}

	// append the pk at the end of the encoded value
	storeKey = append(storeKey, pk...)

	// then append the length of the key
	vbuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(vbuf, uint64(len(pk)))
	storeKey = append(storeKey, vbuf[:n]...)
	// finally, append the size of the varint
	storeKey = append(storeKey, byte(n))

	return st.Put(storeKey, []byte{0})
}

func (idx *Index) Exists(vs []types.Value) (bool, []byte, error) {
	if len(vs) != idx.Arity() {
		return false, nil, stringutil.Errorf("required arity of %d", len(idx.Info.Types))
	}

	st, err := idx.tx.GetStore(idx.Info.StoreName)
	if err != nil {
		if errors.Is(err, engine.ErrStoreNotFound) {
			return false, nil, nil
		}

		return false, nil, err
	}

	// encode the value we are going to use as a key
	vb := document.NewValueBuffer(vs...)
	buf, err := idx.EncodeValueBuffer(vb)
	if err != nil {
		return false, nil, err
	}

	return idx.exists(st, buf)
}

// iterates over the index and check if the value exists
func (idx *Index) exists(st engine.Store, seek []byte) (bool, []byte, error) {
	it := st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		itm := it.Item()
		k := itm.Key()
		if len(seek) > len(k) {
			return false, nil, nil
		}

		value, pk := idx.decodeItemKey(k)

		if bytes.Equal(seek, value) {
			return true, pk, nil
		}
	}

	return false, nil, it.Err()
}

// Delete all the references to the key from the index.
func (idx *Index) Delete(vs []types.Value, k []byte) error {
	st, err := getOrCreateStore(idx.tx, idx.Info.StoreName)
	if err != nil {
		return nil
	}

	err = idx.iterate(st, vs, false, func(itmKey, value, pk []byte) error {
		if bytes.Equal(pk, k) {
			err = st.Delete(itmKey)
			if err == nil {
				err = errStop
			}

			return err
		}

		return nil
	})
	if errors.Is(err, errStop) {
		return nil
	}
	if err != nil {
		return err
	}

	return engine.ErrKeyNotFound
}

type Pivot []types.Value

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
				hasValue = p.V() != nil
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
		res = res && p == nil
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
		if pv != nil && !idx.Info.Types[i].IsAny() && pv.Type() != idx.Info.Types[i] {
			return nil
		}
	}

	st, err := idx.tx.GetStore(idx.Info.StoreName)
	if err != nil && !errors.Is(err, engine.ErrStoreNotFound) {
		return err
	}
	if st == nil {
		return nil
	}

	return idx.iterate(st, pivot, reverse, func(itmKey, value, pk []byte) error {
		return fn(value, pk)
	})
}

// Truncate deletes all the index data.
func (idx *Index) Truncate() error {
	err := idx.tx.DropStore(idx.Info.StoreName)
	if err != nil && !errors.Is(err, engine.ErrStoreNotFound) {
		return err
	}

	return nil
}

// EncodeValueBuffer encodes the value buffer containing a single or
// multiple values being indexed into a byte array, keeping the
// order of the original values.
//
// The values are marshalled and separated with a types.ArrayValueDelim,
// *without* a trailing types.ArrayEnd, which enables to handle cases
// where only some of the values are being provided and still perform lookups
// (like index_foo_a_b_c and providing only a and b).
//
// See IndexValueEncoder for details about how the value themselves are encoded.
func (idx *Index) EncodeValueBuffer(vb *document.ValueBuffer) ([]byte, error) {
	if vb.Len() > idx.Arity() {
		return nil, errors.Wrap(ErrIndexWrongArity)
	}

	var buf bytes.Buffer

	err := types.NewValueEncoder(&buf).Encode(types.NewArrayValue(vb))
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

	if !errors.Is(err, engine.ErrStoreNotFound) {
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
	if idx.Info.Types[0].IsAny() && !pivot[0].Type().IsAny() && pivot[0].V() == nil {
		seek = []byte{byte(types.ArrayValue), byte(pivot[0].Type())}

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

	// remove ArrayEnd from the encoded array
	seek = bytes.TrimSuffix(seek, []byte{types.ArrayEnd})

	if reverse {
		seek = append(seek, 0xFF)
	}

	return seek, nil
}

func (idx *Index) iterate(st engine.Store, pivot Pivot, reverse bool, fn func(itmKey, value, pk []byte) error) error {
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
		if len(pivot) > 0 && idx.Info.Types[0].IsAny() && pivot[0] != nil && itm.Key()[1] != byte(pivot[0].Type()) {
			return nil
		}

		itmKey := itm.Key()

		value, pk := idx.decodeItemKey(itmKey)

		err := fn(itmKey, value, pk)
		if err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}

	return nil
}

func (idx *Index) decodeItemKey(k []byte) (value, pk []byte) {
	// the last byte of the key is the size of the varint representing the size of the primary key.
	varintSize := k[len(k)-1]
	varint := k[len(k)-1-int(varintSize) : len(k)-1]

	keySize, _ := binary.Uvarint(varint)

	value = k[:len(k)-int(keySize)-int(varintSize)-1]
	pk = k[len(value) : len(value)+int(keySize)]
	return
}

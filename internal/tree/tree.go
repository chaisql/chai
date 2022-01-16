package tree

import (
	"bytes"

	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
)

// A Tree is an abstraction over a k-v store that allows
// manipulating data using high level keys and values of the
// Genji type system.
// Trees are used as the basis for tables and indexes.
// The key of a tree is a composite combination of several
// values, while the value can be any value of Genji's type system.
// The tree ensures all keys are sort-ordered according to the rules
// of the types package operators.
// A Tree doesn't support duplicate keys.
type Tree struct {
	Store          *kv.Store
	TransientStore *kv.TransientStore
}

func New(store *kv.Store) *Tree {
	return &Tree{
		Store: store,
	}
}

func NewTransient(store *kv.TransientStore) *Tree {
	return &Tree{
		TransientStore: store,
	}
}

// Put adds or replaces a key-value combination to the tree.
// If the key already exists, its value will be replaced by
// the given value.
func (t *Tree) Put(key Key, value types.Value) error {
	var enc []byte

	if value == nil {
		value = types.NewNullValue()
	}

	var buf bytes.Buffer

	err := encoding.EncodeValue(&buf, value)
	if err != nil {
		return err
	}

	enc = buf.Bytes()

	if t.TransientStore != nil {
		return t.TransientStore.Put(key, enc)
	}

	return t.Store.Put(key, enc)
}

// Get a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Get(key Key) (types.Value, error) {
	if t.TransientStore != nil {
		panic("Get not implemented on transient tree")
	}

	var v Value
	item, err := t.Store.Get(key)
	if err != nil {
		return nil, err
	}

	v.item = item

	return &v, nil
}

// Delete a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Delete(key Key) error {
	if t.TransientStore != nil {
		panic("Delete not implemented on transient tree")
	}

	return t.Store.Delete(key)
}

// Truncate the tree.
func (t *Tree) Truncate() error {
	if t.TransientStore != nil {
		panic("Truncate not implemented on transient tree")
	}

	return t.Store.Truncate()
}

// Iterate over the tree.
// If the pivot is nil and reverse is false, it iterates from the lowest key onwards.
// If the pivot is nil and reverse if true, it iterates from the highest key downwards.
// If the pivot is not nil, it seeks that key in the tree before iterating over
// anything equal, and higher or lower depending on if reverse is false or true.
func (t *Tree) Iterate(pivot Key, reverse bool, fn func(Key, types.Value) error) error {
	var seek []byte

	if pivot != nil {
		seek = pivot
		if reverse {
			seek = append(seek, encoding.ArrayValueDelim, 0xFF)
		}
	}

	return t.iterateRaw(seek, reverse, fn)
}

func (t *Tree) iterateRaw(seek []byte, reverse bool, fn func(Key, types.Value) error) error {
	var it *kv.Iterator

	if t.TransientStore != nil {
		it = t.TransientStore.Iterator(kv.IteratorOptions{Reverse: reverse})
	} else {
		it = t.Store.Iterator(kv.IteratorOptions{Reverse: reverse})
	}
	defer it.Close()

	var value Value

	for it.Seek(seek); it.Valid(); it.Next() {
		i := it.Item()
		value.item = i
		value.v = nil

		err := fn(i.Key(), &value)
		if err != nil {
			return err
		}
	}

	return it.Err()
}

// IterateOnRange iterates on all keys that are in the given range.
// Depending on the direction, the range is translated to the following table:
// | SQL   | Range            | Direction | Seek    | End     |
// | ----- | ---------------- | --------- | ------- | ------- |
// | = 10  | Min: 10, Max: 10 | ASC       | 10      | 10      |
// | > 10  | Min: 10, Excl    | ASC       | 10+0xFF | nil     |
// | >= 10 | Min: 10          | ASC       | 10      | nil     |
// | < 10  | Max: 10, Excl    | ASC       | nil     | 10 excl |
// | <= 10 | Max: 10          | ASC       | nil     | 10      |
// | = 10  | Min: 10, Max: 10 | DESC      | 10+0xFF | 10      |
// | > 10  | Min: 10, Excl    | DESC      | nil     | 10 excl |
// | >= 10 | Min: 10          | DESC      | nil     | 10      |
// | < 10  | Max: 10, Excl    | DESC      | 10      | nil     |
// | <= 10 | Max: 10          | DESC      | 10+0xFF | nil     |
func (t *Tree) IterateOnRange(rng *Range, reverse bool, fn func(Key, types.Value) error) error {
	var err error

	var start, end []byte

	if rng != nil {
		if !reverse {
			start = rng.Min
			if start != nil && rng.Exclusive {
				start = append(start, encoding.ArrayValueDelim, 0xFF)
			}
			end = rng.Max
		} else {
			start = rng.Max
			if start != nil && !rng.Exclusive {
				start = append(start, encoding.ArrayValueDelim, 0xFF)
			}
			end = rng.Min
		}
	}

	if end == nil {
		return t.iterateRaw(start, reverse, fn)
	}

	err = t.iterateRaw(start, reverse, func(k Key, v types.Value) error {
		cmpWith := k

		if len(cmpWith) > len(end) {
			cmpWith = cmpWith[:len(end)]
		}

		cmp := bytes.Compare(cmpWith, end)
		if rng.Exclusive {
			if !reverse && cmp >= 0 {
				return errStop
			}
			if reverse && cmp <= 0 {
				return errStop
			}
		} else {
			if !reverse && cmp > 0 {
				return errStop
			}
			if reverse && cmp < 0 {
				return errStop
			}
		}

		return fn(k, v)
	})
	if err == errStop {
		err = nil
	}

	return err
}

var errStop = errors.New("stop")

// Value is an implementation of the types.Value interface returned by Tree.
// It is used to lazily decode values from the underlying store.
type Value struct {
	item *kv.Item
	v    types.Value
	buf  []byte
}

func (v *Value) decode() {
	if v.v != nil {
		return
	}

	var err error
	v.buf, err = v.item.ValueCopy(v.buf)
	if err != nil {
		panic(err)
	}

	v.v, err = encoding.DecodeValue(v.buf)
	if err != nil {
		panic(err)
	}
}

func (v *Value) Type() types.ValueType {
	v.decode()

	return v.v.Type()
}

func (v *Value) V() interface{} {
	v.decode()

	return v.v.V()
}

func (v *Value) String() string {
	v.decode()

	return v.v.String()
}

func (v *Value) MarshalJSON() ([]byte, error) {
	v.decode()

	return v.v.MarshalJSON()
}

func (v *Value) MarshalText() ([]byte, error) {
	v.decode()

	return v.v.MarshalText()
}

// A Range of keys to iterate on.
// By default, Min and Max are inclusive.
// If Exclusive is true, Min and Max are excluded
// from the results.
// If Type is provided, the results will be filtered by that type.
type Range struct {
	Min, Max  Key
	Exclusive bool
}

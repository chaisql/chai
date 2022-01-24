package tree

import (
	"bytes"

	"github.com/cockroachdb/pebble"
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
	vv, err := t.Store.Get(key)
	if err != nil {
		return nil, err
	}

	v.encoded = vv

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
	var start, end []byte

	if rng == nil {
		rng = &Range{}
	}

	if !rng.Exclusive {
		if rng.Min == nil {
			start = t.buildFirstKey()
		} else {
			start = t.buildStartKeyInclusive(rng.Min)
		}
		if rng.Max == nil {
			end = t.buildLastKey()
		} else {
			end = t.buildEndKeyInclusive(rng.Max)
		}
	} else {
		if rng.Min == nil {
			start = t.buildFirstKey()
		} else {
			start = t.buildStartKeyExclusive(rng.Min)
		}
		if rng.Max == nil {
			end = t.buildLastKey()
		} else {
			end = t.buildEndKeyExclusive(rng.Max)
		}
	}

	var it *pebble.Iterator
	opts := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	if t.TransientStore != nil {
		it = t.TransientStore.Iterator(&opts)
	} else {
		it = t.Store.Iterator(&opts)
	}
	defer it.Close()

	if !reverse {
		it.First()
	} else {
		it.Last()
	}

	var value Value

	for it.Valid() {
		value.encoded = it.Value()
		value.v = nil

		err := fn(bytes.TrimPrefix(it.Key(), t.buildKey(nil)), &value)
		if err != nil {
			return err
		}

		if !reverse {
			it.Next()
		} else {
			it.Prev()
		}
	}

	return it.Error()
}

func (t *Tree) buildKey(key Key) []byte {
	if t.Store != nil {
		return kv.BuildKey(t.Store.Prefix, key)
	}

	return key
}

func (t *Tree) buildFirstKey() []byte {
	return t.buildKey(nil)
}

func (t *Tree) buildLastKey() []byte {
	k := t.buildKey(nil)
	if len(k) == 0 {
		return []byte{0xFF}
	}

	k[len(k)-1] = 0xff
	return k
}

func (t *Tree) buildStartKeyInclusive(key []byte) []byte {
	return t.buildKey(key)
}

func (t *Tree) buildStartKeyExclusive(key []byte) []byte {
	return append(t.buildKey(key), encoding.ArrayValueDelim, 0xFF)
}

func (t *Tree) buildEndKeyInclusive(key []byte) []byte {
	k := t.buildKey(key)
	k = append(k, encoding.ArrayValueDelim, 0xFF)
	return k
}

func (t *Tree) buildEndKeyExclusive(key []byte) []byte {
	return t.buildKey(key)
}

// Value is an implementation of the types.Value interface returned by Tree.
// It is used to lazily decode values from the underlying store.
type Value struct {
	encoded []byte
	v       types.Value
}

func (v *Value) decode() {
	if v.v != nil {
		return
	}

	var err error
	v.v, err = encoding.DecodeValue(v.encoded)
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

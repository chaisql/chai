package tree

import (
	"bytes"
	"io"

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
	Namespace      *kv.Namespace
	TransientStore *kv.TransientStore
}

func New(ns *kv.Namespace) *Tree {
	return &Tree{
		Namespace: ns,
	}
}

func NewTransient(store *kv.TransientStore) *Tree {
	return &Tree{
		TransientStore: store,
	}
}

var defaultValue = []byte{0}

// Put adds or replaces a key-doc combination to the tree.
// If the key already exists, its value will be replaced by
// the given value.
func (t *Tree) Put(key Key, value []byte) error {
	if len(value) == 0 {
		value = defaultValue
	}
	var err error
	if t.TransientStore != nil {
		err = t.TransientStore.Put(key, value)
	} else {
		err = t.Namespace.Put(key, value)
	}
	return err
}

// Get a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Get(key Key) ([]byte, error) {
	if t.TransientStore != nil {
		panic("Get not implemented on transient tree")
	}

	return t.Namespace.Get(key)
}

// Exists returns true if the key exists in the tree.
func (t *Tree) Exists(key Key) (bool, error) {
	if t.TransientStore != nil {
		panic("Exists not implemented on transient tree")
	}

	return t.Namespace.Exists(key)
}

// Delete a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Delete(key Key) error {
	if t.TransientStore != nil {
		panic("Delete not implemented on transient tree")
	}

	return t.Namespace.Delete(key)
}

// Truncate the tree.
func (t *Tree) Truncate() error {
	if t.TransientStore != nil {
		panic("Truncate not implemented on transient tree")
	}

	return t.Namespace.Truncate()
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
func (t *Tree) IterateOnRange(rng *Range, reverse bool, fn func(Key, []byte) error) error {
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

	var it *kv.Iterator
	opts := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	if t.TransientStore != nil {
		it = t.TransientStore.Iterator(&opts)
	} else {
		it = t.Namespace.Iterator(&opts)
	}
	defer it.Close()

	if !reverse {
		it.First()
	} else {
		it.Last()
	}

	prefix := t.buildKey(nil)
	for it.Valid() {
		err := fn(bytes.TrimPrefix(it.Key(), prefix), it.Value())
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
	if t.Namespace != nil {
		return kv.BuildKey(t.Namespace.ID, key)
	}

	return key
}

func (t *Tree) buildFirstKey() []byte {
	return t.buildKey(nil)
}

func (t *Tree) buildLastKey() []byte {
	if t.Namespace != nil {
		return t.Namespace.ID.UpperBound()
	}
	return []byte{0xFF}
}

func (t *Tree) buildStartKeyInclusive(key []byte) []byte {
	return t.buildKey(key)
}

func (t *Tree) buildStartKeyExclusive(key []byte) []byte {
	return append(t.buildKey(key), encoding.ArrayValueDelim, 0xFF)
}

func (t *Tree) buildEndKeyInclusive(key []byte) []byte {
	return append(t.buildKey(key), encoding.ArrayValueDelim, 0xFF)
}

func (t *Tree) buildEndKeyExclusive(key []byte) []byte {
	return t.buildKey(key)
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

type Codec interface {
	Encode(w io.Writer, d types.Document) error
	Decode([]byte) (types.Document, error)
}

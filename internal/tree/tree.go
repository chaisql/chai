package tree

import (
	"fmt"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/types"
)

type Namespace int64

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
	Session   kv.Session
	Namespace Namespace
}

func New(session kv.Session, ns Namespace) *Tree {
	return &Tree{
		Namespace: ns,
		Session:   session,
	}
}

func NewTransient(session kv.Session, ns Namespace) (*Tree, func() error, error) {
	t := Tree{
		Namespace: ns,
		Session:   session,
	}

	// ensure the namespace is not in use
	err := t.IterateOnRange(nil, false, func(k *Key, b []byte) error {
		return errors.Errorf("namespace %d is already in use", ns)
	})
	if err != nil {
		return nil, nil, err
	}

	return &t, t.Truncate, nil
}

var defaultValue = []byte{0}

// Insert adds a key-doc combination to the tree.
// If the key already exists, it returns kv.ErrKeyAlreadyExists.
func (t *Tree) Insert(key *Key, value []byte) error {
	if len(value) == 0 {
		value = defaultValue
	}
	k, err := key.Encode(t.Namespace)
	if err != nil {
		return err
	}

	return t.Session.Insert(k, value)
}

// Put adds or replaces a key-doc combination to the tree.
// If the key already exists, its value will be replaced by
// the given value.
func (t *Tree) Put(key *Key, value []byte) error {
	if len(value) == 0 {
		value = defaultValue
	}
	k, err := key.Encode(t.Namespace)
	if err != nil {
		return err
	}

	return t.Session.Put(k, value)
}

// Get a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Get(key *Key) ([]byte, error) {
	k, err := key.Encode(t.Namespace)
	if err != nil {
		return nil, err
	}

	return t.Session.Get(k)
}

// Exists returns true if the key exists in the tree.
func (t *Tree) Exists(key *Key) (bool, error) {
	k, err := key.Encode(t.Namespace)
	if err != nil {
		return false, err
	}

	return t.Session.Exists(k)
}

// Delete a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Delete(key *Key) error {
	k, err := key.Encode(t.Namespace)
	if err != nil {
		return err
	}

	return t.Session.Delete(k)
}

// Truncate the tree.
func (t *Tree) Truncate() error {
	return t.Session.DeleteRange(encoding.EncodeInt(nil, int64(t.Namespace)), encoding.EncodeInt(nil, int64(t.Namespace)+1))
}

// IterateOnRange iterates on all keys that are in the given range.
func (t *Tree) IterateOnRange(rng *Range, reverse bool, fn func(*Key, []byte) error) error {
	var start, end []byte
	var err error

	if rng == nil {
		rng = &Range{}
	}

	if !rng.Exclusive {
		if rng.Min == nil {
			start, err = t.buildMinKeyForType(rng.Max)
		} else {
			start, err = t.buildStartKeyInclusive(rng.Min)
		}
		if err != nil {
			return err
		}
		if rng.Max == nil {
			end, err = t.buildMaxKeyForType(rng.Min)
		} else {
			end, err = t.buildEndKeyInclusive(rng.Max)
		}
	} else {
		if rng.Min == nil {
			start, err = t.buildMinKeyForType(rng.Max)
		} else {
			start, err = t.buildStartKeyExclusive(rng.Min)
		}
		if err != nil {
			return err
		}
		if rng.Max == nil {
			end, err = t.buildMaxKeyForType(rng.Min)
		} else {
			end, err = t.buildEndKeyExclusive(rng.Max)
		}
	}
	if err != nil {
		return err
	}

	opts := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	it := t.Session.Iterator(&opts)
	defer it.Close()

	if !reverse {
		it.First()
	} else {
		it.Last()
	}

	var k Key
	for it.Valid() {
		k.Encoded = it.Key()
		k.Values = nil

		err := fn(&k, it.Value())
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

func (t *Tree) buildFirstKey() ([]byte, error) {
	k := NewKey()
	return k.Encode(t.Namespace)
}

func (t *Tree) buildMinKeyForType(max *Key) ([]byte, error) {
	if max == nil {
		return t.buildFirstKey()
	}

	if len(max.Values) == 1 {
		return NewKey(t.NewMinValueForType(max.Values[0].Type())).Encode(t.Namespace)
	}

	var values []types.Value
	for i := range max.Values {
		if i < len(max.Values)-1 {
			values = append(values, max.Values[i])
			continue
		}

		values = append(values, t.NewMinValueForType(max.Values[i].Type()))
	}

	return NewKey(values...).Encode(t.Namespace)
}

func (t *Tree) buildMaxKeyForType(min *Key) ([]byte, error) {
	if min == nil {
		return t.buildLastKey(), nil
	}

	if len(min.Values) == 1 {
		buf := encoding.EncodeInt(nil, int64(t.Namespace))
		return append(buf, byte(t.NewMaxTypeForType(min.Values[0].Type()))), nil
	}

	buf, err := NewKey(min.Values[:len(min.Values)-1]...).Encode(t.Namespace)
	if err != nil {
		return nil, err
	}
	return append(buf, byte(t.NewMaxTypeForType(min.Values[len(min.Values)-1].Type()))), nil
}

func (t *Tree) buildLastKey() []byte {
	buf := encoding.EncodeInt(nil, int64(t.Namespace))
	return append(buf, 0xFF)
}

func (t *Tree) buildStartKeyInclusive(key *Key) ([]byte, error) {
	return key.Encode(t.Namespace)
}

func (t *Tree) buildStartKeyExclusive(key *Key) ([]byte, error) {
	b, err := key.Encode(t.Namespace)
	if err != nil {
		return nil, err
	}

	return append(b, 0xFF), nil
}

func (t *Tree) buildEndKeyInclusive(key *Key) ([]byte, error) {
	b, err := key.Encode(t.Namespace)
	if err != nil {
		return nil, err
	}

	return append(b, 0xFF), nil
}

func (t *Tree) buildEndKeyExclusive(key *Key) ([]byte, error) {
	return key.Encode(t.Namespace)
}

func (t *Tree) NewMinValueForType(tp types.ValueType) types.Value {
	switch tp {
	case types.NullValue:
		return types.NewNullValue()
	case types.BooleanValue:
		return types.NewBoolValue(false)
	case types.IntegerValue:
		return types.NewIntegerValue(math.MinInt64)
	case types.DoubleValue:
		return types.NewDoubleValue(-math.MaxFloat64)
	case types.TextValue:
		return types.NewTextValue("")
	case types.BlobValue:
		return types.NewBlobValue(nil)
	case types.ArrayValue:
		return types.NewArrayValue(nil)
	case types.DocumentValue:
		return types.NewDocumentValue(nil)
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func (t *Tree) NewMaxTypeForType(tp types.ValueType) types.ValueType {
	switch tp {
	case types.NullValue:
		return 0x06 // NullValue = 0x05
	case types.BooleanValue:
		return 0x12 // TrueValue = 0x10, FalseValue = 0x11
	case types.IntegerValue:
		return 0xC8 // Integers go from 0x20 to 0xC7
	case types.DoubleValue:
		return 0xD2 // Doubles go from 0xD0 to 0xD1
	case types.TextValue:
		return 0xDB // TextValue = 0xDA
	case types.BlobValue:
		return 0xE1 // BlobValue = 0xE0
	case types.ArrayValue:
		return 0xE7 // ArrayValue = 0xE6
	case types.DocumentValue:
		return 0xF1 // DocumentValue = 0xF0
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

// A Range of keys to iterate on.
// By default, Min and Max are inclusive.
// If Exclusive is true, Min and Max are excluded
// from the results.
type Range struct {
	Min, Max  *Key
	Exclusive bool
}

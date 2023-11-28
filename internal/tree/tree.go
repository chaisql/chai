package tree

import (
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/types"
)

type Namespace uint64

// SortOrder is a 64-bit unsigned integer that represents
// the sort order (ASC or DESC) of each value in a key.
// By default, all values are sorted in ascending order.
// Each bit represents the sort order of the corresponding value
// in the key.
// SortOrder is used in a tree to encode keys.
// It can only support up to 64 values.
type SortOrder uint64

func (o SortOrder) IsDesc(i int) bool {
	if i > 63 {
		panic(fmt.Sprintf("cannot get sort order of value %d, only 64 values are supported", i))
	}

	mask := uint64(1) << (63 - i)
	return uint64(o)&mask>>(63-i) != 0
}

func (o SortOrder) SetDesc(i int) SortOrder {
	if i > 63 {
		panic(fmt.Sprintf("cannot set sort order of value %d, only 64 values are supported", i))
	}

	mask := uint64(1) << (63 - i)
	return SortOrder(uint64(o) | mask)
}

func (o SortOrder) SetAsc(i int) SortOrder {
	if i > 63 {
		panic(fmt.Sprintf("cannot set sort order of value %d, only 64 values are supported", i))
	}
	mask := uint64(1) << (63 - i)
	return SortOrder(uint64(o) &^ mask)
}

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
	Order     SortOrder
}

func New(session kv.Session, ns Namespace, order SortOrder) *Tree {
	return &Tree{
		Namespace: ns,
		Session:   session,
		Order:     order,
	}
}

func NewTransient(session kv.Session, ns Namespace, order SortOrder) (*Tree, func() error, error) {
	t := Tree{
		Namespace: ns,
		Session:   session,
		Order:     order,
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
	k, err := key.Encode(t.Namespace, t.Order)
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
	k, err := key.Encode(t.Namespace, t.Order)
	if err != nil {
		return err
	}

	return t.Session.Put(k, value)
}

// Get a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Get(key *Key) ([]byte, error) {
	k, err := key.Encode(t.Namespace, t.Order)
	if err != nil {
		return nil, err
	}

	return t.Session.Get(k)
}

// Exists returns true if the key exists in the tree.
func (t *Tree) Exists(key *Key) (bool, error) {
	k, err := key.Encode(t.Namespace, t.Order)
	if err != nil {
		return false, err
	}

	return t.Session.Exists(k)
}

// Delete a key from the tree. If the key doesn't exist,
// it returns kv.ErrKeyNotFound.
func (t *Tree) Delete(key *Key) error {
	k, err := key.Encode(t.Namespace, t.Order)
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

	var min, max *Key
	desc := t.isDescRange(rng)
	if !desc {
		min, max = rng.Min, rng.Max
	} else {
		min, max = rng.Max, rng.Min
	}

	if !rng.Exclusive {
		start, end, err = t.buildInclusiveBoundaries(min, max, desc)
	} else {
		start, end, err = t.buildExclusiveBoundaries(min, max, desc)
	}
	if err != nil {
		return err
	}

	opts := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	it, err := t.Session.Iterator(&opts)
	if err != nil {
		return err
	}
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

		v, err := it.ValueAndErr()
		if err != nil {
			return err
		}
		err = fn(&k, v)
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

func (t *Tree) isDescRange(rng *Range) bool {
	if rng.Min != nil {
		return t.Order.IsDesc(len(rng.Min.Values) - 1)
	}
	if rng.Max != nil {
		return t.Order.IsDesc(len(rng.Max.Values) - 1)
	}

	return false
}

func (t *Tree) buildInclusiveBoundaries(min, max *Key, desc bool) (start []byte, end []byte, err error) {
	if min == nil {
		start, err = t.buildMinKeyForType(max, desc)
	} else {
		start, err = t.buildStartKeyInclusive(min, desc)
	}
	if err != nil {
		return
	}
	if max == nil {
		end, err = t.buildMaxKeyForType(min, desc)
	} else {
		end, err = t.buildEndKeyInclusive(max, desc)
	}
	return
}

func (t *Tree) buildExclusiveBoundaries(min, max *Key, desc bool) (start []byte, end []byte, err error) {
	if min == nil {
		start, err = t.buildMinKeyForType(max, desc)
	} else {
		start, err = t.buildStartKeyExclusive(min, desc)
	}
	if err != nil {
		return
	}
	if max == nil {
		end, err = t.buildMaxKeyForType(min, desc)
	} else {
		end, err = t.buildEndKeyExclusive(max, desc)
	}
	return
}

func (t *Tree) buildFirstKey() ([]byte, error) {
	k := NewKey()
	return k.Encode(t.Namespace, t.Order)
}

func (t *Tree) buildMinKeyForType(max *Key, desc bool) ([]byte, error) {
	if max == nil {
		k, err := t.buildFirstKey()
		if err != nil {
			return nil, err
		}
		return k, nil
	}

	if len(max.Values) == 1 {
		buf := encoding.EncodeInt(nil, int64(t.Namespace))
		if desc {
			return append(buf, byte(t.NewMinTypeForTypeDesc(max.Values[0].Type()))), nil
		}

		return append(buf, byte(t.NewMinTypeForType(max.Values[0].Type()))), nil
	}

	buf, err := NewKey(max.Values[:len(max.Values)-1]...).Encode(t.Namespace, t.Order)
	if err != nil {
		return nil, err
	}
	i := len(max.Values) - 1
	if desc {
		return append(buf, byte(t.NewMinTypeForTypeDesc(max.Values[i].Type()))), nil
	}

	return append(buf, byte(t.NewMinTypeForType(max.Values[i].Type()))), nil
}

func (t *Tree) buildMaxKeyForType(min *Key, desc bool) ([]byte, error) {
	if min == nil {
		return t.buildLastKey(), nil
	}

	if len(min.Values) == 1 {
		buf := encoding.EncodeInt(nil, int64(t.Namespace))
		if desc {
			return append(buf, byte(t.NewMaxTypeForTypeDesc(min.Values[0].Type()))), nil
		}
		return append(buf, byte(t.NewMaxTypeForType(min.Values[0].Type()))), nil
	}

	buf, err := NewKey(min.Values[:len(min.Values)-1]...).Encode(t.Namespace, t.Order)
	if err != nil {
		return nil, err
	}
	i := len(min.Values) - 1
	if desc {
		return append(buf, byte(t.NewMaxTypeForTypeDesc(min.Values[i].Type()))), nil
	}

	return append(buf, byte(t.NewMaxTypeForType(min.Values[i].Type()))), nil
}

func (t *Tree) buildLastKey() []byte {
	buf := encoding.EncodeInt(nil, int64(t.Namespace))
	return append(buf, 0xFF)
}

func (t *Tree) buildStartKeyInclusive(key *Key, desc bool) ([]byte, error) {
	return key.Encode(t.Namespace, t.Order)
}

func (t *Tree) buildStartKeyExclusive(key *Key, desc bool) ([]byte, error) {
	b, err := key.Encode(t.Namespace, t.Order)
	if err != nil {
		return nil, err
	}

	return append(b, 0xFF), nil
}

func (t *Tree) buildEndKeyInclusive(key *Key, desc bool) ([]byte, error) {
	b, err := key.Encode(t.Namespace, t.Order)
	if err != nil {
		return nil, err
	}

	return append(b, 0xFF), nil
}

func (t *Tree) buildEndKeyExclusive(key *Key, desc bool) ([]byte, error) {
	return key.Encode(t.Namespace, t.Order)
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
	case types.TimestampValue:
		return types.NewTimestampValue(time.Time{})
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

func (t *Tree) NewMinTypeForType(tp types.ValueType) byte {
	switch tp {
	case types.NullValue:
		return encoding.NullValue
	case types.BooleanValue:
		return encoding.FalseValue
	case types.IntegerValue:
		return encoding.Int64Value
	case types.DoubleValue:
		return encoding.Float64Value
	case types.TimestampValue:
		return encoding.Int64Value
	case types.TextValue:
		return encoding.TextValue
	case types.BlobValue:
		return encoding.BlobValue
	case types.ArrayValue:
		return encoding.ArrayValue
	case types.DocumentValue:
		return encoding.DocumentValue
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func (t *Tree) NewMinTypeForTypeDesc(tp types.ValueType) byte {
	switch tp {
	case types.NullValue:
		return encoding.DESC_NullValue
	case types.BooleanValue:
		return encoding.DESC_TrueValue
	case types.IntegerValue:
		return encoding.DESC_Uint64Value
	case types.DoubleValue:
		return encoding.DESC_Float64Value
	case types.TimestampValue:
		return encoding.DESC_Uint64Value
	case types.TextValue:
		return encoding.DESC_TextValue
	case types.BlobValue:
		return encoding.DESC_BlobValue
	case types.ArrayValue:
		return encoding.DESC_ArrayValue
	case types.DocumentValue:
		return encoding.DESC_DocumentValue
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func (t *Tree) NewMaxTypeForTypeDesc(tp types.ValueType) byte {
	switch tp {
	case types.NullValue:
		return encoding.DESC_NullValue + 1
	case types.BooleanValue:
		return encoding.DESC_FalseValue + 1
	case types.IntegerValue:
		return encoding.DESC_Int64Value + 1
	case types.DoubleValue:
		return encoding.DESC_Float64Value + 1
	case types.TimestampValue:
		return encoding.DESC_Int64Value + 1
	case types.TextValue:
		return encoding.DESC_TextValue + 1
	case types.BlobValue:
		return encoding.DESC_BlobValue + 1
	case types.ArrayValue:
		return encoding.DESC_ArrayValue + 1
	case types.DocumentValue:
		return encoding.DESC_DocumentValue + 1
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func (t *Tree) NewMinValueForTypeDesc(tp types.ValueType) types.Value {
	switch tp {
	case types.NullValue:
		return types.NewNullValue()
	case types.BooleanValue:
		return types.NewBoolValue(true)
	case types.IntegerValue:
		return types.NewIntegerValue(math.MaxInt64)
	case types.DoubleValue:
		return types.NewDoubleValue(math.MaxFloat64)
	case types.TimestampValue:
		return types.NewIntegerValue(math.MaxInt64)
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

func (t *Tree) NewMaxTypeForType(tp types.ValueType) byte {
	switch tp {
	case types.NullValue:
		return encoding.NullValue + 1
	case types.BooleanValue:
		return encoding.TrueValue + 1
	case types.IntegerValue:
		return encoding.Uint64Value + 1
	case types.DoubleValue:
		return encoding.Float64Value + 1
	case types.TimestampValue:
		return encoding.Uint64Value + 1
	case types.TextValue:
		return encoding.TextValue + 1
	case types.BlobValue:
		return encoding.BlobValue + 1
	case types.ArrayValue:
		return encoding.ArrayValue + 1
	case types.DocumentValue:
		return encoding.DocumentValue + 1
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

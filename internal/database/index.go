package database

import (
	"bytes"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
)

var (
	// ErrIndexDuplicateValue is returned when a value is already associated with a key
	ErrIndexDuplicateValue = errors.New("duplicate value")
)

// An Index associates encoded values with keys.
//
// The association is performed by encoding the values in a binary format that preserve
// ordering when compared lexicographically. For the implementation, see the binarysort
// package and the types.ValueEncoder.
type Index struct {
	// How many values the index is operating on.
	// For example, an index created with `CREATE INDEX idx_a_b ON foo (a, b)` has an arity of 2.
	Arity int
	Tree  *tree.Tree
}

// NewIndex creates an index that associates values with a list of keys.
func NewIndex(tr *tree.Tree, opts IndexInfo) *Index {
	return &Index{
		Tree:  tr,
		Arity: len(opts.Paths),
	}
}

var errStop = errors.New("stop")

// Set associates values with a key. If Unique is set to false, it is
// possible to associate multiple keys for the same value
// but a key can be associated to only one value.
//
// Values are stored in the index following the "index format".
// Every record is stored like this:
//   k: <encoded values><primary key>
//   v: length of the encoded value, as an unsigned varint
func (idx *Index) Set(vs []types.Value, key tree.Key) error {
	if len(key) == 0 {
		return errors.New("cannot index value without a key")
	}

	if len(vs) == 0 {
		return errors.New("cannot index without a value")
	}

	if len(vs) != idx.Arity {
		return stringutil.Errorf("cannot index %d values on an index of arity %d", len(vs), idx.Arity)
	}

	// append the key to the values
	values := append(vs, types.NewBlobValue(key))

	// create the key for the tree
	treeKey, err := tree.NewKey(values...)
	if err != nil {
		return err
	}

	return idx.Tree.Put(treeKey, nil)
}

// Exists iterates over the index and check if the value exists
func (idx *Index) Exists(vs []types.Value) (bool, tree.Key, error) {
	if len(vs) != idx.Arity {
		return false, nil, stringutil.Errorf("required arity of %d", idx.Arity)
	}

	seek, err := tree.NewKey(vs...)
	if err != nil {
		return false, nil, err
	}

	var found bool
	var dKey tree.Key

	err = idx.Tree.Iterate(seek, false, func(k tree.Key, v types.Value) error {
		if len(seek) > len(k) {
			return errStop
		}

		if !bytes.Equal(k[:len(seek)], seek) {
			return errStop
		}

		values, err := k.Decode()
		if err != nil {
			return err
		}

		dKey = values[len(values)-1].V().([]byte)
		found = true
		return errStop
	})
	if err == errStop {
		err = nil
	}
	return found, dKey, err
}

// Delete all the references to the key from the index.
func (idx *Index) Delete(vs []types.Value, key tree.Key) error {
	vk, err := tree.NewKey(vs...)
	if err != nil {
		return err
	}

	rng := tree.Range{
		Min: vk,
		Max: vk,
	}

	err = idx.iterateOnRange(&rng, false, func(itmKey tree.Key, pk tree.Key) error {
		if bytes.Equal(pk, key) {
			err := idx.Tree.Delete(itmKey)
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

// IterateOnRange seeks for the pivot and then goes through all the subsequent key value pairs in increasing or decreasing order and calls the given function for each pair.
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
func (idx *Index) IterateOnRange(rng *tree.Range, reverse bool, fn func(key tree.Key) error) error {
	if rng.Min == nil && rng.Max == nil {
		panic("range cannot be empty")
	}

	// if one of the boundaries is nil, ensure the iteration only returns
	// keys of the same type as the other boundary's first value.
	if rng.Min == nil {
		rng.Min = tree.NewMinKeyForType(types.ValueType(rng.Max[0]))
	} else if rng.Max == nil {
		rng.Max = tree.NewMaxKeyForType(types.ValueType(rng.Min[0]))
	}

	return idx.iterateOnRange(rng, reverse, func(itmKey, key tree.Key) error {
		return fn(key)
	})
}

func (idx *Index) iterateOnRange(rng *tree.Range, reverse bool, fn func(itmKey tree.Key, key tree.Key) error) error {
	return idx.Tree.IterateOnRange(rng, reverse, func(k tree.Key, v types.Value) error {
		// we don't care about the value, we just want to extract the key
		// which is the last element of the encoded array
		pos := bytes.LastIndex(k, []byte{encoding.ArrayValueDelim})

		kv, err := encoding.DecodeValue(k[pos+1:])
		if err != nil {
			return err
		}

		pk := tree.Key(kv.V().([]byte))

		return fn(k, pk)
	})
}

// Truncate deletes all the index data.
func (idx *Index) Truncate() error {
	return idx.Tree.Truncate()
}

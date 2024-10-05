package database

import (
	"bytes"
	"fmt"

	"github.com/chaisql/chai/internal/engine"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
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
		Arity: len(opts.Columns),
	}
}

var errStop = errors.New("stop")

// Set associates values with a key. If Unique is set to false, it is
// possible to associate multiple keys for the same value
// but a key can be associated to only one value.
//
// Values are stored in the index following the "index format".
// Every record is stored like this:
//
//	k: <encoded values><primary key>
//	v: length of the encoded value, as an unsigned varint
func (idx *Index) Set(vs []types.Value, key []byte) error {
	if key == nil {
		return errors.New("cannot index value without a key")
	}

	if len(vs) == 0 {
		return errors.New("cannot index without a value")
	}

	if len(vs) != idx.Arity {
		return fmt.Errorf("cannot index %d values on an index of arity %d", len(vs), idx.Arity)
	}

	// append the key to the values
	values := append(vs, types.NewBlobValue(key))

	// create the key for the tree
	treeKey := tree.NewKey(values...)

	return idx.Tree.Put(treeKey, nil)
}

// Exists iterates over the index and check if the value exists
func (idx *Index) Exists(vs []types.Value) (bool, *tree.Key, error) {
	if len(vs) != idx.Arity {
		return false, nil, fmt.Errorf("required arity of %d", idx.Arity)
	}

	seek := tree.NewKey(vs...)

	var found bool
	var dKey *tree.Key

	it, err := idx.Tree.Iterator(&tree.Range{Min: seek, Max: seek})
	if err != nil {
		return false, nil, err
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		k, err := it.Key().Decode()
		if err != nil {
			return false, nil, err
		}

		dKey = tree.NewEncodedKey(types.AsByteSlice(k[len(k)-1]))
		found = true
		break
	}

	return found, dKey, it.Error()
}

// Delete all the references to the key from the index.
func (idx *Index) Delete(vs []types.Value, key []byte) error {
	vk := tree.NewKey(vs...)
	rng := tree.Range{
		Min: vk,
		Max: vk,
	}

	err := idx.iterateOnRange(&rng, false, func(itmKey *tree.Key, pk *tree.Key) error {
		if bytes.Equal(pk.Encoded, key) {
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

	return errors.WithStack(engine.ErrKeyNotFound)
}

func (idx *Index) IterateOnRange(rng *tree.Range, reverse bool, fn func(key *tree.Key) error) error {
	return idx.iterateOnRange(rng, reverse, func(itmKey, key *tree.Key) error {
		return fn(key)
	})
}

func (idx *Index) iterateOnRange(rng *tree.Range, reverse bool, fn func(itmKey *tree.Key, key *tree.Key) error) error {
	it, err := idx.Tree.Iterator(rng)
	if err != nil {
		return err
	}
	defer it.Close()

	if !reverse {
		it.First()
	} else {
		it.Last()
	}

	for it.Valid() {
		k := it.Key()
		// we don't care about the value, we just want to extract the key
		// which is the last element of the encoded array
		values, err := k.Decode()
		if err != nil {
			return err
		}

		pk := tree.NewEncodedKey(types.AsByteSlice(values[len(values)-1]))

		err = fn(k, pk)
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

// Truncate deletes all the index data.
func (idx *Index) Truncate() error {
	return idx.Tree.Truncate()
}

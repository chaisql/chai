package query

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/google/btree"
)

type Matcher interface {
	Match(record.Record) (bool, error)
}

type matcher struct {
	fn func(record.Record) (bool, error)
}

func (m *matcher) Match(r record.Record) (bool, error) {
	return m.fn(r)
}

type IndexMatcher struct {
	Matcher

	fn func(im map[string]index.Index) (*btree.BTree, error)
}

func (m *IndexMatcher) MatchIndex(im map[string]index.Index) (*btree.BTree, error) {
	return m.fn(im)
}

type Item []byte

func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i, than.(Item)) < 0
}

func compareInts(f FieldSelector, op func(int64) bool) func(r record.Record) (bool, error) {
	return func(r record.Record) (bool, error) {
		rf, err := f.SelectField(r)
		if err != nil {
			return false, err
		}

		if rf.Type != field.Int64 {
			return false, errors.New("type mismatch")
		}

		v, err := field.DecodeInt64(rf.Data)
		if err != nil {
			return false, err
		}

		return op(v), nil
	}
}

func compareStrings(f FieldSelector, op func([]byte) bool) func(r record.Record) (bool, error) {
	return func(r record.Record) (bool, error) {
		rf, err := f.SelectField(r)
		if err != nil {
			return false, err
		}

		if rf.Type != field.String {
			return false, errors.New("type mismatch")
		}

		return op(rf.Data), nil
	}
}

func eqIndexMatcher(data []byte, idx index.Index) (*btree.BTree, error) {
	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(data)
	for rowid != nil && bytes.Equal(data, v) {
		tree.ReplaceOrInsert(Item(rowid))
		v, rowid = c.Next()
	}

	return tree, nil
}

func gtIndexMatcher(data []byte, idx index.Index) (*btree.BTree, error) {
	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(data)
	for rowid != nil {
		if !bytes.Equal(data, v) {
			tree.ReplaceOrInsert(Item(rowid))
		}

		v, rowid = c.Next()
	}

	return tree, nil
}

func gteIndexMatcher(data []byte, idx index.Index) (*btree.BTree, error) {
	tree := btree.New(3)

	c := idx.Cursor()
	_, rowid := c.Seek(data)
	for rowid != nil {
		tree.ReplaceOrInsert(Item(rowid))
		_, rowid = c.Next()
	}

	return tree, nil
}

func ltIndexMatcher(data []byte, idx index.Index) (*btree.BTree, error) {
	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(data)
	v, rowid = c.Prev()
	for rowid != nil {
		if !bytes.Equal(data, v) {
			tree.ReplaceOrInsert(Item(rowid))
		}
		v, rowid = c.Prev()
	}

	return tree, nil
}

func lteIndexMatcher(data []byte, idx index.Index) (*btree.BTree, error) {
	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(data)

	for bytes.Equal(v, data) {
		v, rowid = c.Next()
	}
	if v == nil {
		v, rowid = c.Last()
	}

	for rowid != nil {
		if bytes.Compare(v, data) <= 0 {
			tree.ReplaceOrInsert(Item(rowid))
		}

		v, rowid = c.Prev()
	}

	return tree, nil
}

func EqInt(f FieldSelector, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v == base
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return eqIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func GtInt(f FieldSelector, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v > base
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return gtIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func GteInt(f FieldSelector, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v >= base
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return gteIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func LtInt(f FieldSelector, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v < base
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return ltIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func LteInt(f FieldSelector, i int) *IndexMatcher {
	base := int64(i)
	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareInts(f, func(v int64) bool {
				return v <= base
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return lteIndexMatcher(field.EncodeInt64(base), im[f.Name()])
		},
	}
}

func EqStr(f FieldSelector, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Equal(v, base)
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return eqIndexMatcher(base, im[f.Name()])
		},
	}
}

func GtStr(f FieldSelector, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) > 0
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return gtIndexMatcher(base, im[f.Name()])
		},
	}
}

func GteStr(f FieldSelector, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) >= 0
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return gteIndexMatcher(base, im[f.Name()])
		},
	}
}

func LtStr(f FieldSelector, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) < 0
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return ltIndexMatcher(base, im[f.Name()])
		},
	}
}

func LteStr(f FieldSelector, s string) *IndexMatcher {
	base := []byte(s)

	return &IndexMatcher{
		Matcher: &matcher{
			fn: compareStrings(f, func(v []byte) bool {
				return bytes.Compare(v, base) <= 0
			}),
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			return lteIndexMatcher(base, im[f.Name()])
		},
	}
}

func And(matchers ...Matcher) *IndexMatcher {
	return &IndexMatcher{
		Matcher: &matcher{
			fn: func(r record.Record) (bool, error) {
				for _, m := range matchers {
					ok, err := m.Match(r)
					if !ok || err != nil {
						return ok, err
					}
				}

				return true, nil
			},
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			var set *btree.BTree

			for _, m := range matchers {
				if i, ok := m.(*IndexMatcher); ok {
					rowids, err := i.MatchIndex(im)
					if err != nil {
						return nil, err
					}

					if rowids.Len() == 0 {
						return nil, nil
					}

					if set == nil {
						set = rowids.Clone()
						continue
					}

					set = intersection(set, rowids)
					if set.Len() == 0 {
						return nil, nil
					}
				} else {
					return nil, nil
				}
			}

			return set, nil
		},
	}
}

func Or(matchers ...Matcher) *IndexMatcher {
	return &IndexMatcher{
		Matcher: &matcher{
			fn: func(r record.Record) (bool, error) {
				for _, m := range matchers {
					ok, err := m.Match(r)
					if err != nil {
						return false, err
					}

					if ok {
						return true, nil
					}
				}

				return false, nil
			},
		},

		fn: func(im map[string]index.Index) (*btree.BTree, error) {
			var set *btree.BTree

			for _, m := range matchers {
				if i, ok := m.(*IndexMatcher); ok {
					rowids, err := i.MatchIndex(im)
					if err != nil {
						return nil, err
					}

					if set == nil {
						set = rowids.Clone()
						continue
					}

					set = union(set, rowids)
				} else {
					return nil, nil
				}
			}

			return set, nil
		},
	}
}

func intersection(s1, s2 *btree.BTree) *btree.BTree {
	set := btree.New(3)

	s1.Ascend(func(i btree.Item) bool {
		if s2.Has(i) {
			set.ReplaceOrInsert(i)
		}

		return true
	})

	return set
}

func union(s1, s2 *btree.BTree) *btree.BTree {
	s2.Ascend(func(i btree.Item) bool {
		s1.ReplaceOrInsert(i)
		return true
	})

	return s1
}

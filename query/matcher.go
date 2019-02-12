package query

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
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

	fn func(table string, tx engine.Transaction) (*btree.BTree, error)
}

func (m *IndexMatcher) MatchIndex(table string, tx engine.Transaction) (*btree.BTree, error) {
	return m.fn(table, tx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return eqIndexMatcher(field.EncodeInt64(base), idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return gtIndexMatcher(field.EncodeInt64(base), idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return gteIndexMatcher(field.EncodeInt64(base), idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return ltIndexMatcher(field.EncodeInt64(base), idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return lteIndexMatcher(field.EncodeInt64(base), idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}

			return eqIndexMatcher(base, idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return gtIndexMatcher(base, idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return gteIndexMatcher(base, idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return ltIndexMatcher(base, idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			idx, err := tx.Index(table, f.Name())
			if err != nil {
				return nil, err
			}
			return lteIndexMatcher(base, idx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			var set *btree.BTree

			for _, m := range matchers {
				if i, ok := m.(*IndexMatcher); ok {
					rowids, err := i.MatchIndex(table, tx)
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

		fn: func(table string, tx engine.Transaction) (*btree.BTree, error) {
			var set *btree.BTree

			for _, m := range matchers {
				if i, ok := m.(*IndexMatcher); ok {
					rowids, err := i.MatchIndex(table, tx)
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

type indexResultTable struct {
	tree  *btree.BTree
	table table.Table
}

func (i *indexResultTable) Record(rowid []byte) (record.Record, error) {
	it := i.tree.Get(Item(rowid))
	if it == nil {
		return nil, engine.ErrNotFound
	}

	return i.table.Record(rowid)
}

func (i *indexResultTable) Iterate(fn func(record.Record) bool) error {
	var err error

	i.tree.Ascend(func(it btree.Item) bool {
		r, er := i.table.Record([]byte(it.(Item)))
		if err != nil {
			err = er
			return false
		}

		return fn(r)
	})
	return err
}

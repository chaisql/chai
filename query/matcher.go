package query

import (
	"bytes"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/google/btree"
)

type Matcher interface {
	Match(record.Record) (bool, error)
}

type IndexMatcher interface {
	MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error)
}

type Item []byte

func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i, than.(Item)) < 0
}

type EqMatcher struct {
	f FieldSelector
	v []byte
}

func EqInt(f FieldSelector, i int) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeInt64(int64(i)),
	}
}

func EqStr(f FieldSelector, s string) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: []byte(s),
	}
}

func (m *EqMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.f.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.v) == 0, nil
}

func (m *EqMatcher) MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error) {
	idx, err := tx.Index(table, m.f.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.v)
	for rowid != nil && bytes.Equal(m.v, v) {
		tree.ReplaceOrInsert(Item(rowid))
		v, rowid = c.Next()
	}

	return tree, true, nil
}

type GtMatcher struct {
	f FieldSelector
	v []byte
}

func GtInt(f FieldSelector, i int) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeInt64(int64(i)),
	}
}

func GtStr(f FieldSelector, s string) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: []byte(s),
	}
}

func (m *GtMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.f.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.v) > 0, nil
}

func (m *GtMatcher) MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error) {
	idx, err := tx.Index(table, m.f.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.v)
	for rowid != nil {
		if !bytes.Equal(m.v, v) {
			tree.ReplaceOrInsert(Item(rowid))
		}

		v, rowid = c.Next()
	}

	return tree, true, nil
}

type GteMatcher struct {
	f FieldSelector
	v []byte
}

func GteInt(f FieldSelector, i int) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeInt64(int64(i)),
	}
}

func GteStr(f FieldSelector, s string) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: []byte(s),
	}
}

func (m *GteMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.f.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.v) >= 0, nil
}

func (m *GteMatcher) MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error) {
	idx, err := tx.Index(table, m.f.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	_, rowid := c.Seek(m.v)
	for rowid != nil {
		tree.ReplaceOrInsert(Item(rowid))
		_, rowid = c.Next()
	}

	return tree, true, nil
}

type LtMatcher struct {
	f FieldSelector
	v []byte
}

func LtInt(f FieldSelector, i int) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeInt64(int64(i)),
	}
}

func LtStr(f FieldSelector, s string) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: []byte(s),
	}
}

func (m *LtMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.f.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.v) < 0, nil
}

func (m *LtMatcher) MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error) {
	idx, err := tx.Index(table, m.f.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.v)
	v, rowid = c.Prev()
	for rowid != nil {
		if !bytes.Equal(m.v, v) {
			tree.ReplaceOrInsert(Item(rowid))
		}
		v, rowid = c.Prev()
	}

	return tree, true, nil
}

type LteMatcher struct {
	f FieldSelector
	v []byte
}

func LteInt(f FieldSelector, i int) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeInt64(int64(i)),
	}
}

func LteStr(f FieldSelector, s string) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: []byte(s),
	}
}

func (m *LteMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.f.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.v) <= 0, nil
}

func (m *LteMatcher) MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error) {
	idx, err := tx.Index(table, m.f.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.v)

	for bytes.Equal(v, m.v) {
		v, rowid = c.Next()
	}
	if v == nil {
		v, rowid = c.Last()
	}

	for rowid != nil {
		if bytes.Compare(v, m.v) <= 0 {
			tree.ReplaceOrInsert(Item(rowid))
		}

		v, rowid = c.Prev()
	}

	return tree, true, nil
}

type AndMatcher struct {
	matchers []Matcher
}

func And(matchers ...Matcher) *AndMatcher {
	return &AndMatcher{matchers: matchers}
}

func (a *AndMatcher) Match(r record.Record) (bool, error) {
	for _, m := range a.matchers {
		ok, err := m.Match(r)
		if !ok || err != nil {
			return ok, err
		}
	}

	return true, nil
}

func (a *AndMatcher) MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error) {
	var set *btree.BTree

	for _, m := range a.matchers {
		if i, ok := m.(IndexMatcher); ok {
			rowids, ok, err := i.MatchIndex(table, tx)
			if err != nil || !ok {
				return nil, false, err
			}

			if rowids.Len() == 0 {
				return nil, true, nil
			}

			if set == nil {
				set = rowids.Clone()
				continue
			}

			set = intersection(set, rowids)
			if set.Len() == 0 {
				return nil, true, nil
			}
		} else {
			return nil, false, nil
		}
	}

	return set, true, nil
}

type OrMatcher struct {
	matchers []Matcher
}

func Or(matchers ...Matcher) *OrMatcher {
	return &OrMatcher{matchers: matchers}
}

func (o *OrMatcher) Match(r record.Record) (bool, error) {
	for _, m := range o.matchers {
		ok, err := m.Match(r)
		if err != nil {
			return false, err
		}

		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (o *OrMatcher) MatchIndex(table string, tx *genji.Tx) (*btree.BTree, bool, error) {
	var set *btree.BTree

	for _, m := range o.matchers {
		if i, ok := m.(IndexMatcher); ok {
			rowids, ok, err := i.MatchIndex(table, tx)
			if err != nil || !ok {
				return nil, false, err
			}

			if set == nil {
				set = rowids.Clone()
				continue
			}

			set = union(set, rowids)
		} else {
			return nil, false, nil
		}
	}

	return set, true, nil
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
		return nil, table.ErrRecordNotFound
	}

	return i.table.Record(rowid)
}

func (i *indexResultTable) Iterate(fn func([]byte, record.Record) error) error {
	var err error

	i.tree.Ascend(func(it btree.Item) bool {
		var rec record.Record
		rowid := []byte(it.(Item))
		rec, err = i.table.Record(rowid)
		if err != nil {
			return false
		}

		err = fn(rowid, rec)
		return err == nil
	})

	return err
}

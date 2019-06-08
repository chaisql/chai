//go:generate go run ./internal/gen.go

package query

import (
	"bytes"

	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/google/btree"
)

// A Matcher defines conditions and indicates if a record
// satisfies them.
// Implementation can operate on a specific field
// or on the entire record.
type Matcher interface {
	// Match returns true if the given record matches.
	Match(record.Record) (bool, error)
}

// An IndexMatcher defines conditions and scans an index for records
// satisfying them.
type IndexMatcher interface {
	// MatcheIndex returns a tree of all the records rowids matching.
	// If no index is found for a given field, it returns nil and false.
	MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error)
}

// An Item is an element stored in a tree.
// It implements the btree.Item interface.
type Item []byte

// Less implements the bree.Item interface to
// sort the Item in the tree.
// It compares i with than using bytes.Compare.
func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i, than.(Item)) < 0
}

// EqMatcher matches all the records whose field selected by the Field member are equal
// to the Value member. It also supports selecting records from indexes.
type EqMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *EqMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is equal to the Value member.
func (m *EqMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) == 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member equal to the Value member.
func (m *EqMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	idx, err := tx.Index(tableName, m.Field.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.Value)
	for rowid != nil && bytes.Equal(m.Value, v) {
		tree.ReplaceOrInsert(Item(rowid))
		v, rowid = c.Next()
	}

	return tree, true, nil
}

// GtMatcher matches all the records whose field selected by the Field member are strictly greater than
// the Value member. It also supports selecting records from indexes.
type GtMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *GtMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is strictly greater than the Value member.
func (m *GtMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) > 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member strictly greater than the Value member.
func (m *GtMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	idx, err := tx.Index(tableName, m.Field.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.Value)
	for rowid != nil {
		if !bytes.Equal(m.Value, v) {
			tree.ReplaceOrInsert(Item(rowid))
		}

		v, rowid = c.Next()
	}

	return tree, true, nil
}

// GteMatcher matches all the records whose field selected by the Field member are greater than or equal
// to the Value member. It also supports selecting records from indexes.
type GteMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *GteMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is greater than or equal to the Value member.
func (m *GteMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) >= 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member greater than or equal to the Value member.
func (m *GteMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	idx, err := tx.Index(tableName, m.Field.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	_, rowid := c.Seek(m.Value)
	for rowid != nil {
		tree.ReplaceOrInsert(Item(rowid))
		_, rowid = c.Next()
	}

	return tree, true, nil
}

// LtMatcher matches all the records whose field selected by the Field member are strictly lesser than
// the Value member. It also supports selecting records from indexes.
type LtMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *LtMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is strictly lesser than the Value member.
func (m *LtMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) < 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member strictly lesser than the Value member.
func (m *LtMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	idx, err := tx.Index(tableName, m.Field.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.Value)
	v, rowid = c.Prev()
	for rowid != nil {
		if !bytes.Equal(m.Value, v) {
			tree.ReplaceOrInsert(Item(rowid))
		}
		v, rowid = c.Prev()
	}

	return tree, true, nil
}

// LteMatcher matches all the records whose field selected by the Field member are lesser than or equal
// to the Value member. It also supports selecting records from indexes.
type LteMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *LteMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is lesser than or equal to the Value member.
func (m *LteMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) <= 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member lesser than or equal to the Value member.
func (m *LteMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	idx, err := tx.Index(tableName, m.Field.Name())
	if err != nil {
		return nil, false, err
	}

	tree := btree.New(3)

	c := idx.Cursor()
	v, rowid := c.Seek(m.Value)

	for bytes.Equal(v, m.Value) {
		v, rowid = c.Next()
	}
	if v == nil {
		v, rowid = c.Last()
	}

	for rowid != nil {
		if bytes.Compare(v, m.Value) <= 0 {
			tree.ReplaceOrInsert(Item(rowid))
		}

		v, rowid = c.Prev()
	}

	return tree, true, nil
}

// AndMatcher is a logical matcher used to evaluate multiple other matchers.
// It matches if all of them matches.
type AndMatcher struct {
	Matchers []Matcher
}

// And creates an AndMatcher.
func And(matchers ...Matcher) *AndMatcher {
	return &AndMatcher{Matchers: matchers}
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (a *AndMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := a.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match if all Matchers return true.
func (a *AndMatcher) Match(r record.Record) (bool, error) {
	for _, m := range a.Matchers {
		ok, err := m.Match(r)
		if !ok || err != nil {
			return ok, err
		}
	}

	return true, nil
}

// MatchIndex matches if all Matchers implement the IndexMatcher interface and return true.
// MatchIndex returns the intersection between all of trees returned.
func (a *AndMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	var set *btree.BTree

	for _, m := range a.Matchers {
		if i, ok := m.(IndexMatcher); ok {
			rowids, ok, err := i.MatchIndex(tx, tableName)
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

// OrMatcher is a logical matcher used to evaluate multiple other matchers.
// It matches if one of them matches.
type OrMatcher struct {
	Matchers []Matcher
}

// Or creates an OrMatcher.
func Or(matchers ...Matcher) *OrMatcher {
	return &OrMatcher{Matchers: matchers}
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (o *OrMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := o.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match if one of the Matchers return true.
func (o *OrMatcher) Match(r record.Record) (bool, error) {
	for _, m := range o.Matchers {
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

// MatchIndex matches if all Matchers implement the IndexMatcher interface and return true.
// MatchIndex returns the union between all of trees returned.
func (o *OrMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	var set *btree.BTree

	for _, m := range o.Matchers {
		if i, ok := m.(IndexMatcher); ok {
			rowids, ok, err := i.MatchIndex(tx, tableName)
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

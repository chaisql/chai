//go:generate go run ./internal/gen.go

package query

import (
	"bytes"

	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/google/btree"
)

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

// eqMatcher matches all the records whose field selected by the Field member are equal
// to the Value member. It also supports selecting records from indexes.
type eqMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *eqMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is equal to the Value member.
func (m *eqMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) == 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member equal to the Value member.
func (m *eqMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
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

// gtMatcher matches all the records whose field selected by the Field member are strictly greater than
// the Value member. It also supports selecting records from indexes.
type gtMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *gtMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is strictly greater than the Value member.
func (m *gtMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) > 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member strictly greater than the Value member.
func (m *gtMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
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

// gteMatcher matches all the records whose field selected by the Field member are greater than or equal
// to the Value member. It also supports selecting records from indexes.
type gteMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *gteMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is greater than or equal to the Value member.
func (m *gteMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) >= 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member greater than or equal to the Value member.
func (m *gteMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
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

// ltMatcher matches all the records whose field selected by the Field member are strictly lesser than
// the Value member. It also supports selecting records from indexes.
type ltMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *ltMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is strictly lesser than the Value member.
func (m *ltMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) < 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member strictly lesser than the Value member.
func (m *ltMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
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

// lteMatcher matches all the records whose field selected by the Field member are lesser than or equal
// to the Value member. It also supports selecting records from indexes.
type lteMatcher struct {
	Field FieldSelector
	Value []byte
}

// Eval implements the Expr interface. It calls the Match method and translates
// the result as a scalar.
func (m *lteMatcher) Eval(ctx EvalContext) (Scalar, error) {
	ok, err := m.Match(ctx.Record)
	if err != nil || !ok {
		return falseScalar, err
	}

	return trueScalar, err
}

// Match uses the field selector to select a field from r and returns true
// if its encoded value is lesser than or equal to the Value member.
func (m *lteMatcher) Match(r record.Record) (bool, error) {
	rf, err := m.Field.SelectField(r)
	if err != nil {
		return false, err
	}

	return bytes.Compare(rf.Data, m.Value) <= 0, nil
}

// MatchIndex selects the index from tx and returns all the rowids of the records that have the value of
// the field selected by the Field member lesser than or equal to the Value member.
func (m *lteMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
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

// andMatcher is a logical matcher used to evaluate multiple other matchers.
// It matches if all of them matches.
type andMatcher struct {
	exprs []Expr
}

// And creates an expression that evaluates all of the given expressions and returns true if all of them are truthy.
func And(exprs ...Expr) Expr {
	return &andMatcher{exprs: exprs}
}

// Eval implements the Expr interface.
func (a *andMatcher) Eval(ctx EvalContext) (Scalar, error) {
	for _, e := range a.exprs {
		s, err := e.Eval(ctx)
		if err != nil || !s.Truthy() {
			return falseScalar, err
		}
	}

	return trueScalar, nil
}

// MatchIndex matches if all exprs implement the IndexMatcher interface and return true.
// MatchIndex returns the intersection between all of trees returned.
func (a *andMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	var set *btree.BTree

	for _, e := range a.exprs {
		if i, ok := e.(IndexMatcher); ok {
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

// orMatcher is a logical matcher used to evaluate multiple other matchers.
// It matches if one of them matches.
type orMatcher struct {
	exprs []Expr
}

// Or creates an expression that evaluates all of the given expressions until one returns a truthy value, otherwise returns false.
func Or(exprs ...Expr) Expr {
	return &orMatcher{exprs: exprs}
}

// Eval implements the Expr interface.
func (o *orMatcher) Eval(ctx EvalContext) (Scalar, error) {
	for _, e := range o.exprs {
		s, err := e.Eval(ctx)
		if err != nil {
			return falseScalar, err
		}

		if s.Truthy() {
			return trueScalar, nil
		}
	}

	return falseScalar, nil
}

// MatchIndex matches if all Matchers implement the IndexMatcher interface and return true.
// MatchIndex returns the union between all of trees returned.
func (o *orMatcher) MatchIndex(tx *genji.Tx, tableName string) (*btree.BTree, bool, error) {
	var set *btree.BTree

	for _, e := range o.exprs {
		if i, ok := e.(IndexMatcher); ok {
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

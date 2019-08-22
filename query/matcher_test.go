package query_test

import (
	"fmt"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

func createRecord(age int) record.Record {
	var fb record.FieldBuffer

	fb.Add(field.NewInt64("age", int64(age)))

	return &fb
}

func TestMatchers(t *testing.T) {
	r := createRecord(10)
	tests := []struct {
		name    string
		matcher query.Expr
		match   bool
	}{
		{"eq", query.IntField("age").Eq(10), true},
		{"gt/10>10", query.IntField("age").Gt(10), false},
		{"gt/10>11", query.IntField("age").Gt(11), false},
		{"gt/10>9", query.IntField("age").Gt(9), true},
		{"gte/10>=10", query.IntField("age").Gte(10), true},
		{"gte/10>=11", query.IntField("age").Gte(11), false},
		{"gte/10>=9", query.IntField("age").Gte(9), true},
		{"lt/10<10", query.IntField("age").Lt(10), false},
		{"lt/10<11", query.IntField("age").Lt(11), true},
		{"lt/10<9", query.IntField("age").Lt(9), false},
		{"lte/10<=10", query.IntField("age").Lte(10), true},
		{"lte/10<=11", query.IntField("age").Lte(11), true},
		{"lte/10<=9", query.IntField("age").Lte(9), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matched, err := test.matcher.Eval(query.EvalContext{Record: r})
			require.NoError(t, err)
			require.Equal(t, test.match, matched.Truthy())
		})
	}
}

func createIndexes(t require.TestingT, ages, teams []indexPair) (*genji.Table, func()) {
	db, err := genji.New(memory.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	tb, err := tx.CreateTable("test")
	require.NoError(t, err)

	createIntIndex(t, tb, ages)
	createStrIndex(t, tb, teams)

	return tb, func() {
		tx.Rollback()
	}
}

func createIntIndex(t require.TestingT, tb *genji.Table, ages []indexPair) {
	idx, err := tb.CreateIndex("age", index.Options{})
	require.NoError(t, err)

	for _, pair := range ages {
		err := idx.Set(field.EncodeInt(pair.V.(int)), []byte(pair.R.(string)))
		require.NoError(t, err)
	}
}

func createStrIndex(t require.TestingT, tb *genji.Table, teams []indexPair) {
	idx, err := tb.CreateIndex("team", index.Options{})
	require.NoError(t, err)

	for _, pair := range teams {
		err := idx.Set([]byte(pair.V.(string)), []byte(pair.R.(string)))
		require.NoError(t, err)
	}
}

type indexPair struct {
	V, R interface{}
}

func TestIndexMatchers(t *testing.T) {
	tb, cleanup := createIndexes(t, []indexPair{{1, "z"}, {2, "y"}, {2, "x"}, {3, "a"}, {5, "b"}, {10, "c"}}, []indexPair{{"ACA", "x"}, {"LOSC", "a"}, {"OL", "z"}, {"OM", "b"}, {"OM", "y"}, {"PSG", "c"}})
	defer cleanup()

	tests := []struct {
		name      string
		matcher   query.Expr
		recordIDs []string
	}{
		{"eq/int/one", query.IntField("age").Eq(10), []string{"c"}},
		{"eq/int/multiple", query.IntField("age").Eq(2), []string{"x", "y"}},
		{"eq/int/none", query.IntField("age").Eq(15), nil},
		{"eq/str/one", query.StringField("team").Eq("PSG"), []string{"c"}},
		{"eq/str/multiple", query.StringField("team").Eq("OM"), []string{"b", "y"}},
		{"eq/str/none", query.StringField("team").Eq("SCB"), nil},
		{"gt/int/>10", query.IntField("age").Gt(10), nil},
		{"gt/int/>7", query.IntField("age").Gt(7), []string{"c"}},
		{"gt/int/>1", query.IntField("age").Gt(1), []string{"a", "b", "c", "x", "y"}},
		{"gt/int/>-1", query.IntField("age").Gt(-1), []string{"a", "b", "c", "x", "y", "z"}},
		{"gt/str/>PSG", query.StringField("team").Gt("PSG"), nil},
		{"gt/str/>OM", query.StringField("team").Gt("OM"), []string{"c"}},
		{"gt/str/>NICE", query.StringField("team").Gt("NICE"), []string{"b", "c", "y", "z"}},
		{"gt/str/>ACA", query.StringField("team").Gt("ACA"), []string{"a", "b", "c", "y", "z"}},
		{"gt/str/>A", query.StringField("team").Gt("A"), []string{"a", "b", "c", "x", "y", "z"}},
		{"gte/int/>=11", query.IntField("age").Gte(11), nil},
		{"gte/int/>=7", query.IntField("age").Gte(7), []string{"c"}},
		{"gte/int/>=2", query.IntField("age").Gte(2), []string{"a", "b", "c", "x", "y"}},
		{"gte/int/>=1", query.IntField("age").Gte(1), []string{"a", "b", "c", "x", "y", "z"}},
		{"gte/str/>=PSG", query.StringField("team").Gte("PSG"), []string{"c"}},
		{"gte/str/>=OM", query.StringField("team").Gte("OM"), []string{"b", "c", "y"}},
		{"gte/str/>=NICE", query.StringField("team").Gte("NICE"), []string{"b", "c", "y", "z"}},
		{"gte/str/>=ACA", query.StringField("team").Gte("ACA"), []string{"a", "b", "c", "x", "y", "z"}},
		{"lt/int/<1", query.IntField("age").Lt(1), nil},
		{"lt/int/<4", query.IntField("age").Lt(4), []string{"a", "x", "y", "z"}},
		{"lt/int/<10", query.IntField("age").Lt(10), []string{"a", "b", "x", "y", "z"}},
		{"lt/int/<11", query.IntField("age").Lt(11), []string{"a", "b", "c", "x", "y", "z"}},
		{"lt/str/<A", query.StringField("team").Lt("A"), nil},
		{"lt/str/<ACA", query.StringField("team").Lt("ACA"), nil},
		{"lt/str/<NICE", query.StringField("team").Lt("NICE"), []string{"a", "x"}},
		{"lt/str/<OM", query.StringField("team").Lt("OM"), []string{"a", "x", "z"}},
		{"lt/str/<STRASBOURG", query.StringField("team").Lt("STRASBOURG"), []string{"a", "b", "c", "x", "y", "z"}},
		{"lte/int/<=0", query.IntField("age").Lte(0), nil},
		{"lte/int/<=2", query.IntField("age").Lte(2), []string{"x", "y", "z"}},
		{"lte/int/<=4", query.IntField("age").Lte(4), []string{"a", "x", "y", "z"}},
		{"lte/int/<=10", query.IntField("age").Lte(10), []string{"a", "b", "c", "x", "y", "z"}},
		{"lte/int/<=11", query.IntField("age").Lte(11), []string{"a", "b", "c", "x", "y", "z"}},
		{"lte/str/<=A", query.StringField("team").Lte("A"), nil},
		{"lte/str/<=ACA", query.StringField("team").Lte("ACA"), []string{"x"}},
		{"lte/str/<=NICE", query.StringField("team").Lte("NICE"), []string{"a", "x"}},
		{"lte/str/<=OM", query.StringField("team").Lte("OM"), []string{"a", "b", "x", "y", "z"}},
		{"lte/str/<=STRASBOURG", query.StringField("team").Lte("STRASBOURG"), []string{"a", "b", "c", "x", "y", "z"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recordIDs, ok, err := test.matcher.(query.IndexMatcher).MatchIndex(tb)
			require.NoError(t, err)
			require.True(t, ok)
			var ids []string
			recordIDs.Ascend(func(i btree.Item) bool {
				ids = append(ids, string(i.(query.Item)))
				return true
			})

			require.EqualValues(t, test.recordIDs, ids)
		})
	}
}

type simpleExpr struct{}

func (s *simpleExpr) Eval(query.EvalContext) (query.Scalar, error) {
	return query.Scalar{Type: field.Bool, Data: field.EncodeBool(false)}, nil
}

func TestAndMatcher(t *testing.T) {
	t.Run("Matcher", func(t *testing.T) {
		m := query.And(
			query.IntField("age").Gt(2),
			query.IntField("age").Lt(10),
		)

		ok, err := m.Eval(query.EvalContext{Record: createRecord(5)})
		require.NoError(t, err)
		require.True(t, ok.Truthy())

		ok, err = m.Eval(query.EvalContext{Record: createRecord(10)})
		require.NoError(t, err)
		require.False(t, ok.Truthy())
	})

	t.Run("IndexMatcher", func(t *testing.T) {
		tb, cleanup := createIndexes(t, []indexPair{{1, "z"}, {2, "y"}, {2, "x"}, {3, "a"}, {5, "b"}, {10, "c"}}, nil)
		defer cleanup()

		tests := []struct {
			name     string
			exprs    []query.Expr
			expected []string
		}{
			{">2", []query.Expr{query.IntField("age").Gt(2)}, []string{"a", "b", "c"}},
			{">2 && <10", []query.Expr{query.IntField("age").Gt(2), query.IntField("age").Lt(10)}, []string{"a", "b"}},
			{">10 && <20", []query.Expr{query.IntField("age").Gt(10), query.IntField("age").Lt(20)}, []string{}},
			{">8 && <3", []query.Expr{query.IntField("age").Gt(8), query.IntField("age").Lt(3)}, []string{}},
			{">8 && non index matcher", []query.Expr{query.IntField("age").Gt(8), new(simpleExpr)}, []string{}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				m := query.And(test.exprs...)

				recordIDs, _, err := m.(query.IndexMatcher).MatchIndex(tb)
				require.NoError(t, err)

				ids := []string{}

				if recordIDs != nil {
					ids = make([]string, 0, recordIDs.Len())
					recordIDs.Ascend(func(i btree.Item) bool {
						ids = append(ids, string(i.(query.Item)))
						return true
					})
				}

				require.Equal(t, test.expected, ids)
			})
		}

	})
}

func TestOrMatcher(t *testing.T) {
	t.Run("Matcher", func(t *testing.T) {
		e := query.Or(
			query.IntField("age").Gt(8),
			query.IntField("age").Lt(2),
		)

		ok, err := e.Eval(query.EvalContext{Record: createRecord(1)})
		require.NoError(t, err)
		require.True(t, ok.Truthy())

		ok, err = e.Eval(query.EvalContext{Record: createRecord(9)})
		require.NoError(t, err)
		require.True(t, ok.Truthy())

		ok, err = e.Eval(query.EvalContext{Record: createRecord(5)})
		require.NoError(t, err)
		require.False(t, ok.Truthy())
	})

	t.Run("IndexMatcher", func(t *testing.T) {
		tb, cleanup := createIndexes(t, []indexPair{{1, "z"}, {2, "y"}, {2, "x"}, {3, "a"}, {5, "b"}, {10, "c"}}, nil)
		defer cleanup()

		tests := []struct {
			name     string
			exprs    []query.Expr
			expected []string
		}{
			{">2", []query.Expr{query.IntField("age").Gt(2)}, []string{"a", "b", "c"}},
			{">8 || <2", []query.Expr{query.IntField("age").Gt(8), query.IntField("age").Lt(2)}, []string{"c", "z"}},
			{">0 || <11", []query.Expr{query.IntField("age").Gt(0), query.IntField("age").Lt(11)}, []string{"a", "b", "c", "x", "y", "z"}},
			{">10 || <20", []query.Expr{query.IntField("age").Gt(10), query.IntField("age").Lt(20)}, []string{"a", "b", "c", "x", "y", "z"}},
			{">10 || >20", []query.Expr{query.IntField("age").Gt(10), query.IntField("age").Gt(20)}, []string{}},
			{">8 || non index matcher", []query.Expr{query.IntField("age").Gt(8), new(simpleExpr)}, []string{}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				m := query.Or(test.exprs...)

				recordIDs, _, err := m.(query.IndexMatcher).MatchIndex(tb)
				require.NoError(t, err)

				ids := []string{}

				if recordIDs != nil {
					ids = make([]string, 0, recordIDs.Len())
					recordIDs.Ascend(func(i btree.Item) bool {
						ids = append(ids, string(i.(query.Item)))
						return true
					})
				}

				require.Equal(t, test.expected, ids)
			})
		}

	})
}

func TestNotMatcher(t *testing.T) {
	t.Run("Matcher", func(t *testing.T) {
		ok, err := query.Not(query.Int32Value(10)).Eval(query.EvalContext{})
		require.NoError(t, err)
		require.False(t, ok.Truthy())

		ok, err = query.Not(query.Int32Value(0)).Eval(query.EvalContext{})
		require.NoError(t, err)
		require.True(t, ok.Truthy())
	})
}
func BenchmarkMatcher(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			records := make([]record.Record, size)
			for i := 0; i < size; i++ {
				records[i] = createRecord(i)
			}

			matcher := query.IntField("age").Eq(size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, r := range records {
					matcher.Eval(query.EvalContext{Record: r})
				}
			}
		})
	}
}

func BenchmarkIndexMatcher(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			ages := make([]indexPair, size)
			for i := 0; i < size; i++ {
				ages[i] = indexPair{V: i, R: fmt.Sprintf("%d", i)}
			}

			tb, cleanup := createIndexes(b, ages, nil)
			defer cleanup()

			matcher := query.IntField("age").Eq(size).(query.IndexMatcher)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				matcher.MatchIndex(tb)
			}
			b.StopTimer()
		})
	}
}

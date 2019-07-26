package query_test

import (
	"fmt"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
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
		{"eq", query.EqInt(query.Field("age"), 10), true},
		{"gt/10>10", query.GtInt(query.Field("age"), 10), false},
		{"gt/10>11", query.GtInt(query.Field("age"), 11), false},
		{"gt/10>9", query.GtInt(query.Field("age"), 9), true},
		{"gte/10>=10", query.GteInt(query.Field("age"), 10), true},
		{"gte/10>=11", query.GteInt(query.Field("age"), 11), false},
		{"gte/10>=9", query.GteInt(query.Field("age"), 9), true},
		{"lt/10<10", query.LtInt(query.Field("age"), 10), false},
		{"lt/10<11", query.LtInt(query.Field("age"), 11), true},
		{"lt/10<9", query.LtInt(query.Field("age"), 9), false},
		{"lte/10<=10", query.LteInt(query.Field("age"), 10), true},
		{"lte/10<=11", query.LteInt(query.Field("age"), 11), true},
		{"lte/10<=9", query.LteInt(query.Field("age"), 9), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matched, err := test.matcher.Eval(query.EvalContext{Record: r})
			require.NoError(t, err)
			require.Equal(t, test.match, matched.Truthy())
		})
	}
}

func createIndexes(t require.TestingT, ages, teams []indexPair) (*genji.Tx, func()) {
	db, err := genji.New(memory.NewEngine())
	tx, err := db.Begin(true)
	require.NoError(t, err)

	err = tx.CreateTable("test")
	require.NoError(t, err)

	createIntIndex(t, tx, ages)
	createStrIndex(t, tx, teams)

	return tx, func() {
		tx.Rollback()
		db.Close()
	}
}

func createIntIndex(t require.TestingT, tx *genji.Tx, ages []indexPair) {
	err := tx.CreateIndex("test", "age")
	require.NoError(t, err)
	idx, err := tx.Index("test", "age")
	require.NoError(t, err)

	for _, pair := range ages {
		err := idx.Set(field.EncodeInt(pair.V.(int)), []byte(pair.R.(string)))
		require.NoError(t, err)
	}
}

func createStrIndex(t require.TestingT, tx *genji.Tx, teams []indexPair) {
	err := tx.CreateIndex("test", "team")
	require.NoError(t, err)
	idx, err := tx.Index("test", "team")
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
	tx, cleanup := createIndexes(t, []indexPair{{1, "z"}, {2, "y"}, {2, "x"}, {3, "a"}, {5, "b"}, {10, "c"}}, []indexPair{{"ACA", "x"}, {"LOSC", "a"}, {"OL", "z"}, {"OM", "b"}, {"OM", "y"}, {"PSG", "c"}})
	defer cleanup()

	tests := []struct {
		name    string
		matcher query.Expr
		recordIDs  []string
	}{
		{"eq/int/one", query.EqInt(query.Field("age"), 10), []string{"c"}},
		{"eq/int/multiple", query.EqInt(query.Field("age"), 2), []string{"x", "y"}},
		{"eq/int/none", query.EqInt(query.Field("age"), 15), nil},
		{"eq/str/one", query.EqString(query.Field("team"), "PSG"), []string{"c"}},
		{"eq/str/multiple", query.EqString(query.Field("team"), "OM"), []string{"b", "y"}},
		{"eq/str/none", query.EqString(query.Field("team"), "SCB"), nil},
		{"gt/int/>10", query.GtInt(query.Field("age"), 10), nil},
		{"gt/int/>7", query.GtInt(query.Field("age"), 7), []string{"c"}},
		{"gt/int/>1", query.GtInt(query.Field("age"), 1), []string{"a", "b", "c", "x", "y"}},
		{"gt/int/>-1", query.GtInt(query.Field("age"), -1), []string{"a", "b", "c", "x", "y", "z"}},
		{"gt/str/>PSG", query.GtString(query.Field("team"), "PSG"), nil},
		{"gt/str/>OM", query.GtString(query.Field("team"), "OM"), []string{"c"}},
		{"gt/str/>NICE", query.GtString(query.Field("team"), "NICE"), []string{"b", "c", "y", "z"}},
		{"gt/str/>ACA", query.GtString(query.Field("team"), "ACA"), []string{"a", "b", "c", "y", "z"}},
		{"gt/str/>A", query.GtString(query.Field("team"), "A"), []string{"a", "b", "c", "x", "y", "z"}},
		{"gte/int/>=11", query.GteInt(query.Field("age"), 11), nil},
		{"gte/int/>=7", query.GteInt(query.Field("age"), 7), []string{"c"}},
		{"gte/int/>=2", query.GteInt(query.Field("age"), 2), []string{"a", "b", "c", "x", "y"}},
		{"gte/int/>=1", query.GteInt(query.Field("age"), 1), []string{"a", "b", "c", "x", "y", "z"}},
		{"gte/str/>=PSG", query.GteString(query.Field("team"), "PSG"), []string{"c"}},
		{"gte/str/>=OM", query.GteString(query.Field("team"), "OM"), []string{"b", "c", "y"}},
		{"gte/str/>=NICE", query.GteString(query.Field("team"), "NICE"), []string{"b", "c", "y", "z"}},
		{"gte/str/>=ACA", query.GteString(query.Field("team"), "ACA"), []string{"a", "b", "c", "x", "y", "z"}},
		{"lt/int/<1", query.LtInt(query.Field("age"), 1), nil},
		{"lt/int/<4", query.LtInt(query.Field("age"), 4), []string{"a", "x", "y", "z"}},
		{"lt/int/<10", query.LtInt(query.Field("age"), 10), []string{"a", "b", "x", "y", "z"}},
		{"lt/int/<11", query.LtInt(query.Field("age"), 11), []string{"a", "b", "c", "x", "y", "z"}},
		{"lt/str/<A", query.LtString(query.Field("team"), "A"), nil},
		{"lt/str/<ACA", query.LtString(query.Field("team"), "ACA"), nil},
		{"lt/str/<NICE", query.LtString(query.Field("team"), "NICE"), []string{"a", "x"}},
		{"lt/str/<OM", query.LtString(query.Field("team"), "OM"), []string{"a", "x", "z"}},
		{"lt/str/<STRASBOURG", query.LtString(query.Field("team"), "STRASBOURG"), []string{"a", "b", "c", "x", "y", "z"}},
		{"lte/int/<=0", query.LteInt(query.Field("age"), 0), nil},
		{"lte/int/<=2", query.LteInt(query.Field("age"), 2), []string{"x", "y", "z"}},
		{"lte/int/<=4", query.LteInt(query.Field("age"), 4), []string{"a", "x", "y", "z"}},
		{"lte/int/<=10", query.LteInt(query.Field("age"), 10), []string{"a", "b", "c", "x", "y", "z"}},
		{"lte/int/<=11", query.LteInt(query.Field("age"), 11), []string{"a", "b", "c", "x", "y", "z"}},
		{"lte/str/<=A", query.LteString(query.Field("team"), "A"), nil},
		{"lte/str/<=ACA", query.LteString(query.Field("team"), "ACA"), []string{"x"}},
		{"lte/str/<=NICE", query.LteString(query.Field("team"), "NICE"), []string{"a", "x"}},
		{"lte/str/<=OM", query.LteString(query.Field("team"), "OM"), []string{"a", "b", "x", "y", "z"}},
		{"lte/str/<=STRASBOURG", query.LteString(query.Field("team"), "STRASBOURG"), []string{"a", "b", "c", "x", "y", "z"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recordIDs, ok, err := test.matcher.(query.IndexMatcher).MatchIndex(tx, "test")
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
			query.GtInt(query.Field("age"), 2),
			query.LtInt(query.Field("age"), 10),
		)

		ok, err := m.Eval(query.EvalContext{Record: createRecord(5)})
		require.NoError(t, err)
		require.True(t, ok.Truthy())

		ok, err = m.Eval(query.EvalContext{Record: createRecord(10)})
		require.NoError(t, err)
		require.False(t, ok.Truthy())
	})

	t.Run("IndexMatcher", func(t *testing.T) {
		tx, cleanup := createIndexes(t, []indexPair{{1, "z"}, {2, "y"}, {2, "x"}, {3, "a"}, {5, "b"}, {10, "c"}}, nil)
		defer cleanup()

		tests := []struct {
			name     string
			exprs    []query.Expr
			expected []string
		}{
			{">2", []query.Expr{query.GtInt(query.Field("age"), 2)}, []string{"a", "b", "c"}},
			{">2 && <10", []query.Expr{query.GtInt(query.Field("age"), 2), query.LtInt(query.Field("age"), 10)}, []string{"a", "b"}},
			{">10 && <20", []query.Expr{query.GtInt(query.Field("age"), 10), query.LtInt(query.Field("age"), 20)}, []string{}},
			{">8 && <3", []query.Expr{query.GtInt(query.Field("age"), 8), query.LtInt(query.Field("age"), 3)}, []string{}},
			{">8 && non index matcher", []query.Expr{query.GtInt(query.Field("age"), 8), new(simpleExpr)}, []string{}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				m := query.And(test.exprs...)

				recordIDs, _, err := m.(query.IndexMatcher).MatchIndex(tx, "test")
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
			query.GtInt(query.Field("age"), 8),
			query.LtInt(query.Field("age"), 2),
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
		tx, cleanup := createIndexes(t, []indexPair{{1, "z"}, {2, "y"}, {2, "x"}, {3, "a"}, {5, "b"}, {10, "c"}}, nil)
		defer cleanup()

		tests := []struct {
			name     string
			exprs    []query.Expr
			expected []string
		}{
			{">2", []query.Expr{query.GtInt(query.Field("age"), 2)}, []string{"a", "b", "c"}},
			{">8 || <2", []query.Expr{query.GtInt(query.Field("age"), 8), query.LtInt(query.Field("age"), 2)}, []string{"c", "z"}},
			{">0 || <11", []query.Expr{query.GtInt(query.Field("age"), 0), query.LtInt(query.Field("age"), 11)}, []string{"a", "b", "c", "x", "y", "z"}},
			{">10 || <20", []query.Expr{query.GtInt(query.Field("age"), 10), query.LtInt(query.Field("age"), 20)}, []string{"a", "b", "c", "x", "y", "z"}},
			{">10 || >20", []query.Expr{query.GtInt(query.Field("age"), 10), query.GtInt(query.Field("age"), 20)}, []string{}},
			{">8 || non index matcher", []query.Expr{query.GtInt(query.Field("age"), 8), new(simpleExpr)}, []string{}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				m := query.Or(test.exprs...)

				recordIDs, _, err := m.(query.IndexMatcher).MatchIndex(tx, "test")
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

func BenchmarkMatcher(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			records := make([]record.Record, size)
			for i := 0; i < size; i++ {
				records[i] = createRecord(i)
			}

			matcher := query.EqInt(query.Field("age"), size)

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

			tx, cleanup := createIndexes(b, ages, nil)
			defer cleanup()

			matcher := query.EqInt(query.Field("age"), size).(query.IndexMatcher)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				matcher.MatchIndex(tx, "test")
			}
			b.StopTimer()
		})
	}
}

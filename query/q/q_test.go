package q_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/asdine/genji/engine/bolt"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/query/q"
	"github.com/asdine/genji/record"
	bbolt "github.com/etcd-io/bbolt"
	"github.com/stretchr/testify/require"
)

func createRecord(age int) record.Record {
	var fb record.FieldBuffer

	fb.Add(field.NewInt64("age", int64(age)))

	return &fb
}

func TestMatchers(t *testing.T) {
	type matcher interface {
		Match(record.Record) (bool, error)
	}

	r := createRecord(10)
	tests := []struct {
		name    string
		matcher matcher
		match   bool
	}{
		{"eq", q.EqInt(q.Field("age"), 10), true},
		{"gt/10>10", q.GtInt(q.Field("age"), 10), false},
		{"gt/10>11", q.GtInt(q.Field("age"), 11), false},
		{"gt/10>9", q.GtInt(q.Field("age"), 9), true},
		{"gte/10>=10", q.GteInt(q.Field("age"), 10), true},
		{"gte/10>=11", q.GteInt(q.Field("age"), 11), false},
		{"gte/10>=9", q.GteInt(q.Field("age"), 9), true},
		{"lt/10<10", q.LtInt(q.Field("age"), 10), false},
		{"lt/10<11", q.LtInt(q.Field("age"), 11), true},
		{"lt/10<9", q.LtInt(q.Field("age"), 9), false},
		{"lte/10<=10", q.LteInt(q.Field("age"), 10), true},
		{"lte/10<=11", q.LteInt(q.Field("age"), 11), true},
		{"lte/10<=9", q.LteInt(q.Field("age"), 9), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matched, err := test.matcher.Match(r)
			require.NoError(t, err)
			require.Equal(t, test.match, matched)
		})
	}
}

func createIndexMap(t *testing.T, ages []int, teams []string) (map[string]index.Index, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	db, err := bbolt.Open(path.Join(dir, "test.db"), 0600, nil)
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	indexes := make(map[string]index.Index)
	indexes["age"] = createIntIndex(t, tx, ages)
	indexes["team"] = createStrIndex(t, tx, teams)

	return indexes, func() {
		tx.Rollback()
		db.Close()
		os.RemoveAll(dir)
	}
}

func createIntIndex(t *testing.T, tx *bbolt.Tx, ages []int) index.Index {
	b, err := tx.CreateBucket([]byte("age"))
	require.NoError(t, err)

	idx := bolt.NewIndex(b)

	for i, age := range ages {
		err := idx.Set(field.EncodeInt64(int64(age)), field.EncodeInt64(int64(i)))
		require.NoError(t, err)
	}

	return idx
}

func createStrIndex(t *testing.T, tx *bbolt.Tx, teams []string) index.Index {
	b, err := tx.CreateBucket([]byte("team"))
	require.NoError(t, err)

	idx := bolt.NewIndex(b)

	for i, team := range teams {
		err := idx.Set([]byte(team), field.EncodeInt64(int64(i)))
		require.NoError(t, err)
	}

	return idx
}

func TestIndexMatchers(t *testing.T) {
	type indexMatcher interface {
		MatchIndex(im map[string]index.Index) ([][]byte, error)
	}

	im, cleanup := createIndexMap(t, []int{1, 2, 2, 3, 5, 10}, []string{"ACA", "LOSC", "OL", "OM", "OM", "PSG"})
	defer cleanup()

	tests := []struct {
		name    string
		matcher indexMatcher
		rowids  []int64
	}{
		{"eq/int/one", q.EqInt(q.Field("age"), 10), []int64{5}},
		{"eq/int/multiple", q.EqInt(q.Field("age"), 2), []int64{1, 2}},
		{"eq/int/none", q.EqInt(q.Field("age"), 15), nil},
		{"eq/str/one", q.EqStr(q.Field("team"), "PSG"), []int64{5}},
		{"eq/str/multiple", q.EqStr(q.Field("team"), "OM"), []int64{3, 4}},
		{"eq/str/none", q.EqStr(q.Field("team"), "SCB"), nil},
		{"gt/int/>10", q.GtInt(q.Field("age"), 10), nil},
		{"gt/int/>7", q.GtInt(q.Field("age"), 7), []int64{5}},
		{"gt/int/>1", q.GtInt(q.Field("age"), 1), []int64{1, 2, 3, 4, 5}},
		{"gt/int/>-1", q.GtInt(q.Field("age"), -1), []int64{0, 1, 2, 3, 4, 5}},
		{"gt/str/>PSG", q.GtStr(q.Field("team"), "PSG"), nil},
		{"gt/str/>OM", q.GtStr(q.Field("team"), "OM"), []int64{5}},
		{"gt/str/>NICE", q.GtStr(q.Field("team"), "NICE"), []int64{2, 3, 4, 5}},
		{"gt/str/>ACA", q.GtStr(q.Field("team"), "ACA"), []int64{1, 2, 3, 4, 5}},
		{"gt/str/>A", q.GtStr(q.Field("team"), "A"), []int64{0, 1, 2, 3, 4, 5}},
		{"gte/int/>=11", q.GteInt(q.Field("age"), 11), nil},
		{"gte/int/>=7", q.GteInt(q.Field("age"), 7), []int64{5}},
		{"gte/int/>=2", q.GteInt(q.Field("age"), 2), []int64{1, 2, 3, 4, 5}},
		{"gte/int/>=1", q.GteInt(q.Field("age"), 1), []int64{0, 1, 2, 3, 4, 5}},
		{"gte/str/>=PSG", q.GteStr(q.Field("team"), "PSG"), []int64{5}},
		{"gte/str/>=OM", q.GteStr(q.Field("team"), "OM"), []int64{3, 4, 5}},
		{"gte/str/>=NICE", q.GteStr(q.Field("team"), "NICE"), []int64{2, 3, 4, 5}},
		{"gte/str/>=ACA", q.GteStr(q.Field("team"), "ACA"), []int64{0, 1, 2, 3, 4, 5}},
		{"lt/int/<1", q.LtInt(q.Field("age"), 1), nil},
		{"lt/int/<4", q.LtInt(q.Field("age"), 4), []int64{0, 1, 2, 3}},
		{"lt/int/<10", q.LtInt(q.Field("age"), 10), []int64{0, 1, 2, 3, 4}},
		{"lt/int/<11", q.LtInt(q.Field("age"), 11), []int64{0, 1, 2, 3, 4, 5}},
		{"lt/str/<A", q.LtStr(q.Field("team"), "A"), nil},
		{"lt/str/<ACA", q.LtStr(q.Field("team"), "ACA"), nil},
		{"lt/str/<NICE", q.LtStr(q.Field("team"), "NICE"), []int64{0, 1}},
		{"lt/str/<OM", q.LtStr(q.Field("team"), "OM"), []int64{0, 1, 2}},
		{"lt/str/<STRASBOURG", q.LtStr(q.Field("team"), "STRASBOURG"), []int64{0, 1, 2, 3, 4, 5}},
		{"lte/int/<=0", q.LteInt(q.Field("age"), 0), nil},
		{"lte/int/<=2", q.LteInt(q.Field("age"), 2), []int64{0, 1, 2}},
		{"lte/int/<=4", q.LteInt(q.Field("age"), 4), []int64{0, 1, 2, 3}},
		{"lte/int/<=10", q.LteInt(q.Field("age"), 10), []int64{0, 1, 2, 3, 4, 5}},
		{"lte/int/<=11", q.LteInt(q.Field("age"), 11), []int64{0, 1, 2, 3, 4, 5}},
		{"lte/str/<=A", q.LteStr(q.Field("team"), "A"), nil},
		{"lte/str/<=ACA", q.LteStr(q.Field("team"), "ACA"), []int64{0}},
		{"lte/str/<=NICE", q.LteStr(q.Field("team"), "NICE"), []int64{0, 1}},
		{"lte/str/<=OM", q.LteStr(q.Field("team"), "OM"), []int64{0, 1, 2, 3, 4}},
		{"lte/str/<=STRASBOURG", q.LteStr(q.Field("team"), "STRASBOURG"), []int64{0, 1, 2, 3, 4, 5}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rowids, err := test.matcher.MatchIndex(im)
			require.NoError(t, err)
			var ids []int64
			for _, rowid := range rowids {
				id, err := field.DecodeInt64(rowid)
				require.NoError(t, err)
				ids = append(ids, id)
			}
			require.EqualValues(t, test.rowids, ids)
		})
	}
}

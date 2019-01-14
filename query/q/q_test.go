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

func createIndexMap(t *testing.T, ages ...int) (map[string]index.Index, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	db, err := bbolt.Open(path.Join(dir, "test.db"), 0600, nil)
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	b, err := tx.CreateBucket([]byte("age"))
	require.NoError(t, err)

	idx := bolt.NewIndex(b)

	for i, age := range ages {
		err := idx.Set(field.EncodeInt64(int64(age)), field.EncodeInt64(int64(i)))
		require.NoError(t, err)
	}

	indexes := make(map[string]index.Index)
	indexes["age"] = idx

	return indexes, func() {
		tx.Rollback()
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestIndexMatchers(t *testing.T) {
	type indexMatcher interface {
		MatchIndex(im map[string]index.Index) ([][]byte, error)
	}

	im, cleanup := createIndexMap(t, 1, 2, 2, 3, 5, 10)
	defer cleanup()

	tests := []struct {
		name    string
		matcher indexMatcher
		rowids  []int64
	}{
		{"eq/one", q.EqInt(q.Field("age"), 10), []int64{5}},
		{"eq/multiple", q.EqInt(q.Field("age"), 2), []int64{1, 2}},
		{"eq/none", q.EqInt(q.Field("age"), 15), nil},
		{"gt/>10", q.GtInt(q.Field("age"), 10), nil},
		{"gt/>7", q.GtInt(q.Field("age"), 7), []int64{5}},
		{"gt/>1", q.GtInt(q.Field("age"), 1), []int64{1, 2, 3, 4, 5}},
		{"gt/>-1", q.GtInt(q.Field("age"), -1), []int64{0, 1, 2, 3, 4, 5}},
		{"gte/>=11", q.GteInt(q.Field("age"), 11), nil},
		{"gte/>=7", q.GteInt(q.Field("age"), 7), []int64{5}},
		{"gte/>=2", q.GteInt(q.Field("age"), 2), []int64{1, 2, 3, 4, 5}},
		{"gte/>=1", q.GteInt(q.Field("age"), 1), []int64{0, 1, 2, 3, 4, 5}},
		{"lt/<1", q.LtInt(q.Field("age"), 1), nil},
		{"lt/<4", q.LtInt(q.Field("age"), 4), []int64{0, 1, 2, 3}},
		{"lt/<10", q.LtInt(q.Field("age"), 10), []int64{0, 1, 2, 3, 4}},
		{"lt/<11", q.LtInt(q.Field("age"), 11), []int64{0, 1, 2, 3, 4, 5}},
		{"lte/<=0", q.LteInt(q.Field("age"), 0), nil},
		{"lte/<=4", q.LteInt(q.Field("age"), 4), []int64{0, 1, 2, 3}},
		{"lte/<=10", q.LteInt(q.Field("age"), 10), []int64{0, 1, 2, 3, 4, 5}},
		{"lte/<=11", q.LteInt(q.Field("age"), 11), []int64{0, 1, 2, 3, 4, 5}},
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

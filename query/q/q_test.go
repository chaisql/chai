package q_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/query/q"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
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

type idx struct {
	b *bolt.Bucket
}

func (idx *idx) Cursor() index.Cursor {
	return idx.b.Cursor()
}

func createIndexMap(t *testing.T, ages ...int) (map[string]index.Index, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	db, err := bolt.Open(path.Join(dir, "test.db"), 0600, nil)
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	b, err := tx.CreateBucket([]byte("age"))
	require.NoError(t, err)

	for i, age := range ages {
		err := b.Put(field.EncodeInt64(int64(age)), field.EncodeInt64(int64(i)))
		require.NoError(t, err)
	}

	indexes := make(map[string]index.Index)
	indexes["age"] = &idx{b}

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
		// {"gt/10>10", q.GtInt(q.Field("age"), 10), []int{5}},
		// {"gt/10>11", q.GtInt(q.Field("age"), 11), []int{5}},
		// {"gt/10>9", q.GtInt(q.Field("age"), 9), []int{5}},
		// {"gte/10>=10", q.GteInt(q.Field("age"), 10), []int{5}},
		// {"gte/10>=11", q.GteInt(q.Field("age"), 11), []int{5}},
		// {"gte/10>=9", q.GteInt(q.Field("age"), 9), []int{5}},
		// {"lt/10<10", q.LtInt(q.Field("age"), 10), []int{5}},
		// {"lt/10<11", q.LtInt(q.Field("age"), 11), []int{5}},
		// {"lt/10<9", q.LtInt(q.Field("age"), 9), []int{5}},
		// {"lte/10<=10", q.LteInt(q.Field("age"), 10), []int{5}},
		// {"lte/10<=11", q.LteInt(q.Field("age"), 11), []int{5}},
		// {"lte/10<=9", q.LteInt(q.Field("age"), 9), []int{5}},
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

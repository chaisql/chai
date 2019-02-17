package bolt

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/asdine/genji/engine"
	genjitesting "github.com/asdine/genji/engine/testing"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	bolt "github.com/etcd-io/bbolt"
	"github.com/stretchr/testify/require"
)

func tempDir(t require.TestingT) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

func tempDB(t require.TestingT) (*bolt.DB, func()) {
	dir, cleanup := tempDir(t)
	db, err := bolt.Open(path.Join(dir, "test.db"), 0660, nil)
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	return db, func() {
		db.Close()
		cleanup()
	}
}

func tempBucket(t require.TestingT, writable bool) (*bolt.Bucket, func()) {
	db, cleanup := tempDB(t)

	tx, err := db.Begin(writable)
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	b, err := tx.CreateBucketIfNotExists([]byte("test"))
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	return b, func() {
		tx.Rollback()
		cleanup()
	}
}

func TestBoltEngine(t *testing.T) {
	genjitesting.TestSuite(t, func() (engine.Engine, func()) {
		dir, cleanup := tempDir(t)
		ng, err := NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)
		return ng, cleanup
	})
}

func countItems(t require.TestingT, b *bolt.Bucket) int {
	i := 0
	err := b.ForEach(func(k, v []byte) error {
		i++
		return nil
	})
	require.NoError(t, err)

	return i
}

func benchmarkTableInsert(b *testing.B, size int) {
	db, cleanup := tempDB(b)
	defer cleanup()

	var fields []field.Field

	for i := int64(0); i < 10; i++ {
		fields = append(fields, field.NewInt64(fmt.Sprintf("name-%d", i), i))
	}

	rec := record.FieldBuffer(fields)

	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin(true)
		require.NoError(b, err)
		bck, err := tx.CreateBucket([]byte("test"))
		require.NoError(b, err)
		tab := &Table{
			Bucket: bck,
		}

		b.StartTimer()
		for j := 0; j < size; j++ {
			tab.Insert(rec)
		}
		b.StopTimer()

		tx.Rollback()
	}
}

func BenchmarkTableInsert1(b *testing.B) {
	benchmarkTableInsert(b, 1)
}

func BenchmarkTableInsert10(b *testing.B) {
	benchmarkTableInsert(b, 10)
}

func BenchmarkTableInsert100(b *testing.B) {
	benchmarkTableInsert(b, 100)
}

func BenchmarkTableInsert1000(b *testing.B) {
	benchmarkTableInsert(b, 1000)
}

func BenchmarkTableInsert10000(b *testing.B) {
	benchmarkTableInsert(b, 10000)
}

func benchmarkTableScan(b *testing.B, size int) {
	bucket, cleanup := tempBucket(b, true)
	defer cleanup()

	tab := &Table{
		Bucket: bucket,
	}

	var fields []field.Field

	for i := int64(0); i < 10; i++ {
		fields = append(fields, field.NewInt64(fmt.Sprintf("name-%d", i), i))
	}

	rec := record.FieldBuffer(fields)

	for i := 0; i < size; i++ {
		_, err := tab.Insert(rec)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tab.Iterate(func(record.Record) bool {
			return true
		})
	}
	b.StopTimer()
}

func BenchmarkTableScan1(b *testing.B) {
	benchmarkTableScan(b, 1)
}

func BenchmarkTableScan10(b *testing.B) {
	benchmarkTableScan(b, 10)
}

func BenchmarkTableScan100(b *testing.B) {
	benchmarkTableScan(b, 100)
}

func BenchmarkTableScan1000(b *testing.B) {
	benchmarkTableScan(b, 1000)
}

func BenchmarkTableScan10000(b *testing.B) {
	benchmarkTableScan(b, 10000)
}

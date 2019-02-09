package bolt

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin(true)
		require.NoError(b, err)
		bck, err := tx.CreateBucket([]byte("test"))
		require.NoError(b, err)
		tab := &Table{
			bucket: bck,
		}

		b.StartTimer()
		for j := 0; j < size; j++ {
			tab.Insert(record.FieldBuffer([]field.Field{
				field.NewString("name", fmt.Sprintf("name-%d", j)),
				field.NewInt64("age", int64(j)),
			}))
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
		bucket: bucket,
	}

	for i := 0; i < size; i++ {
		_, err := tab.Insert(record.FieldBuffer([]field.Field{
			field.NewString("name", fmt.Sprintf("name-%d", i)),
			field.NewInt64("age", int64(i)),
		}))
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := tab.Cursor()
		for c.Next() {
			c.Record()
		}
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

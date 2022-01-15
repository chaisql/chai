package badgerengine_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/enginetest"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func builder(t testing.TB) func() (engine.Engine, func()) {
	return func() (engine.Engine, func()) {
		dir, cleanup := tempDir(t)
		opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
		opts.Logger = nil

		ng, err := badgerengine.NewEngine(opts)
		assert.NoError(t, err)
		return ng, cleanup
	}
}

func TestBadgerEngine(t *testing.T) {
	enginetest.TestSuite(t, builder(t))
}

func TestTransient(t *testing.T) {
	var ng badgerengine.Engine

	ts, err := ng.NewTransientStore(context.Background())
	assert.NoError(t, err)

	dir := ts.(*badgerengine.TransientStore).DB.Opts().Dir

	err = ts.Put([]byte("foo"), []byte("bar"))
	assert.NoError(t, err)

	it := ts.Iterator(engine.IteratorOptions{})
	defer it.Close()

	it.Seek([]byte("foo"))
	require.True(t, it.Valid())

	err = ts.Drop(context.Background())
	assert.NoError(t, err)

	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err))
}

func BenchmarkBadgerEngineStorePut(b *testing.B) {
	enginetest.BenchmarkStorePut(b, builder(b))
}

func BenchmarkBadgerEngineTransientStorePut(b *testing.B) {
	enginetest.BenchmarkTransientStorePut(b, builder(b))
}

func BenchmarkBadgerEngineTableScan(b *testing.B) {
	enginetest.BenchmarkStoreScan(b, builder(b))
}

func tempDir(t testing.TB) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	assert.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

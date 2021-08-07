package badgerengine_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/enginetest"
	"github.com/genjidb/genji/internal/testutil/assert"
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

func BenchmarkBadgerEngineStorePut(b *testing.B) {
	enginetest.BenchmarkStorePut(b, builder(b))
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

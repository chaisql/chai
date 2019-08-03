package badger_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/badger"
	"github.com/asdine/genji/engine/enginetest"
	bdg "github.com/dgraph-io/badger"
	"github.com/stretchr/testify/require"
)

func builder(t testing.TB) func() (engine.Engine, func()) {
	return func() (engine.Engine, func()) {
		dir, cleanup := tempDir(t)
		ng, err := badger.NewEngine(bdg.DefaultOptions(path.Join(dir, "badger")))
		require.NoError(t, err)
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

func tempDir(t require.TestingT) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

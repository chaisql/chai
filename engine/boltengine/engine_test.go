package boltengine_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/boltengine"
	"github.com/genjidb/genji/engine/enginetest"
	"github.com/stretchr/testify/require"
)

func builder(t testing.TB) func() (engine.Engine, func()) {
	return func() (engine.Engine, func()) {
		dir, cleanup := tempDir(t)
		ng, err := boltengine.NewEngine(filepath.Join(dir, "test.db"), 0o600, nil)
		require.NoError(t, err)
		return ng, cleanup
	}
}

func TestBoltEngine(t *testing.T) {
	enginetest.TestSuite(t, builder(t))
}

func BenchmarkBoltEngineStorePut(b *testing.B) {
	enginetest.BenchmarkStorePut(b, builder(b))
}

func BenchmarkBoltEngineTableScan(b *testing.B) {
	enginetest.BenchmarkStoreScan(b, builder(b))
}

func tempDir(t require.TestingT) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

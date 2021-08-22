package boltengine_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/boltengine"
	"github.com/genjidb/genji/engine/enginetest"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func builder(t testing.TB) func() (engine.Engine, func()) {
	return func() (engine.Engine, func()) {
		dir, cleanup := tempDir(t)
		ng, err := boltengine.NewEngine(filepath.Join(dir, "test.db"), 0o600, nil)
		assert.NoError(t, err)
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

func tempDir(t testing.TB) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	assert.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

func TestTransient(t *testing.T) {
	var ng boltengine.Engine

	tng, err := ng.NewTransientEngine(context.Background())
	assert.NoError(t, err)

	path := tng.(*boltengine.Engine).DB.Path()

	tx, err := tng.Begin(context.Background(), engine.TxOptions{Writable: true})
	assert.NoError(t, err)
	err = tx.Rollback()
	assert.NoError(t, err)

	err = tng.Drop(context.Background())
	assert.NoError(t, err)

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

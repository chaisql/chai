package bolt

import (
	"path"
	"testing"

	"github.com/asdine/genji/table"
	"github.com/asdine/genji/table/tabletest"
	"github.com/stretchr/testify/require"
)

func TestBoltEngineTable(t *testing.T) {
	tabletest.TestSuite(t, func() (table.Table, func()) {
		dir, cleanup := tempDir(t)
		ng, err := NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)

		tx, err := ng.Begin(true)
		require.NoError(t, err)

		tb, err := tx.CreateTable("test")
		require.NoError(t, err)

		return tb, func() {
			tx.Rollback()
			ng.Close()
			cleanup()
		}
	})
}

package bolt

import (
	"path"
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/enginetest"
	"github.com/stretchr/testify/require"
)

func TestBoltEngine(t *testing.T) {
	enginetest.TestSuite(t, func() (engine.Engine, func()) {
		dir, cleanup := tempDir(t)
		ng, err := NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)
		return ng, cleanup
	})
}

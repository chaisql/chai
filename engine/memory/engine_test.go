package memory

import (
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/enginetest"
)

func TestMemoryEngine(t *testing.T) {
	enginetest.TestSuite(t, func() (engine.Engine, func()) {
		ng := NewEngine()
		return ng, func() { ng.Close() }
	})
}

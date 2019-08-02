package memory_test

import (
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/enginetest"
	"github.com/asdine/genji/engine/memory"
)

func builder() (engine.Engine, func()) {
	ng := memory.NewEngine()
	return ng, func() { ng.Close() }
}

func TestMemoryEngine(t *testing.T) {
	enginetest.TestSuite(t, builder)
}

func BenchmarkMemoryEngineStorePut(b *testing.B) {
	enginetest.BenchmarkStorePut(b, builder)
}

func BenchmarkMemoryEngineStoreScan(b *testing.B) {
	enginetest.BenchmarkStoreScan(b, builder)
}

// +build !wasm

package genji

import (
	"context"

	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/database"
)

// New initializes the DB using the given engine.
func New(ctx context.Context, ng engine.Engine) (*DB, error) {
	return newDatabase(ctx, ng, database.Options{Codec: msgpack.NewCodec()})
}

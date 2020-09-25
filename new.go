// +build !wasm

package genji

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
)

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*DB, error) {
	db, err := database.New(ng, database.Options{Codec: msgpack.NewCodec()})
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: db,
	}, nil
}

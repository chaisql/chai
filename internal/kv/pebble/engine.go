package pebble

import (
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	ipebble "github.com/genjidb/genji/internal/database/pebble"
	"github.com/genjidb/genji/internal/kv"
)

func init() {
	kv.RegisterEngine("pebble", &engine{})
}

type engine struct {
}

func (e engine) New(opt kv.Options) (kv.Store, error) {
	var opts pebble.Options

	path, ok := opt.Extra["path"]
	if !ok {
		return nil, errors.New("engine pebble need `path`")
	}

	if path == ":memory:" {
		opts.FS = vfs.NewMem()
		path = ""
	}

	pdb, err := ipebble.Open(path, &opts)
	if err != nil {
		return nil, err
	}

	return NewStore(pdb, opt), nil
}

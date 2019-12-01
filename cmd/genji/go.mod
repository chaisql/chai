module github.com/asdine/genji/cmd/genji

go 1.13

require (
	github.com/asdine/genji v0.3.0
	github.com/asdine/genji/engine/badgerengine v0.3.0
	github.com/c-bata/go-prompt v0.2.3
	github.com/dgraph-io/badger/v2 v2.0.0
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.1
	golang.org/x/tools v0.0.0-20191127201027-ecd32218bd7f
)

replace github.com/asdine/genji v0.3.0 => ../..

replace github.com/asdine/genji/engine/badgerengine v0.3.0 => ../../engine/badgerengine

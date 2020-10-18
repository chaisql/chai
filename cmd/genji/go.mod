module github.com/genjidb/genji/cmd/genji

go 1.15

require (
	github.com/agnivade/levenshtein v1.1.0
	github.com/c-bata/go-prompt v0.2.5
	github.com/dgraph-io/badger/v2 v2.2007.2
	github.com/genjidb/genji v0.9.0
	github.com/genjidb/genji/engine/badgerengine v0.9.0
	github.com/stretchr/testify v1.6.1
	github.com/urfave/cli/v2 v2.2.0
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
)

replace (
	github.com/genjidb/genji v0.9.0 => ../../
	github.com/genjidb/genji/engine/badgerengine v0.9.0 => ../../engine/badgerengine
)

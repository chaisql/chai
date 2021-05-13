module github.com/genjidb/genji/cmd/genji

go 1.16

require (
	github.com/agnivade/levenshtein v1.1.0
	github.com/c-bata/go-prompt v0.2.5
	github.com/dgraph-io/badger/v3 v3.2011.1
	github.com/genjidb/genji v0.13.0
	github.com/genjidb/genji/engine/badgerengine v0.13.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	go.etcd.io/bbolt v1.3.5
	go.uber.org/multierr v1.6.0
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
)

replace (
	github.com/genjidb/genji => ../../
	github.com/genjidb/genji/engine/badgerengine => ../../engine/badgerengine
)

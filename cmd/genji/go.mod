module github.com/genjidb/genji/cmd/genji

go 1.16

require (
	github.com/agnivade/levenshtein v1.1.1
	github.com/c-bata/go-prompt v0.2.6
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/genjidb/genji v0.14.0
	github.com/genjidb/genji/engine/badgerengine v0.14.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	go.etcd.io/bbolt v1.3.6
	go.uber.org/multierr v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

replace (
	github.com/genjidb/genji v0.14.0 => ../../
	github.com/genjidb/genji/engine/badgerengine v0.14.0 => ../../engine/badgerengine
)

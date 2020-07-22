module github.com/genjidb/genji/engine/badgerengine

go 1.14

require (
	github.com/dgraph-io/badger/v2 v2.0.3
	github.com/genjidb/genji v0.7.0
	github.com/stretchr/testify v1.6.1
)

replace github.com/genjidb/genji v0.7.0 => ../../

module github.com/genjidb/genji/engine/badgerengine

go 1.15

require (
	github.com/dgraph-io/badger/v3 v3.2011.1
	github.com/genjidb/genji v0.11.0
	github.com/stretchr/testify v1.7.0
)

replace github.com/genjidb/genji v0.11.0 => ../../

module github.com/genjidb/genji/cmd/genji

go 1.15

require (
	github.com/c-bata/go-prompt v0.2.3
	github.com/dgraph-io/badger/v2 v2.0.3
	github.com/genjidb/genji v0.7.0
	github.com/genjidb/genji/engine/badgerengine v0.7.0
	github.com/mattn/go-runewidth v0.0.7 // indirect
	github.com/mattn/go-tty v0.0.3 // indirect
	github.com/pkg/term v0.0.0-20190109203006-aa71e9d9e942 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/urfave/cli v1.22.1
)

replace github.com/genjidb/genji v0.7.0 => ../../

replace github.com/genjidb/genji/engine/badgerengine v0.7.0 => ../../engine/badgerengine

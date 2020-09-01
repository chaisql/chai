module github.com/genjidb/genji/cmd/genji

go 1.15

require (
	github.com/c-bata/go-prompt v0.2.3
	github.com/dgraph-io/badger/v2 v2.2007.1
	github.com/genjidb/genji v0.8.0
	github.com/genjidb/genji/engine/badgerengine v0.8.0
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/mattn/go-tty v0.0.3 // indirect
	github.com/pkg/term v0.0.0-20200520122047-c3ffed290a03 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/urfave/cli v1.22.4
)

replace github.com/genjidb/genji v0.8.0 => ../../

replace github.com/genjidb/genji/engine/badgerengine v0.8.0 => ../../engine/badgerengine

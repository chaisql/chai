module github.com/asdine/genji/cmd/genji

go 1.13

require (
	github.com/asdine/genji v0.5.0
	github.com/asdine/genji/engine/badgerengine v0.5.0
	github.com/c-bata/go-prompt v0.2.3
	github.com/dgraph-io/badger/v2 v2.0.0
	github.com/mattn/go-runewidth v0.0.7 // indirect
	github.com/mattn/go-tty v0.0.3 // indirect
	github.com/pkg/term v0.0.0-20190109203006-aa71e9d9e942 // indirect
	github.com/urfave/cli v1.22.1
)

replace (
	github.com/asdine/genji v0.5.0 => ../../
	github.com/asdine/genji/engine/badgerengine v0.5.0 => ../../engine/badgerengine
)

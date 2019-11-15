module github.com/asdine/genji/cmd/genji

go 1.13

require (
	github.com/asdine/genji v0.2.2
	github.com/asdine/genji/engine/badgerengine v0.2.2
	github.com/c-bata/go-prompt v0.2.3
	github.com/dgraph-io/badger/v2 v2.0.0
	github.com/mattn/go-runewidth v0.0.6 // indirect
	github.com/pkg/errors v0.8.1
	github.com/pkg/term v0.0.0-20190109203006-aa71e9d9e942 // indirect
	github.com/urfave/cli v1.22.1
	golang.org/x/tools v0.0.0-20191114222411-4191b8cbba09
)

replace github.com/asdine/genji v0.2.2 => ../../

replace github.com/asdine/genji/engine/badgerengine v0.2.2 => ../../engine/badgerengine

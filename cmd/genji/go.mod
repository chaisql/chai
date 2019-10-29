module github.com/asdine/genji/cmd/genji

go 1.13

require (
	github.com/asdine/genji v0.2.1
	github.com/c-bata/go-prompt v0.2.3
	github.com/dgraph-io/badger v1.6.0
	github.com/mattn/go-colorable v0.1.4 // indirect
	github.com/mattn/go-runewidth v0.0.5 // indirect
	github.com/mattn/go-tty v0.0.0-20190424173100-523744f04859 // indirect
	github.com/pkg/errors v0.8.1
	github.com/pkg/term v0.0.0-20190109203006-aa71e9d9e942 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.1
	golang.org/x/tools v0.0.0-20191025023517-2077df36852e
)

replace github.com/asdine/genji v0.2.1 => ../../

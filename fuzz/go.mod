module github.com/genjidb/genji/fuzz

go 1.15

require (
	github.com/dvyukov/go-fuzz v0.0.0-20210429054444-fca39067bc72
	github.com/elazarl/go-bindata-assetfs v1.0.1 // indirect
	github.com/genjidb/genji v0.13.0
	github.com/stephens2424/writerset v1.0.2 // indirect
	golang.org/x/mod v0.4.2 // indirect
	golang.org/x/sys v0.0.0-20210507161434-a76c4d0a0096 // indirect
)

replace github.com/genjidb/genji v0.13.0 => ../

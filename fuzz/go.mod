module github.com/genjidb/genji/fuzz

go 1.15

require (
	github.com/dvyukov/go-fuzz v0.0.0-20201003075337-90825f39c90b
	github.com/elazarl/go-bindata-assetfs v1.0.1 // indirect
	github.com/genjidb/genji v0.9.0
	github.com/stephens2424/writerset v1.0.2 // indirect
	golang.org/x/tools v0.0.0-20201013053347-2db1cd791039 // indirect
)

replace github.com/genjidb/genji v0.9.0 => ../

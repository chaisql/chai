package parser

import "github.com/genjidb/genji/internal/expr"

// Options of the SQL parser.
type Options struct {
	// A table of function packages.
	PackagesTable expr.PackagesTable
}

func defaultOptions() *Options {
	return &Options{
		PackagesTable: expr.DefaultPackagesTable(),
	}
}

package parser

import (
	"github.com/genjidb/genji/internal/expr/functions"
)

// Options of the SQL parser.
type Options struct {
	// A table of function packages.
	PackagesTable functions.PackagesTable
}

func defaultOptions() *Options {
	return &Options{
		PackagesTable: functions.DefaultPackagesTable(),
	}
}

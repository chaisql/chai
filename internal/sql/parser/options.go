package parser

import (
	"github.com/genjidb/genji/internal/expr/functions"
)

// Options of the SQL parser.
type Options struct {
	// A table of function packages.
	Packages functions.Packages
}

func defaultOptions() *Options {
	return &Options{
		Packages: functions.DefaultPackages(),
	}
}

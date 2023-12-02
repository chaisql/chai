package parser

import (
	"github.com/chaisql/chai/internal/expr/functions"
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

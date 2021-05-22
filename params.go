// +build !wasm

package genji

import (
	"database/sql"
	"database/sql/driver"

	"github.com/genjidb/genji/internal/expr"
)

func argsToParams(args []interface{}) []expr.Param {
	nv := make([]expr.Param, len(args))
	for i := range args {
		switch t := args[i].(type) {
		case sql.NamedArg:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case *sql.NamedArg:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case driver.NamedValue:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case *driver.NamedValue:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case *expr.Param:
			nv[i] = *t
		case expr.Param:
			nv[i] = t
		default:
			nv[i].Value = args[i]
		}
	}

	return nv
}

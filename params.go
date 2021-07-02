// +build !wasm

package genji

import (
	"database/sql"
	"database/sql/driver"

	"github.com/genjidb/genji/internal/environment"
)

func argsToParams(args []interface{}) []environment.Param {
	nv := make([]environment.Param, len(args))
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
		case *environment.Param:
			nv[i] = *t
		case environment.Param:
			nv[i] = t
		default:
			nv[i].Value = args[i]
		}
	}

	return nv
}

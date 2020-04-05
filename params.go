// +build !wasm

package genji

import (
	"database/sql"
	"database/sql/driver"

	"github.com/asdine/genji/sql/query"
)

func argsToParams(args []interface{}) []query.Param {
	nv := make([]query.Param, len(args))
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
		case *query.Param:
			nv[i] = *t
		case query.Param:
			nv[i] = t
		default:
			nv[i].Value = args[i]
		}
	}

	return nv
}

package genji

import (
	"github.com/asdine/genji/sql/query"
)

func argsToParams(args []interface{}) []query.Param {
	nv := make([]query.Param, len(args))
	for i := range args {
		switch t := args[i].(type) {
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

package genji

import (
	"github.com/genjidb/genji/internal/environment"
)

func argsToParams(args []interface{}) []environment.Param {
	nv := make([]environment.Param, len(args))
	for i := range args {
		switch t := args[i].(type) {
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

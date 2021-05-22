package genji

import (
	"github.com/genjidb/genji/internal/expr"
)

func argsToParams(args []interface{}) []expr.Param {
	nv := make([]expr.Param, len(args))
	for i := range args {
		switch t := args[i].(type) {
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

package expr_test

import (
	"testing"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/types"
)

func TestPkExpr(t *testing.T) {
	tests := []struct {
		name string
		env  *environment.Environment
		res  types.Value
	}{
		{"empty env", &environment.Environment{}, nullLiteral},
		{"env with doc", envWithDoc, nullLiteral},
		{"env with doc and key", envWithDocAndKey, types.NewIntegerValue(1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testExpr(t, "pk()", test.env, test.res, false)
		})
	}
}

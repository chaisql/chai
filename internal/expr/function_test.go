package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
)

func TestPkExpr(t *testing.T) {
	tests := []struct {
		name string
		env  *environment.Environment
		res  document.Value
	}{
		{"empty env", &environment.Environment{}, nullLitteral},
		{"env with doc", envWithDoc, nullLitteral},
		{"env with doc and key", envWithDocAndKey, document.NewIntegerValue(1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testExpr(t, "pk()", test.env, test.res, false)
		})
	}
}

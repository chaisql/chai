package functions_test

import (
	"path/filepath"
	"testing"

	"github.com/genjidb/genji/internal/testutil"
)

func TestBuiltinFunctions(t *testing.T) {
	testutil.ExprRunner(t, filepath.Join("testdata", "builtin_functions.sql"))
}

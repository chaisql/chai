package expr_test

import (
	"path/filepath"
	"testing"

	"github.com/genjidb/genji/testutil"
)

func TestArithmetic(t *testing.T) {
	testutil.ExprRunner(t, filepath.Join("testdata", "arithmetic.sql"))
}

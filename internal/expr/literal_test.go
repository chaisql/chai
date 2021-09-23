package expr_test

import (
	"path/filepath"
	"testing"

	"github.com/genjidb/genji/internal/testutil"
)

func TestLiteral(t *testing.T) {
	testutil.ExprRunner(t, filepath.Join("testdata", "literal.sql"))
}

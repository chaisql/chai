package expr_test

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/chaisql/chai/internal/testutil"
)

func TestSQLExpr(t *testing.T) {
	filepath.Walk(".", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(info.Name()) != ".sql" {
			return nil
		}

		t.Run(info.Name(), func(t *testing.T) {
			testutil.ExprRunner(t, info.Name())
		})
		return nil
	})
}

package doc_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/cmd/genji/doc"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/stretchr/testify/require"
)

func TestFunctions(t *testing.T) {
	table := functions.DefaultPackagesTable()
	for pkgname, pkg := range table {
		for fname, def := range pkg {
			if pkgname == "" {
				t.Run(fmt.Sprintf("%s has all its arguments mentioned", fname), func(t *testing.T) {
					str, err := doc.DocString(fname)
					require.NoError(t, err)
					for i := 0; i < def.Arity(); i++ {
						require.Contains(t, str, fmt.Sprintf("arg%d", i+1))
					}
				})
			} else {
				t.Run(fmt.Sprintf("%s.%s has all its arguments mentioned", pkgname, fname), func(t *testing.T) {
					str, err := doc.DocString(fmt.Sprintf("%s.%s", pkgname, fname))
					require.NoError(t, err)
					for i := 0; i < def.Arity(); i++ {
						require.Contains(t, str, fmt.Sprintf("arg%d", i+1))
					}
				})
			}
		}
	}
}

func TestTokens(t *testing.T) {
	for _, tok := range scanner.AllKeywords() {
		t.Run(fmt.Sprintf("%s is documented", tok.String()), func(t *testing.T) {
			str, err := doc.DocString(tok.String())
			require.NoError(t, err)
			require.NotEqual(t, "", str)
			if str == "TODO" {
				t.Logf("warning, %s is not yet documented", tok.String())
			} else {
				// if the token is documented, its description should contain its own name.
				require.Contains(t, str, tok.String())
			}
		})
	}
}

func TestDocString(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		str, err := doc.DocString("BY")
		require.NoError(t, err)
		require.NotEmpty(t, str)
		require.NotEqual(t, "TODO", str)
	})

	t.Run("NOK illegal input", func(t *testing.T) {
		_, err := doc.DocString("😀")
		require.Equal(t, doc.ErrInvalid, err)
	})

	t.Run("NOK empty input", func(t *testing.T) {
		_, err := doc.DocString("")
		require.Equal(t, doc.ErrInvalid, err)
	})

	t.Run("NOK no doc found", func(t *testing.T) {
		_, err := doc.DocString("foo.bar")
		require.ErrorIs(t, err, doc.ErrNotFound)
	})
}

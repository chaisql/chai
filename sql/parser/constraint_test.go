package parser

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestConstraintTree(t *testing.T) {
	a := require.New(t)
	tree := constraintTree{}

	// initial check
	err := tree.insert(parsePath(t, `a.b.c`), document.IntegerValue)
	a.NoError(err)
	v := tree.search(parsePath(t, `a`))
	a.NotNil(v)
	a.Equal(document.DocumentValue, v.typ)

	v = tree.search(parsePath(t, `a.b`))
	a.NotNil(v)
	a.Equal(document.DocumentValue, v.typ)

	v = tree.search(parsePath(t, `a.b.c`))
	a.NotNil(v)
	a.Equal(document.IntegerValue, v.typ)

	// Check second insert
	err = tree.insert(parsePath(t, `a.b.d`), document.BoolValue)
	a.NoError(err)
	v = tree.search(parsePath(t, `a.b`))
	a.NotNil(v)
	a.Len(v.sub, 2)

	v = tree.search(parsePath(t, `a.b.d`))
	a.NotNil(v)
	a.Equal(document.BoolValue, v.typ)

	// Check array
	err = tree.insert(parsePath(t, `a.b.e[1]`), document.IntegerValue)
	a.NoError(err)
	v = tree.search(parsePath(t, `a.b.e`))
	a.NotNil(v)
	a.Equal(document.ArrayValue, v.typ)

	// Check another root
	err = tree.insert(parsePath(t, `b.c.d`), document.BoolValue)
	a.NoError(err)
	v = tree.search(parsePath(t, `b.c.d`))
	a.NotNil(v)
	a.Equal(document.BoolValue, v.typ)

	// Check type rewrite error
	err = tree.insert(parsePath(t, `a.b.e.c`), document.IntegerValue)
	a.Error(err)
	err = tree.insert(parsePath(t, `b.c[0]`), document.IntegerValue)
	a.Error(err)
}

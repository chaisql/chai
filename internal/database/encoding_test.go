package database_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestEncoding(t *testing.T) {
	var ti database.TableInfo

	err := ti.AddColumnConstraint(&database.ColumnConstraint{
		Position: 0,
		Column:   "a",
		Type:     types.TypeInteger,
	})
	require.NoError(t, err)

	err = ti.AddColumnConstraint(&database.ColumnConstraint{
		Position: 1,
		Column:   "b",
		Type:     types.TypeText,
	})
	require.NoError(t, err)

	err = ti.AddColumnConstraint(&database.ColumnConstraint{
		Position:  2,
		Column:    "c",
		Type:      types.TypeDouble,
		IsNotNull: true,
	})
	require.NoError(t, err)

	err = ti.AddColumnConstraint(&database.ColumnConstraint{
		Position:     3,
		Column:       "d",
		Type:         types.TypeDouble,
		DefaultValue: expr.Constraint(testutil.ParseExpr(t, `10`)),
	})
	require.NoError(t, err)

	err = ti.AddColumnConstraint(&database.ColumnConstraint{
		Position: 4,
		Column:   "e",
		Type:     types.TypeDouble,
	})
	require.NoError(t, err)

	r := row.NewFromMap(map[string]any{
		"a": int64(1),
		"b": "hello",
		"c": float64(3.14),
		"e": int64(100),
	})

	var buf []byte
	buf, err = ti.EncodeRow(nil, buf, r)
	require.NoError(t, err)

	er := database.NewEncodedRow(&ti.ColumnConstraints, buf)
	require.NoError(t, err)

	want := row.NewFromMap(map[string]any{
		"a": int64(1),
		"b": "hello",
		"c": float64(3.14),
		"d": float64(10),
		"e": float64(100),
	})

	testutil.RequireRowEqual(t, want, er)
}

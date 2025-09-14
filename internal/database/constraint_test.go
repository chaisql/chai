package database_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/expr/functions"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestColumnConstraintsAdd(t *testing.T) {
	tests := []struct {
		name  string
		got   []*database.ColumnConstraint
		add   database.ColumnConstraint
		want  []*database.ColumnConstraint
		fails bool
	}{
		{
			"Same path",
			[]*database.ColumnConstraint{{Column: "a", Type: types.TypeInteger}},
			database.ColumnConstraint{Column: "a", Type: types.TypeInteger},
			nil,
			true,
		},
		{
			"Different path",
			[]*database.ColumnConstraint{{Column: "a", Type: types.TypeInteger}},
			database.ColumnConstraint{Column: "b", Type: types.TypeInteger},
			[]*database.ColumnConstraint{
				{Position: 0, Column: "a", Type: types.TypeInteger},
				{Position: 1, Column: "b", Type: types.TypeInteger},
			},
			false,
		},
		{
			"Default value conversion, typed constraint",
			[]*database.ColumnConstraint{{Column: "a", Type: types.TypeInteger}},
			database.ColumnConstraint{Column: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			[]*database.ColumnConstraint{
				{Position: 0, Column: "a", Type: types.TypeInteger},
				{Position: 1, Column: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, nextval",
			[]*database.ColumnConstraint{{Column: "a", Type: types.TypeInteger}},
			database.ColumnConstraint{Column: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(&functions.NextVal{Expr: testutil.TextValue("seq")})},
			[]*database.ColumnConstraint{
				{Position: 0, Column: "a", Type: types.TypeInteger},
				{Position: 1, Column: "b", Type: types.TypeInteger, DefaultValue: expr.Constraint(&functions.NextVal{Expr: testutil.TextValue("seq")})},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, nextval with bytea",
			[]*database.ColumnConstraint{{Column: "a", Type: types.TypeInteger}},
			database.ColumnConstraint{Column: "b", Type: types.TypeBytea, DefaultValue: expr.Constraint(&functions.NextVal{Expr: testutil.TextValue("seq")})},
			nil,
			true,
		},
		{
			"Default value conversion, typed constraint, incompatible value",
			[]*database.ColumnConstraint{{Column: "a", Type: types.TypeInteger}},
			database.ColumnConstraint{Column: "b", Type: types.TypeDouble, DefaultValue: expr.Constraint(testutil.BoolValue(true))},
			nil,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fcs := database.MustNewColumnConstraints(test.got...)
			err := fcs.Add(&test.add)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, fcs.Ordered)
			}
		})
	}
}

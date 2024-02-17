package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestParserAlterTable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Basic", "ALTER TABLE foo RENAME TO bar", statement.AlterTableRenameStmt{TableName: "foo", NewTableName: "bar"}, false},
		{"With error / missing TABLE keyword", "ALTER foo RENAME TO bar", statement.AlterTableRenameStmt{}, true},
		{"With error / two identifiers for table name", "ALTER TABLE foo baz RENAME TO bar", statement.AlterTableRenameStmt{}, true},
		{"With error / two identifiers for new table name", "ALTER TABLE foo RENAME TO bar baz", statement.AlterTableRenameStmt{}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestParserAlterTableAddColumn(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Without type", "ALTER TABLE foo ADD COLUMN bar", nil, true},
		{"With type", "ALTER TABLE foo ADD COLUMN bar integer", &statement.AlterTableAddColumnStmt{
			TableName: "foo",
			ColumnConstraint: &database.ColumnConstraint{
				Column: "bar",
				Type:   types.TypeInteger,
			},
		}, false},
		{"With not null", "ALTER TABLE foo ADD COLUMN bar TEXT NOT NULL", &statement.AlterTableAddColumnStmt{
			TableName: "foo",
			ColumnConstraint: &database.ColumnConstraint{
				Column:    "bar",
				Type:      types.TypeText,
				IsNotNull: true,
			},
		}, false},
		{"With primary key", "ALTER TABLE foo ADD COLUMN bar TEXT PRIMARY KEY", &statement.AlterTableAddColumnStmt{
			TableName: "foo",
			ColumnConstraint: &database.ColumnConstraint{
				Column: "bar",
				Type:   types.TypeText,
			},
			TableConstraints: database.TableConstraints{
				&database.TableConstraint{
					Columns:    []string{"bar"},
					PrimaryKey: true,
				},
			},
		}, false},
		{"With multiple constraints", "ALTER TABLE foo ADD COLUMN bar integer NOT NULL DEFAULT 0", &statement.AlterTableAddColumnStmt{
			TableName: "foo",
			ColumnConstraint: &database.ColumnConstraint{
				Column:       "bar",
				Type:         types.TypeInteger,
				IsNotNull:    true,
				DefaultValue: expr.Constraint(expr.LiteralValue{Value: types.NewIntegerValue(0)}),
			},
		}, false},
		{"With error / missing COLUMN keyword", "ALTER TABLE foo ADD bar", nil, true},
		{"With error / missing column name", "ALTER TABLE foo ADD COLUMN", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/object"
	"github.com/genjidb/genji/types"
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

func TestParserAlterTableAddField(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Basic", "ALTER TABLE foo ADD FIELD bar", &statement.AlterTableAddFieldStmt{
			TableName: "foo",
			FieldConstraint: &database.FieldConstraint{
				Field: "bar",
				Type:  types.AnyValue,
			},
		}, false},
		{"With type", "ALTER TABLE foo ADD FIELD bar integer", &statement.AlterTableAddFieldStmt{
			TableName: "foo",
			FieldConstraint: &database.FieldConstraint{
				Field: "bar",
				Type:  types.IntegerValue,
			},
		}, false},
		{"With not null", "ALTER TABLE foo ADD FIELD bar NOT NULL", &statement.AlterTableAddFieldStmt{
			TableName: "foo",
			FieldConstraint: &database.FieldConstraint{
				Field:     "bar",
				IsNotNull: true,
			},
		}, false},
		{"With primary key", "ALTER TABLE foo ADD FIELD bar PRIMARY KEY", &statement.AlterTableAddFieldStmt{
			TableName: "foo",
			FieldConstraint: &database.FieldConstraint{
				Field: "bar",
				Type:  types.AnyValue,
			},
			TableConstraints: database.TableConstraints{
				&database.TableConstraint{
					Paths:      object.Paths{object.NewPath("bar")},
					PrimaryKey: true,
				},
			},
		}, false},
		{"With multiple constraints", "ALTER TABLE foo ADD FIELD bar integer NOT NULL DEFAULT 0", &statement.AlterTableAddFieldStmt{
			TableName: "foo",
			FieldConstraint: &database.FieldConstraint{
				Field:        "bar",
				Type:         types.IntegerValue,
				IsNotNull:    true,
				DefaultValue: expr.Constraint(expr.LiteralValue{Value: types.NewIntegerValue(0)}),
			},
		}, false},
		{"With error / missing FIELD keyword", "ALTER TABLE foo ADD bar", nil, true},
		{"With error / missing field name", "ALTER TABLE foo ADD FIELD", nil, true},
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

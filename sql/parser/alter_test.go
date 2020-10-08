package parser

import (
	"context"
	"testing"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/stretchr/testify/require"
)

func TestParserAlterTable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Basic", "ALTER TABLE foo RENAME TO bar", query.AlterStmt{TableName: "foo", NewTableName: "bar"}, false},
		{"With error / missing TABLE keyword", "ALTER foo RENAME TO bar", query.AlterStmt{}, true},
		{"With error / two identifiers for table name", "ALTER TABLE foo baz RENAME TO bar", query.AlterStmt{}, true},
		{"With error / two identifiers for new table name", "ALTER TABLE foo RENAME TO bar baz", query.AlterStmt{}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(context.Background(), test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestParserAlterTableAddColumn(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Basic", "ALTER TABLE foo ADD COLUMN bar", query.AlterTableAddColumn{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path: parsePath(t, "bar"),
			},
		}, false},
		{"With type", "ALTER TABLE foo ADD COLUMN bar integer", query.AlterTableAddColumn{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path: parsePath(t, "bar"),
				Type: document.IntegerValue,
			},
		}, false},
		{"With not null", "ALTER TABLE foo ADD COLUMN bar NOT NULL", query.AlterTableAddColumn{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path:      parsePath(t, "bar"),
				IsNotNull: true,
			},
		}, false},
		{"With primary key", "ALTER TABLE foo ADD COLUMN bar PRIMARY KEY", query.AlterTableAddColumn{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path:         parsePath(t, "bar"),
				IsPrimaryKey: true,
			},
		}, false},
		{"With multiple constraints", "ALTER TABLE foo ADD COLUMN bar integer NOT NULL DEFAULT 0", query.AlterTableAddColumn{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path:         parsePath(t, "bar"),
				Type:         document.IntegerValue,
				IsNotNull:    true,
				DefaultValue: document.NewIntegerValue(0),
			},
		}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(context.Background(), test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

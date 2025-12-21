package analyzer

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/stretchr/testify/require"
)

func testConn(t *testing.T) *database.Connection {
	db, err := database.Open(t.TempDir(), nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	conn, err := db.Connect()
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

func testTx(t *testing.T, conn *database.Connection) *database.Transaction {
	tx, err := conn.BeginTx(&database.TxOptions{
		ReadOnly: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = tx.Rollback()
		require.NoError(t, err)
	})

	return tx
}

func TestParserToQuery(t *testing.T) {
	conn := testConn(t)
	tx := testTx(t, conn)

	c := database.NewCatalog()
	cw := database.NewCatalogWriter(c)
	var cols database.ColumnConstraints
	err := cols.Add(&database.ColumnConstraint{
		Position:  0,
		Column:    "a",
		Type:      types.TypeText,
		IsNotNull: true,
	})
	require.NoError(t, err)
	err = cols.Add(&database.ColumnConstraint{
		Position:  1,
		Column:    "b",
		Type:      types.TypeInteger,
		IsNotNull: false,
	})
	require.NoError(t, err)
	err = cols.Add(&database.ColumnConstraint{
		Position:  2,
		Column:    "c",
		Type:      types.TypeBoolean,
		IsNotNull: false,
	})
	require.NoError(t, err)

	err = cw.CreateTable(tx, "test", &database.TableInfo{
		TableName:         "test",
		ColumnConstraints: cols,
		StoreNamespace:    1,
	})
	require.NoError(t, err)

	ast, err := parser.ParseOne("SELECT a, b FROM test WHERE c > 10")
	require.NoError(t, err)

	a := New(c)
	q, err := a.AnalyzeStatement(ast)
	require.NoError(t, err)

	want := AnalyzedQuery{
		StatementKind: StmtSelect,
		RangeTables: []*RTE{
			{
				Kind:  RTERelation,
				Alias: "test",
				Columns: []ColumnDef{
					{AttNo: 1, Name: "a", Type: types.TypeText},
					{AttNo: 2, Name: "b", Type: types.TypeInteger},
					{AttNo: 3, Name: "c", Type: types.TypeBoolean},
				},
			},
		},
		TargetEntries: []TargetEntry{
			{
				Expr: &ColumnRef{
					BaseExpr: BaseExpr{
						Type:        types.TypeText,
						Nullability: NotNull,
					},
					RTEIndex: 0,
					AttNo:    1,
				},
				Name:    "a",
				Resno:   1,
				Resjunk: false,
			},
			{
				Expr: &ColumnRef{
					BaseExpr: BaseExpr{
						Type:        types.TypeInteger,
						Nullability: Nullable,
					},
					RTEIndex: 0,
					AttNo:    2,
				},
				Name:    "b",
				Resno:   2,
				Resjunk: false,
			},
		},
		Where: &OpCall{
			BaseExpr: BaseExpr{
				Type:        types.TypeBoolean,
				Nullability: Nullable,
			},
		},
	}
	require.Equal(t, &want, q)
}

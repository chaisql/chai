package analyzer

import (
	"testing"

	"github.com/chaisql/chai/internal/catalog"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserToQuery(t *testing.T) {
	sp := catalog.SearchPath{
		catalog.Ident{Raw: "chai_catalog", Folded: "chai_catalog", Quoted: false},
		catalog.Ident{Raw: "public", Folded: "public", Quoted: false},
	}

	c := catalog.New()
	cw := catalog.NewWriter(c)
	var cols []catalog.Attribute
	textTp, ok, err := c.ResolveType(sp, catalog.QualifiedName{
		Name: catalog.NormalizeIdent("text", false),
	})
	require.NoError(t, err)
	require.True(t, ok)
	integerTp, ok, err := c.ResolveType(sp, catalog.QualifiedName{
		Name: catalog.NormalizeIdent("int4", false),
	})
	require.NoError(t, err)
	require.True(t, ok)
	booleanTp, ok, err := c.ResolveType(sp, catalog.QualifiedName{
		Name: catalog.NormalizeIdent("bool", false),
	})
	require.NoError(t, err)
	require.True(t, ok)

	cols = append(cols, catalog.Attribute{
		Name:    "a",
		TypeOID: textTp.OID,
		NotNull: true,
	})
	cols = append(cols, catalog.Attribute{
		Name:    "b",
		TypeOID: integerTp.OID,
		NotNull: false,
	})
	cols = append(cols, catalog.Attribute{
		Name:    "c",
		TypeOID: booleanTp.OID,
		NotNull: false,
	})

	rel, err := cw.CreateTable(catalog.QualifiedName{
		Name: catalog.NormalizeIdent("test", true),
	}, cols, nil)
	require.NoError(t, err)

	ast, err := parser.ParseOne("SELECT a, b FROM test WHERE c > 10")
	require.NoError(t, err)

	a := New(c, sp)
	q, err := a.AnalyzeStatement(ast)
	require.NoError(t, err)

	want := AnalyzedQuery{
		StatementKind: StmtSelect,
		RangeTables: []*RTE{
			{
				Kind:  RTERelation,
				Alias: "test",
				Columns: []catalog.Attribute{
					{RelationOID: rel.OID, AttNum: 1, Name: "a", TypeOID: textTp.OID, NotNull: true},
					{RelationOID: rel.OID, AttNum: 2, Name: "b", TypeOID: integerTp.OID},
					{RelationOID: rel.OID, AttNum: 3, Name: "c", TypeOID: booleanTp.OID},
				},
			},
		},
		TargetEntries: []TargetEntry{
			{
				Expr: &ColumnRef{
					BaseExpr: BaseExpr{
						Type:        textTp.OID,
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
						Type:        integerTp.OID,
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
				Type:        booleanTp.OID,
				Nullability: Nullable,
			},
		},
	}
	require.Equal(t, &want, q)
}

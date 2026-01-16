package analyzer

import (
	"github.com/chaisql/chai/internal/catalog"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type Analyzer struct {
	Catalog    *catalog.Catalog
	SearchPath catalog.SearchPath
}

func New(catalog *catalog.Catalog, searchPath catalog.SearchPath) *Analyzer {
	return &Analyzer{
		Catalog:    catalog,
		SearchPath: searchPath,
	}
}

func (a *Analyzer) AnalyzeStatement(statement statements.Statement[tree.Statement]) (*AnalyzedQuery, error) {
	var q AnalyzedQuery
	var err error

	switch ast := statement.AST.(type) {
	case *tree.Select:
		err = a.analyzeSelect(&q, ast)
	default:
		return nil, errors.Errorf("unsupported statement type: %T", ast)
	}

	return &q, err
}

func (a *Analyzer) analyzeSelect(q *AnalyzedQuery, selectStmt *tree.Select) error {
	q.StatementKind = StmtSelect

	// Analyze core select clause
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok {
		return errors.New("only simple SELECT clauses are supported")
	}

	err := a.analyzeFrom(q, &selectClause.From)
	if err != nil {
		return err
	}

	err = a.analyzeSelectExprs(q, selectClause.Exprs)
	if err != nil {
		return err
	}

	return nil
}

func (a *Analyzer) analyzeFrom(q *AnalyzedQuery, from *tree.From) error {
	// Analyze FROM clause
	if len(from.Tables) != 1 {
		return errors.New("only single table SELECTs are supported")
	}

	table := from.Tables[0]
	rte, err := a.analyzeTableExpr(table)
	if err != nil {
		return err
	}
	q.RangeTables = append(q.RangeTables, rte)
	return nil
}

func (a *Analyzer) analyzeTableExpr(table tree.TableExpr) (*RTE, error) {
	switch t := table.(type) {
	case *tree.TableName:
		var rte RTE
		rte.Kind = RTERelation
		rte.Alias = t.Table()
		tableDef, found, err := a.Catalog.ResolveRelation(a.SearchPath, catalog.QualifiedName{
			Name: catalog.NormalizeIdent(rte.Alias, true),
		})
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.Errorf("table %q not found", rte.Alias)
		}

		rte.Columns, err = a.Catalog.ListAttributes(tableDef.OID, false)
		if err != nil {
			return nil, err
		}

		return &rte, nil
	case *tree.AliasedTableExpr:
		rte, err := a.analyzeTableExpr(t.Expr)
		if err != nil {
			return nil, err
		}
		if t.As.Alias != "" {
			rte.Alias = string(t.As.Alias)
		}
		return rte, nil
	default:
		return nil, errors.Errorf("unsupported table expression type: %T", t)
	}
}

func (a *Analyzer) analyzeSelectExprs(q *AnalyzedQuery, exprs tree.SelectExprs) error {
	for i, expr := range exprs {
		aliasedExpr, ok := expr.(*tree.AliasedExpr)
		if !ok {
			return errors.Errorf("unsupported select expression type: %T", expr)
		}
		
		targetEntry := TargetEntry{
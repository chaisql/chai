package parser

import (
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*planner.Tree, error) {
	var cfg selectConfig
	var err error

	cfg.Distinct, err = p.parseDistinct()
	if err != nil {
		return nil, err
	}

	// Parse path list or query.Wildcard
	cfg.ProjectionExprs, err = p.parseResultFields()
	if err != nil {
		return nil, err
	}

	// Parse "FROM".
	var found bool
	cfg.TableName, found, err = p.parseFrom()
	if err != nil {
		return nil, err
	}
	if !found {
		return cfg.ToTree()
	}

	// Parse condition: "WHERE expr".
	cfg.WhereExpr, err = p.parseCondition()
	if err != nil {
		return nil, err
	}

	// Parse group by: "GROUP BY expr"
	cfg.GroupByExpr, err = p.parseGroupBy()
	if err != nil {
		return nil, err
	}

	// Parse order by: "ORDER BY path [ASC|DESC]?"
	cfg.OrderBy, cfg.OrderByDirection, err = p.parseOrderBy()
	if err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT expr"
	cfg.LimitExpr, err = p.parseLimit()
	if err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET expr"
	cfg.OffsetExpr, err = p.parseOffset()
	if err != nil {
		return nil, err
	}

	return cfg.ToTree()
}

// parseResultFields parses the list of result fields.
func (p *Parser) parseResultFields() ([]planner.ProjectedField, error) {
	// Parse first (required) result path.
	rf, err := p.parseResultField()
	if err != nil {
		return nil, err
	}
	rfields := []planner.ProjectedField{rf}

	// Parse remaining (optional) result fields.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			return rfields, nil
		}

		if rf, err = p.parseResultField(); err != nil {
			return nil, err
		}

		rfields = append(rfields, rf)
	}
}

// parseResultField parses the list of result fields.
func (p *Parser) parseResultField() (planner.ProjectedField, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return planner.Wildcard{}, nil
	}
	p.Unscan()

	e, lit, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// Paths may be quoted, we make sure we name the result path
	// with the unquoted name instead.
	if fs, ok := e.(expr.Path); ok {
		lit = fs.String()
	}

	rf := planner.ProjectedExpr{Expr: e, ExprName: lit}

	// Check if the AS token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.AS {
		rf.ExprName, err = p.parseIdent()
		if err != nil {
			return nil, err
		}

		return rf, nil
	}
	p.Unscan()

	return rf, nil
}

func (p *Parser) parseDistinct() (bool, error) {
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.DISTINCT {
		p.Unscan()
		return false, nil
	}

	return true, nil
}

func (p *Parser) parseFrom() (string, bool, error) {
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		p.Unscan()
		return "", false, nil
	}

	// Parse table name
	ident, err := p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return ident, true, pErr
	}

	return ident, true, nil
}

func (p *Parser) parseGroupBy() (expr.Expr, error) {
	// parse GROUP token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.GROUP {
		p.Unscan()
		return nil, nil
	}

	// parse BY token
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.BY {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"BY"}, pos)
	}

	// parse expr
	e, _, err := p.ParseExpr()
	return e, err
}

func (p *Parser) parseOrderBy() (expr.Path, scanner.Token, error) {
	// parse ORDER token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.ORDER {
		p.Unscan()
		return nil, 0, nil
	}

	// parse BY token
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.BY {
		return nil, 0, newParseError(scanner.Tokstr(tok, lit), []string{"BY"}, pos)
	}

	// parse path
	path, err := p.parsePath()
	if err != nil {
		return nil, 0, err
	}

	// parse optional ASC or DESC
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.ASC || tok == scanner.DESC {
		return expr.Path(path), tok, nil
	}
	p.Unscan()

	return expr.Path(path), 0, nil
}

func (p *Parser) parseLimit() (expr.Expr, error) {
	// parse LIMIT token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LIMIT {
		p.Unscan()
		return nil, nil
	}

	e, _, err := p.ParseExpr()
	return e, err
}

func (p *Parser) parseOffset() (expr.Expr, error) {
	// parse OFFSET token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.OFFSET {
		p.Unscan()
		return nil, nil
	}

	e, _, err := p.ParseExpr()
	return e, err
}

// SelectConfig holds SELECT configuration.
type selectConfig struct {
	TableName        string
	Distinct         bool
	WhereExpr        expr.Expr
	GroupByExpr      expr.Expr
	OrderBy          expr.Path
	OrderByDirection scanner.Token
	OffsetExpr       expr.Expr
	LimitExpr        expr.Expr
	ProjectionExprs  []planner.ProjectedField
}

// ToTree turns the statement into an expression tree.
func (cfg selectConfig) ToTree() (*planner.Tree, error) {
	var n planner.Node

	if cfg.TableName != "" {
		n = planner.NewTableInputNode(cfg.TableName)
	}

	if cfg.WhereExpr != nil {
		n = planner.NewSelectionNode(n, cfg.WhereExpr)
	}

	// when using GROUP BY, only aggregation functions or GroupByExpr can be selected
	if cfg.GroupByExpr != nil {
		// add Group node
		n = planner.NewGroupingNode(n, cfg.GroupByExpr)

		var invalidProjectedField planner.ProjectedField
		var aggregators []document.AggregatorBuilder

		for _, pe := range cfg.ProjectionExprs {
			pre, ok := pe.(planner.ProjectedExpr)
			if !ok {
				invalidProjectedField = pe
				break
			}
			e := pre.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(document.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
				continue
			}

			// check if this is the same expression as the one used in the GROUP BY clause
			if expr.Equal(e, cfg.GroupByExpr) {
				aggregators = append(aggregators, &planner.ProjectedGroupAggregatorBuilder{Expr: pre.Expr})
				continue
			}

			// otherwise it's an error
			invalidProjectedField = pre
			break
		}

		if invalidProjectedField != nil {
			return nil, fmt.Errorf("field %q must appear in the GROUP BY clause or be used in an aggregate function", invalidProjectedField)
		}

		// add Aggregation node
		n = planner.NewAggregationNode(n, aggregators)
	} else {
		// if there is no GROUP BY clause, check if there are any aggregation function
		// and if so add an aggregation node
		var aggregators []document.AggregatorBuilder

		for _, pe := range cfg.ProjectionExprs {
			pre, ok := pe.(planner.ProjectedExpr)
			if !ok {
				continue
			}
			e := pre.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(document.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
			}
		}

		// add Aggregation node
		if len(aggregators) > 0 {
			n = planner.NewAggregationNode(n, aggregators)
		}
	}

	n = planner.NewProjectionNode(n, cfg.ProjectionExprs, cfg.TableName)

	if cfg.Distinct {
		n = planner.NewDedupNode(n, cfg.TableName)
	}

	if cfg.OrderBy != nil {
		n = planner.NewSortNode(n, cfg.OrderBy, cfg.OrderByDirection)
	}

	if cfg.OffsetExpr != nil {
		v, err := cfg.OffsetExpr.Eval(expr.EvalStack{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, fmt.Errorf("offset expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		n = planner.NewOffsetNode(n, int(v.V.(int64)))
	}

	if cfg.LimitExpr != nil {
		v, err := cfg.LimitExpr.Eval(expr.EvalStack{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		n = planner.NewLimitNode(n, int(v.V.(int64)))
	}

	return &planner.Tree{Root: n}, nil
}

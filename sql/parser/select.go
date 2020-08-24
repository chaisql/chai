package parser

import (
	"fmt"
	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*planner.Tree, error) {
	var cfg selectConfig
	var err error

	// Parse field list or query.Wildcard
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

	// Parse condition: "WHERE EXPR".
	cfg.WhereExpr, err = p.parseCondition()
	if err != nil {
		return nil, err
	}

	// Parse order by: "ORDER BY fieldRef [ASC|DESC]?"
	cfg.OrderBy, cfg.OrderByDirection, err = p.parseOrderBy()
	if err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT EXPR"
	cfg.LimitExpr, err = p.parseLimit()
	if err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET EXPR"
	cfg.OffsetExpr, err = p.parseOffset()
	if err != nil {
		return nil, err
	}

	return cfg.ToTree()
}

// parseResultFields parses the list of result fields.
func (p *Parser) parseResultFields() ([]planner.ResultField, error) {
	// Parse first (required) result field.
	rf, err := p.parseResultField()
	if err != nil {
		return nil, err
	}
	rfields := []planner.ResultField{rf}

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
func (p *Parser) parseResultField() (planner.ResultField, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return planner.Wildcard{}, nil
	}
	p.Unscan()

	e, lit, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// FieldSelectors may be quoted, we make sure we name the result field
	// with the unquoted name instead.
	if fs, ok := e.(expr.FieldSelector); ok {
		lit = fs.String()
	}

	rf := planner.ResultFieldExpr{Expr: e, ExprName: lit}

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

	return ident, true,  nil
}

func (p *Parser) parseOrderBy() (expr.FieldSelector, scanner.Token, error) {
	// parse ORDER token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.ORDER {
		p.Unscan()
		return nil, 0, nil
	}

	// parse BY token
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.BY {
		return nil, 0, newParseError(scanner.Tokstr(tok, lit), []string{"BY"}, pos)
	}

	// parse field reference
	ref, err := p.parseFieldRef()
	if err != nil {
		return nil, 0, err
	}

	// parse optional ASC or DESC
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.ASC || tok == scanner.DESC {
		return expr.FieldSelector(ref), tok, nil
	}
	p.Unscan()

	return expr.FieldSelector(ref), 0, nil
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
	WhereExpr        expr.Expr
	OrderBy          expr.FieldSelector
	OrderByDirection scanner.Token
	OffsetExpr       expr.Expr
	LimitExpr        expr.Expr
	ProjectionExprs  []planner.ResultField
}

// ToTree turns the statement into an expression tree.
func (cfg selectConfig) ToTree() (*planner.Tree, error) {
	if cfg.TableName == "" {
		return planner.NewTree(planner.NewProjectionNode(nil, cfg.ProjectionExprs, "")), nil
	}

	t := planner.NewTableInputNode(cfg.TableName)

	if cfg.WhereExpr != nil {
		t = planner.NewSelectionNode(t, cfg.WhereExpr)
	}

	if cfg.OrderBy != nil {
		t = planner.NewSortNode(t, cfg.OrderBy, cfg.OrderByDirection)
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

		t = planner.NewOffsetNode(t, int(v.V.(int64)))
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

		t = planner.NewLimitNode(t, int(v.V.(int64)))
	}

	t = planner.NewProjectionNode(t, cfg.ProjectionExprs, cfg.TableName)

	return &planner.Tree{Root: t}, nil
}

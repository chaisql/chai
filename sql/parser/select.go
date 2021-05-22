package parser

import (
	"errors"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/sql/scanner"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*query.StreamStmt, error) {
	var cfg selectConfig
	var err error

	cfg.Distinct, err = p.parseDistinct()
	if err != nil {
		return nil, err
	}

	// Parse path list or query.Wildcard
	cfg.ProjectionExprs, err = p.parseProjectedExprs()
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
		return cfg.ToStream()
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

	return cfg.ToStream()
}

// parseProjectedExprs parses the list of projected fields.
func (p *Parser) parseProjectedExprs() ([]expr.Expr, error) {
	// Parse first (required) result path.
	pe, err := p.parseProjectedExpr()
	if err != nil {
		return nil, err
	}
	pexprs := []expr.Expr{pe}

	// Parse remaining (optional) result fields.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			return pexprs, nil
		}

		if pe, err = p.parseProjectedExpr(); err != nil {
			return nil, err
		}

		pexprs = append(pexprs, pe)
	}
}

// parseProjectedExpr parses one projected expression.
func (p *Parser) parseProjectedExpr() (expr.Expr, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return expr.Wildcard{}, nil
	}
	p.Unscan()

	e, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	rf := &expr.NamedExpr{Expr: e, ExprName: e.String()}

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
	ok, err := p.parseOptional(scanner.GROUP, scanner.BY)
	if err != nil || !ok {
		return nil, err
	}

	// parse expr
	e, err := p.ParseExpr()
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
	ProjectionExprs  []expr.Expr
}

func (cfg selectConfig) ToStream() (*query.StreamStmt, error) {
	var s *stream.Stream

	if cfg.TableName != "" {
		s = stream.New(stream.SeqScan(cfg.TableName))
	}

	if cfg.WhereExpr != nil {
		s = s.Pipe(stream.Filter(cfg.WhereExpr))
	}

	// when using GROUP BY, only aggregation functions or GroupByExpr can be selected
	if cfg.GroupByExpr != nil {
		// add Group node
		s = s.Pipe(stream.GroupBy(cfg.GroupByExpr))

		var invalidProjectedField expr.Expr
		var aggregators []expr.AggregatorBuilder

		for _, pe := range cfg.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				invalidProjectedField = pe
				break
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
				continue
			}

			// check if this is the same expression as the one used in the GROUP BY clause
			if expr.Equal(e, cfg.GroupByExpr) {
				continue
			}

			// otherwise it's an error
			invalidProjectedField = ne
			break
		}

		if invalidProjectedField != nil {
			return nil, stringutil.Errorf("field %q must appear in the GROUP BY clause or be used in an aggregate function", invalidProjectedField)
		}

		// add Aggregation node
		s = s.Pipe(stream.HashAggregate(aggregators...))
	} else {
		// if there is no GROUP BY clause, check if there are any aggregation function
		// and if so add an aggregation node
		var aggregators []expr.AggregatorBuilder

		for _, pe := range cfg.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				continue
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
			}
		}

		// add Aggregation node
		if len(aggregators) > 0 {
			s = s.Pipe(stream.HashAggregate(aggregators...))
		}
	}

	// If there is no FROM clause ensure there is no wildcard or path
	if cfg.TableName == "" {
		var err error

		for _, e := range cfg.ProjectionExprs {
			expr.Walk(e, func(e expr.Expr) bool {
				switch e.(type) {
				case expr.Path, expr.Wildcard:
					err = errors.New("no tables specified")
					return false
				default:
					return true
				}
			})
			if err != nil {
				return nil, err
			}
		}
	}

	s = s.Pipe(stream.Project(cfg.ProjectionExprs...))

	if cfg.Distinct {
		s = s.Pipe(stream.Distinct())
	}

	if cfg.OrderBy != nil {
		if cfg.OrderByDirection == scanner.DESC {
			s = s.Pipe(stream.SortReverse(cfg.OrderBy))
		} else {
			s = s.Pipe(stream.Sort(cfg.OrderBy))
		}
	}

	if cfg.OffsetExpr != nil {
		v, err := cfg.OffsetExpr.Eval(&expr.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, stringutil.Errorf("offset expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Skip(v.V.(int64)))
	}

	if cfg.LimitExpr != nil {
		v, err := cfg.LimitExpr.Eval(&expr.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, stringutil.Errorf("limit expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Take(v.V.(int64)))
	}

	return &query.StreamStmt{
		Stream:   s,
		ReadOnly: true,
	}, nil
}

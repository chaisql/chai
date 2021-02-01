package parser

import (
	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/genjidb/genji/stream"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (*planner.Statement, error) {
	var cfg deleteConfig
	var err error

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	cfg.TableName, err = p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	// Parse condition: "WHERE EXPR".
	cfg.WhereExpr, err = p.parseCondition()
	if err != nil {
		return nil, err
	}

	return cfg.ToStream(), nil
}

// DeleteConfig holds DELETE configuration.
type deleteConfig struct {
	TableName string
	WhereExpr expr.Expr
}

func (cfg deleteConfig) ToStream() *planner.Statement {
	s := stream.New(stream.SeqScan(cfg.TableName))

	if cfg.WhereExpr != nil {
		s = s.Pipe(stream.Filter(cfg.WhereExpr))
	}

	s = s.Pipe(stream.TableDelete(cfg.TableName))

	return &planner.Statement{
		Stream:   s,
		ReadOnly: false,
	}
}

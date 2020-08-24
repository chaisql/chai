package parser

import (
	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (*planner.Tree, error) {
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

	return cfg.ToTree(), nil
}

// DeleteConfig holds DELETE configuration.
type deleteConfig struct {
	TableName string
	WhereExpr expr.Expr
}

// ToTree turns the statement into an expression tree.
func (cfg deleteConfig) ToTree() *planner.Tree {
	t := planner.NewTableInputNode(cfg.TableName)

	if cfg.WhereExpr != nil {
		t = planner.NewSelectionNode(t, cfg.WhereExpr)
	}

	t = planner.NewDeletionNode(t, cfg.TableName)

	return &planner.Tree{Root: t}
}

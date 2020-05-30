package parser

import (
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/genjidb/genji/sql/tree"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (*tree.Tree, error) {
	var cfg deleteConfig
	var err error

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	cfg.TableName, err = p.parseIdent()
	if err != nil {
		return nil, err
	}

	// Parse condition: "WHERE EXPR".
	cfg.WhereExpr, err = p.parseCondition()
	if err != nil {
		return nil, err
	}

	return cfg.ToTree(), nil
}

// deleteConfig holds DELETE configuration.
type deleteConfig struct {
	TableName string
	WhereExpr expr.Expr
}

// ToTree turns the statement into an expression tree.
func (cfg deleteConfig) ToTree() *tree.Tree {
	t := tree.NewInputNode("table", cfg.TableName)

	if cfg.WhereExpr != nil {
		t = tree.NewSelectionNode(t, cfg.WhereExpr)
	}

	t = tree.NewDeletionNode(t, cfg.TableName)

	return &tree.Tree{Root: t}
}

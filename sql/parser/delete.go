package parser

import (
	"github.com/genjidb/genji/sql/scanner"
	"github.com/genjidb/genji/sql/tree"
)

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (*tree.Tree, error) {
	var cfg tree.DeleteConfig
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

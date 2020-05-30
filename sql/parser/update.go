package parser

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/genjidb/genji/sql/tree"
)

// parseUpdateStatement parses a update string and returns a Statement AST object.
// This function assumes the UPDATE token has already been consumed.
func (p *Parser) parseUpdateStatement() (*tree.Tree, error) {
	var cfg updateConfig
	var err error

	// Parse table name
	cfg.TableName, err = p.parseIdent()
	if err != nil {
		return nil, err
	}

	// Parse clause: SET or UNSET.
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.SET:
		cfg.SetPairs, err = p.parseSetClause()
	case scanner.UNSET:
		cfg.UnsetFields, err = p.parseUnsetClause()
	default:
		err = newParseError(scanner.Tokstr(tok, lit), []string{"SET", "UNSET"}, pos)
	}
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

// parseSetClause parses the "SET" clause of the query.
func (p *Parser) parseSetClause() (map[string]expr.Expr, error) {
	pairs := make(map[string]expr.Expr)

	firstPair := true
	for {
		if !firstPair {
			// Scan for a comma.
			tok, _, _ := p.ScanIgnoreWhitespace()
			if tok != scanner.COMMA {
				p.Unscan()
				break
			}
		}

		// Scan the identifier for the field name.
		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok != scanner.IDENT {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}

		// Scan the eq sign
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EQ {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"="}, pos)
		}

		// Scan the expr for the value.
		expr, _, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		pairs[lit] = expr

		firstPair = false
	}

	return pairs, nil
}

func (p *Parser) parseUnsetClause() ([]string, error) {
	var fields []string

	firstField := true
	for {
		if !firstField {
			// Scan for a comma.
			tok, _, _ := p.ScanIgnoreWhitespace()
			if tok != scanner.COMMA {
				p.Unscan()
				break
			}
		}

		// Scan the identifier for the field to unset.
		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok != scanner.IDENT {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}
		fields = append(fields, lit)

		firstField = false
	}
	return fields, nil
}

// updateConfig holds UPDATE configuration.
type updateConfig struct {
	TableName string

	// SetPairs is used along with the Set clause. It holds
	// each field with its corresponding value that
	// should be set in the document.
	SetPairs map[string]expr.Expr

	// UnsetFields is used along with the Unset clause. It holds
	// each field that should be unset from the document.
	UnsetFields []string

	WhereExpr expr.Expr
}

// ToTree turns the statement into an expression tree.
func (cfg updateConfig) ToTree() *tree.Tree {
	t := tree.NewInputNode("table", cfg.TableName)

	if cfg.WhereExpr != nil {
		t = tree.NewSelectionNode(t, cfg.WhereExpr)
	}

	if cfg.SetPairs != nil {
		for name, expr := range cfg.SetPairs {
			t = tree.NewSetNode(t, document.NewValuePath(name), expr)
		}
	} else if cfg.UnsetFields != nil {
		for _, name := range cfg.UnsetFields {
			t = tree.NewUnsetNode(t, document.NewValuePath(name))
		}
	}

	t = tree.NewReplacementNode(t, cfg.TableName)

	return &tree.Tree{Root: t}
}

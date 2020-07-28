package parser

import (
	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"strings"
)

// parseUpdateStatement parses a update string and returns a Statement AST object.
// This function assumes the UPDATE token has already been consumed.
func (p *Parser) parseUpdateStatement() (*planner.Tree, error) {
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
func (p *Parser) parseSetClause() ([]updateSetPair, error) {
	var pairs []updateSetPair

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

		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok != scanner.IDENT {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}

		p.Unscan()
		ref, err := p.parseFieldRef()
		if err != nil {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}

		lit = strings.Join(ref, ".")
		// Scan the eq sign
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EQ {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"="}, pos)
		}

		// Scan the expr for the value.
		expr, _, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		pairs = append(pairs, updateSetPair{lit, expr})
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

// UpdateConfig holds UPDATE configuration.
type updateConfig struct {
	TableName string

	// SetPairs is used along with the Set clause. It holds
	// each field with its corresponding value that
	// should be set in the document.
	SetPairs []updateSetPair

	// UnsetFields is used along with the Unset clause. It holds
	// each field that should be unset from the document.
	UnsetFields []string

	WhereExpr expr.Expr
}

type updateSetPair struct {
	field string
	e     expr.Expr
}

// ToTree turns the statement into an expression tree.
func (cfg updateConfig) ToTree() *planner.Tree {
	t := planner.NewTableInputNode(cfg.TableName)

	if cfg.WhereExpr != nil {
		t = planner.NewSelectionNode(t, cfg.WhereExpr)
	}

	if cfg.SetPairs != nil {
		for _, pair := range cfg.SetPairs {
			t = planner.NewSetNode(t, pair.field, pair.e)
		}
	} else if cfg.UnsetFields != nil {
		for _, name := range cfg.UnsetFields {
			t = planner.NewUnsetNode(t, name)
		}
	}

	t = planner.NewReplacementNode(t, cfg.TableName)

	return &planner.Tree{Root: t}
}

package tree

import (
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
)

// SelectConfig holds SELECT configuration.
type SelectConfig struct {
	TableName        string
	WhereExpr        expr.Expr
	OrderBy          expr.FieldSelector
	OrderByDirection scanner.Token
	OffsetExpr       expr.Expr
	LimitExpr        expr.Expr
	ProjectionExprs  []query.ResultField
}

// ToTree turns the statement into an expression tree.
func (cfg SelectConfig) ToTree() *Tree {
	if cfg.TableName == "" {
		return New(NewProjectionNode(nil, cfg.ProjectionExprs))
	}

	t := NewTableInputNode(cfg.TableName)

	if cfg.WhereExpr != nil {
		t = NewSelectionNode(t, cfg.WhereExpr)
	}

	if cfg.OrderBy != nil {
		t = NewSortNode(t, cfg.OrderBy, cfg.OrderByDirection)
	}

	if cfg.OffsetExpr != nil {
		t = NewOffsetNode(t, cfg.OffsetExpr)
	}

	if cfg.LimitExpr != nil {
		t = NewLimitNode(t, cfg.LimitExpr)
	}

	t = NewProjectionNode(t, cfg.ProjectionExprs)

	return &Tree{Root: t}
}

// UpdateConfig holds UPDATE configuration.
type UpdateConfig struct {
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
func (cfg UpdateConfig) ToTree() *Tree {
	t := NewTableInputNode(cfg.TableName)

	if cfg.WhereExpr != nil {
		t = NewSelectionNode(t, cfg.WhereExpr)
	}

	if cfg.SetPairs != nil {
		for name, expr := range cfg.SetPairs {
			t = NewSetNode(t, name, expr)
		}
	} else if cfg.UnsetFields != nil {
		for _, name := range cfg.UnsetFields {
			t = NewUnsetNode(t, name)
		}
	}

	t = NewReplacementNode(t, cfg.TableName)

	return &Tree{Root: t}
}

// DeleteConfig holds DELETE configuration.
type DeleteConfig struct {
	TableName string
	WhereExpr expr.Expr
}

// ToTree turns the statement into an expression tree.
func (cfg DeleteConfig) ToTree() *Tree {
	t := NewTableInputNode(cfg.TableName)

	if cfg.WhereExpr != nil {
		t = NewSelectionNode(t, cfg.WhereExpr)
	}

	t = NewDeletionNode(t, cfg.TableName)

	return &Tree{Root: t}
}

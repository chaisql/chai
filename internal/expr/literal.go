package expr

import (
	"strings"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/types"
)

// A LiteralValue represents a literal value of any type defined by the value package.
type LiteralValue struct {
	Value types.Value
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (v LiteralValue) IsEqual(other Expr) bool {
	o, ok := other.(LiteralValue)
	if !ok {
		return false
	}
	ok, err := v.Value.EQ(o.Value)
	return ok && err == nil
}

// String implements the fmt.Stringer interface.
func (v LiteralValue) String() string {
	return v.Value.String()
}

// Eval returns l. It implements the Expr interface.
func (v LiteralValue) Eval(*environment.Environment) (types.Value, error) {
	return types.Value(v.Value), nil
}

// LiteralExprList is a list of expressions.
type LiteralExprList []Expr

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (l LiteralExprList) IsEqual(o LiteralExprList) bool {
	if len(l) != len(o) {
		return false
	}

	for i := range l {
		if !Equal(l[i], o[i]) {
			return false
		}
	}

	return true
}

// String implements the fmt.Stringer interface.
func (l LiteralExprList) String() string {
	var b strings.Builder

	b.WriteRune('(')
	for i, e := range l {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(e.String())
	}
	b.WriteRune(')')

	return b.String()
}

func (l LiteralExprList) Eval(env *environment.Environment) (types.Value, error) {
	panic("not implemented")
}

// Eval evaluates all the expressions and returns a literalValueList. It implements the Expr interface.
func (l LiteralExprList) EvalAll(env *environment.Environment) ([]types.Value, error) {
	var err error
	if len(l) == 0 {
		return nil, nil
	}
	values := make([]types.Value, len(l))
	for i, e := range l {
		values[i], err = e.Eval(env)
		if err != nil {
			return nil, err
		}
	}

	return values, nil
}

type Row struct {
	Columns []string
	Exprs   []Expr
}

func (r *Row) Eval(env *environment.Environment) (row.Row, error) {
	var cb row.ColumnBuffer

	for i, e := range r.Exprs {
		v, err := e.Eval(env)
		if err != nil {
			return nil, err
		}

		err = cb.Set(r.Columns[i], v)
		if err != nil {
			return nil, err
		}
	}

	return &cb, nil
}

func (r *Row) String() string {
	return LiteralExprList(r.Exprs).String()
}

package expr

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/stringutil"
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
	ok, err := types.IsEqual(v.Value, o.Value)
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
func (l LiteralExprList) IsEqual(other Expr) bool {
	o, ok := other.(LiteralExprList)
	if !ok {
		return false
	}
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

	b.WriteRune('[')
	for i, e := range l {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(e.String())
	}
	b.WriteRune(']')

	return b.String()
}

// Eval evaluates all the expressions and returns a literalValueList. It implements the Expr interface.
func (l LiteralExprList) Eval(env *environment.Environment) (types.Value, error) {
	var err error
	if len(l) == 0 {
		return types.NewArrayValue(object.NewValueBuffer()), nil
	}
	values := make([]types.Value, len(l))
	for i, e := range l {
		values[i], err = e.Eval(env)
		if err != nil {
			return NullLiteral, err
		}
	}

	return types.NewArrayValue(object.NewValueBuffer(values...)), nil
}

// KVPair associates an identifier with an expression.
type KVPair struct {
	K string
	V Expr
}

// String implements the fmt.Stringer interface.
func (p KVPair) String() string {
	if stringutil.NeedsQuotes(p.K) {
		return fmt.Sprintf("%q: %v", p.K, p.V)
	}
	return fmt.Sprintf("%s: %v", p.K, p.V)
}

// KVPairs is a list of KVPair.
type KVPairs struct {
	Pairs          []KVPair
	SelfReferenced bool
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (kvp *KVPairs) IsEqual(other Expr) bool {
	o, ok := other.(*KVPairs)
	if !ok {
		return false
	}
	if kvp.SelfReferenced != o.SelfReferenced {
		return false
	}

	if len(kvp.Pairs) != len(o.Pairs) {
		return false
	}

	for i := range kvp.Pairs {
		if kvp.Pairs[i].K != o.Pairs[i].K {
			return false
		}
		if !Equal(kvp.Pairs[i].V, o.Pairs[i].V) {
			return false
		}
	}

	return true
}

// Eval turns a list of KVPairs into an object.
func (kvp *KVPairs) Eval(env *environment.Environment) (types.Value, error) {
	var fb object.FieldBuffer
	if kvp.SelfReferenced {
		if _, ok := env.GetRow(); !ok {
			env.SetRowFromObject(&fb)
		}
	}

	for _, kv := range kvp.Pairs {
		v, err := kv.V.Eval(env)
		if err != nil {
			return nil, err
		}

		fb.Add(kv.K, v)
	}

	return types.NewObjectValue(&fb), nil
}

// String implements the fmt.Stringer interface.
func (kvp *KVPairs) String() string {
	var b strings.Builder

	b.WriteRune('{')
	for i, p := range kvp.Pairs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%s", p))
	}
	b.WriteRune('}')

	return b.String()
}

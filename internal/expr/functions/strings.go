package functions

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/types"
)

// Lower is the LOWER function
// It returns the lower-case version of a string
type Lower struct {
	Expr expr.Expr
}

func (s *Lower) Clone() expr.Expr {
	return &Lower{
		Expr: expr.Clone(s.Expr),
	}
}

func (s *Lower) Eval(env *environment.Environment) (types.Value, error) {
	val, err := s.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	if val.Type() != types.TypeText {
		return types.NewNullValue(), nil
	}

	lowerCaseString := strings.ToLower(types.AsString(val))

	return types.NewTextValue(lowerCaseString), nil
}

func (s *Lower) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Lower)
	if !ok {
		return false
	}

	return expr.Equal(s.Expr, o.Expr)
}

func (s *Lower) Params() []expr.Expr { return []expr.Expr{s.Expr} }

func (s *Lower) String() string {
	return fmt.Sprintf("LOWER(%v)", s.Expr)
}

// Upper is the UPPER function
// It returns the upper-case version of a string
type Upper struct {
	Expr expr.Expr
}

func (s *Upper) Clone() expr.Expr {
	return &Upper{
		Expr: expr.Clone(s.Expr),
	}
}

func (s *Upper) Eval(env *environment.Environment) (types.Value, error) {
	val, err := s.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	if val.Type() != types.TypeText {
		return types.NewNullValue(), nil
	}

	upperCaseString := strings.ToUpper(types.AsString(val))

	return types.NewTextValue(upperCaseString), nil
}

func (s *Upper) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Upper)
	if !ok {
		return false
	}

	return expr.Equal(s.Expr, o.Expr)
}

func (s *Upper) Params() []expr.Expr { return []expr.Expr{s.Expr} }

func (s *Upper) String() string {
	return fmt.Sprintf("UPPER(%v)", s.Expr)
}

// TRIM removes leading and trailing characters from a string based on the given input.
// LTRIM removes leading characters
// RTRIM removes trailing characters
// By default remove space " "
type Trim struct {
	Expr     []expr.Expr
	TrimFunc TrimFunc
	Name     string
}

type TrimFunc func(string, string) string

func (s *Trim) Clone() expr.Expr {
	exprs := make([]expr.Expr, len(s.Expr))
	for i := range s.Expr {
		exprs[i] = expr.Clone(s.Expr[i])
	}
	return &Trim{
		Expr:     exprs,
		TrimFunc: s.TrimFunc,
		Name:     s.Name,
	}
}

func (s *Trim) Eval(env *environment.Environment) (types.Value, error) {
	if len(s.Expr) > 2 {
		return nil, fmt.Errorf("misuse of string function %v()", s.Name)
	}

	input, err := s.Expr[0].Eval(env)
	if err != nil {
		return nil, err
	}

	if input.Type() != types.TypeText {
		return types.NewNullValue(), nil
	}

	var cutset = " "

	if len(s.Expr) == 2 {
		remove, err := s.Expr[1].Eval(env)
		if err != nil {
			return nil, err
		}
		if remove.Type() != types.TypeText {
			return types.NewNullValue(), nil
		}
		cutset = types.AsString(remove)
	}

	trimmed := s.TrimFunc(types.AsString(input), cutset)

	return types.NewTextValue(trimmed), nil
}

func (s *Trim) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}
	o, ok := other.(*Trim)
	if !ok {
		return false
	}
	if len(s.Expr) != len(o.Expr) {
		return false
	}

	for i := range s.Expr {
		if !expr.Equal(s.Expr[i], o.Expr[i]) {
			return false
		}
	}

	return true
}

func (s *Trim) Params() []expr.Expr {
	return s.Expr
}

func (s *Trim) String() string {
	if len(s.Expr) == 1 {
		return fmt.Sprintf("%v(%v)", s.Name, s.Expr[0])
	}
	return fmt.Sprintf("%v(%v, %v)", s.Name, s.Expr[0], s.Expr[1])
}

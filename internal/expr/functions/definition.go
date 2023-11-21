package functions

import (
	"fmt"
	"strings"

	"github.com/genjidb/genji/internal/expr"
)

// UNLIMITED represents an unlimited number of arguments.
const UNLIMITED = -1

// A Definition transforms a list of expressions into a Function.
type Definition interface {
	Name() string
	String() string
	Function(...expr.Expr) (expr.Function, error)
	Arity() int
}

// Definitions table holds a map of definition, indexed by their names.
type Definitions map[string]Definition

// Packages represent a table of SQL functions grouped by their packages
type Packages map[string]Definitions

func DefaultPackages() Packages {
	return Packages{
		"":        BuiltinDefinitions(),
		"math":    MathFunctions(),
		"strings": StringsDefinitions(),
	}
}

// GetFunc return a function definition by its package and name.
func (t Packages) GetFunc(pkg string, fname string) (Definition, error) {
	fs, ok := t[pkg]
	if !ok {
		return nil, fmt.Errorf("no such package: %q", fname)
	}
	def, ok := fs[strings.ToLower(fname)]
	if !ok {
		if pkg == "" {
			return nil, fmt.Errorf("no such function: %q", fname)
		}
		return nil, fmt.Errorf("no such function: %q.%q", pkg, fname)
	}
	return def, nil
}

// A definition is the most basic version of a function definition.
type definition struct {
	name          string
	arity         int
	constructorFn func(...expr.Expr) (expr.Function, error)
}

func (fd *definition) Name() string {
	return fd.name
}

func (fd *definition) Function(args ...expr.Expr) (expr.Function, error) {
	if fd.arity != UNLIMITED && (len(args) != fd.arity) {
		return nil, fmt.Errorf("%s() takes %d argument(s), not %d", fd.name, fd.arity, len(args))
	}
	return fd.constructorFn(args...)
}

func (fd *definition) String() string {
	arity := fd.arity
	if arity < 0 {
		arity = 0
	}
	args := make([]string, 0, arity)
	for i := 0; i < arity; i++ {
		args = append(args, fmt.Sprintf("arg%d", i+1))
	}
	return fmt.Sprintf("%s(%s)", fd.name, strings.Join(args, ", "))
}

func (fd *definition) Arity() int {
	return fd.arity
}

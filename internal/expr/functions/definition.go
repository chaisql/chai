package functions

import (
	"strings"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

// A Definition transforms a list of expressions into a Function.
type Definition interface {
	Name() string
	String() string
	Function(...expr.Expr) (expr.Function, error)
	Arity() int
}

// A Definitions table holds a map of definitions, indexed by their names.
type DefinitionsTable map[string]Definition

// PackagesTable represents a table of SQL functions grouped by their packages
type PackagesTable map[string]DefinitionsTable

func DefaultPackagesTable() PackagesTable {
	return PackagesTable{
		"":     BuiltinDefinitions(),
		"math": MathFunctions(),
	}
}

// GetFunc return a function definition by its package and name.
func (t PackagesTable) GetFunc(pkg string, fname string) (Definition, error) {
	fs, ok := t[pkg]
	if !ok {
		return nil, stringutil.Errorf("no such package: %q", fname)
	}
	def, ok := fs[strings.ToLower(fname)]
	if !ok {
		if pkg == "" {
			return nil, stringutil.Errorf("no such function: %q", fname)
		}
		return nil, stringutil.Errorf("no such function: %q.%q", pkg, fname)
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
	if len(args) != fd.arity {
		return nil, stringutil.Errorf("%s() takes %d argument(s), not %d", fd.name, fd.arity, len(args))
	}
	return fd.constructorFn(args...)
}

func (fd *definition) String() string {
	args := make([]string, 0, fd.arity)
	for i := 0; i < fd.arity; i++ {
		args = append(args, stringutil.Sprintf("arg%d", i+1))
	}
	return stringutil.Sprintf("%s(%s)", fd.name, strings.Join(args, ", "))
}

func (fd *definition) Arity() int {
	return fd.arity
}

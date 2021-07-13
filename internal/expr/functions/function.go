package functions

import (
	"strings"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

// A FunctionDef transforms a list of expressions into a Function.
type FunctionDef interface {
	Name() string
	String() string
	Function(...expr.Expr) (expr.Function, error)
	Arity() int
}
type FunctionsTable map[string]FunctionDef

// PackagesTable represents a table of SQL functions grouped by their packages
type PackagesTable map[string]FunctionsTable

func DefaultPackagesTable() PackagesTable {
	return PackagesTable{
		"":     BuiltinFunctions(),
		"math": MathFunctions(),
	}
}

// GetFunc return a function definition by its package and name.
func (t PackagesTable) GetFunc(pkg string, fname string) (FunctionDef, error) {
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

type functionDef struct {
	name          string
	arity         int
	constructorFn func(...expr.Expr) (expr.Function, error)
}

func (fd *functionDef) Name() string {
	return fd.name
}

func (fd *functionDef) Function(args ...expr.Expr) (expr.Function, error) {
	if len(args) != fd.arity {
		return nil, stringutil.Errorf("%s() takes %d argument, not %d", fd.name, fd.arity, len(args))
	}
	return fd.constructorFn(args...)
}

func (fd *functionDef) String() string {
	args := make([]string, 0, fd.arity)
	for i := 0; i < fd.arity; i++ {
		args = append(args, stringutil.Sprintf("arg%d", i+1))
	}
	return stringutil.Sprintf("%s(%s)", fd.name, strings.Join(args, ", "))
}

func (fd *functionDef) Arity() int {
	return fd.arity
}

// A Aggregator is an expression that aggregates documents into one result.
type Aggregator interface {
	expr.Expr

	Aggregate(env *environment.Environment) error
}

// An AggregatorBuilder is a type that can create aggregators.
type AggregatorBuilder interface {
	expr.Expr

	Aggregator() Aggregator
}

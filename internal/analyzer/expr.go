package analyzer

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/types"
)

// Expr is the bound, typed expression interface. Every node carries type info.
type Expr interface {
	expr()
}

// Nullability describes whether an expression may produce NULLs.
type Nullability uint8

const (
	NullUnknown Nullability = iota // cannot be proven not-null
	Nullable
	NotNull
)

// CastContext encodes whether a coercion was implicit, assignment, or explicit.
type CastContext uint8

const (
	CastExplicit CastContext = iota
	CastAssignment
	CastImplicit
)

// BaseExpr provides common fields for all expression types.
type BaseExpr struct {
	Type        types.Type
	Nullability Nullability
}

func (e *BaseExpr) expr() {}

// ColumnRef represents a reference to a column in an expression.
type ColumnRef struct {
	BaseExpr
	// Index of the RangeTable entry this column belongs to.
	RTEIndex uint64
	// 1-based attribute number of this column.
	AttNo int
}

// Const represents a constant value in an expression.
type Const struct {
	BaseExpr
	// Concrete value of this constant.
	Value types.Value
}

// ParamRef is a $n parameter. Type may be unknown until protocol Bind.
type ParamRef struct {
	BaseExpr
	// 1-based index of the parameter.
	Index int
	// True if the binder knows the type of this parameter.
	Known bool
}

// Coerce casts the input to a target type.
type Coerce struct {
	BaseExpr
	Input   Expr
	Context CastContext
}

// FuncCall is a resolved function call expression.
type FuncCall struct {
	BaseExpr
	FuncName string
	Args     []Expr // already coerced to match func signature
	Variadic bool   // true if last arg is a variadic slice
}

// OpCall is a resolved operator (which is a function under the hood).
type OpCall struct {
	BaseExpr
	Op   database.OpID
	Args []Expr // already coerced to match operator signature
}

// BoolTest applies IS [NOT] TRUE/FALSE/UNKNOWN semantics.
type BoolTest struct {
	BaseExpr
	Input Expr
	Op    BoolTestOp
}

type BoolTestOp uint8

const (
	IsTrue BoolTestOp = iota
	IsNotTrue
	IsFalse
	IsNotFalse
	IsUnknown
	IsNotUnknown
)

// NullTest applies IS [NOT] NULL.
type NullTest struct {
	BaseExpr
	Input Expr
	Not   bool
}

// ArrayExpr constructs an array literal of a fixed element type.
type ArrayExpr struct {
	BaseExpr
	Elems       []Expr
	ElemType    types.Type
	ElemTypeMod int32
}

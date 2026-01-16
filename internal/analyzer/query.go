package analyzer

import (
	"github.com/chaisql/chai/internal/catalog"
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/types"
)

type StmtKind uint8

const (
	StmtSelect StmtKind = iota + 1
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtExplain
)

type AnalyzedQuery struct {
	StatementKind StmtKind
	// FROM items
	RangeTables []*RTE

	// SELECT / RETURNING projection
	TargetEntries []TargetEntry

	// WHERE, GROUP BY, HAVING, WINDOW
	Where Expr
	Group *GroupSpec

	// ORDER BY, DISTINCT, LIMIT/OFFSET
	OrderBy  []OrderKey
	Distinct *DistinctSpec
	Limit    *LimitSpec

	// Set operations (top-level). If non-nil, many fields above are unused at this level.
	SetOp *SetOp

	// Prepared parameter typing (vector indexed by $n)
	ParamTypes []ParamType

	// Output schema promised to clients
	OutputSchema []OutputColumn

	// Dependencies & privilege checks
	Dependencies []Dependency
}

type RTEKind uint8

const (
	RTERelation RTEKind = iota + 1
	RTESubquery
	RTEFunction // SRF
	RTEValues
	RTEJoin
	RTECTE
)

type RTE struct {
	Kind    RTEKind
	Alias   string
	Columns []catalog.Attribute
}

// TargetEntry is one output item. resjunk items are not visible to clients
// but are kept for ORDER BY/DISTINCT/RETURNING mechanics.
type TargetEntry struct {
	Expr Expr
	// Final output name, alias or derived
	Name string
	// 1-based position in output
	Resno int
	// true if not part of the user-visible output
	Resjunk bool
}

// OrderKey binds ORDER BY to concrete expressions and direction.
type OrderKey struct {
	Expr Expr
	Desc bool
}

type GroupSpec struct {
	Keys []Expr
}

// DistinctSpec represents DISTINCT or DISTINCT ON.
type DistinctSpec struct {
	On []Expr // empty → DISTINCT (all columns); else DISTINCT ON keys
}

// LimitSpec holds LIMIT/OFFSET (constant-folded if immutable).
type LimitSpec struct {
	Take Expr
	Skip Expr
}

// SetOp is a typed set operation (UNION/INTERSECT/EXCEPT) with per-branch coercions fixed.
type SetOp struct {
	Op           SetOpKind
	All          bool
	Left         *AnalyzedQuery
	Right        *AnalyzedQuery
	OutputSchema []OutputColumn
}

type SetOpKind uint8

const (
	SetUnion SetOpKind = iota
	SetIntersect
	SetExcept
)

// OutputColumn is the client-visible column schema of the query.
type OutputColumn struct {
	Name     string
	Type     types.Type
	Typemod  int32
	Nullable Nullability
}

// ParamType describes a parameter slot decided by the binder (or left unknown).
type ParamType struct {
	Type   types.Type
	Typmod int32
	Known  bool // false if still unknown at prepare time
}

// ObjectKind classifies dependency/privilege subjects.
type ObjectKind int

const (
	ObjRelation ObjectKind = iota
	ObjColumn
	ObjFunction
	ObjOperator
	ObjType
	ObjCollation
	ObjSequence
	ObjIndex
	ObjView
	ObjSchema
)

type Dependency struct {
	Kind ObjectKind
	// Identifier is a compact key (e.g., RelationID for tables; TypeID for types).
	// For columns, use the owning RelationID with Attno set; for others, Attno=-1.
	Relation database.RelationID
	Attno    int32 // 1-based; -1 when not a column dependency
	Type     types.Type
	Func     database.FuncID
	Operator database.OpID
}

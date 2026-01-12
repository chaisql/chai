package catalog

type (
	SchemaID     uint32
	TypeID       uint32
	CollationID  uint32
	RelationID   uint32
	IndexID      uint32
	ConstraintID uint32
	FuncID       uint32
	OpID         uint32
)

// Schema represents a database schema in the catalog.
type Schema struct {
	OID  SchemaID
	Name string
}

// Type categories roughly follow Postgres for overload heuristics.
type TypeCategory uint8

const (
	TypeCatNumeric TypeCategory = iota
	TypeCatString
	TypeCatDatetime
	TypeCatBool
	TypeCatBinary
	TypeCatUser
)

// Type represents a data type in the catalog.
type Type struct {
	OID                 TypeID
	SchemaOID           SchemaID
	Name                string
	Category            TypeCategory
	Preferred           bool   // for overload resolution
	Collatable          bool   // for string types
	ElemTypeID          TypeID // for array types
	DefaultCollationOID CollationID
}

// Collation represents a collation in the catalog.
type Collation struct {
	OID           CollationID
	SchemaOID     SchemaID
	Name          string
	Locale        string
	Deterministic bool
}

type RelationKind uint8

const (
	RelationKindTable RelationKind = iota + 1
	RelationKindView
	RelationKindMaterializedView
	RelationKindSequence
)

// Relation represents a table, view, materialized view, or sequence.
type Relation struct {
	OID       RelationID
	SchemaOID SchemaID
	Name      string
	Kind      RelationKind
	Owner     string
}

// Attribute represents a column in a relation.
type Attribute struct {
	RelationOID  RelationID
	AttNum       uint16
	Name         string
	TypeOID      TypeID
	TypeMod      int32
	CollationOID CollationID
	NotNull      bool
	Dropped      bool
}

// Index represents a secondary index on a relation.
type Index struct {
	OID         IndexID
	RelationOID RelationID
	Name        string
	Unique      bool
	Keys        []IndexKey
}

// IndexKey represents a key column in an index.
type IndexKey struct {
	AttNum       uint16
	CollationOID CollationID
	Desc         bool
}

type ConstraintKind uint8

const (
	ConstraintKindPrimaryKey ConstraintKind = iota + 1
	ConstraintKindUnique
	ConstraintKindForeignKey
	ConstraintKindCheck
)

// Constraint represents a constraint on a column.
type Constraint struct {
	OID            ConstraintID
	RelationOID    RelationID
	Name           string
	Kind           ConstraintKind
	KeyAttnums     []uint16   // for primary key, unique, foreign key
	RefRelationOID RelationID // for foreign key
	RefKeyAttnums  []uint16   // for foreign key
	CheckExpr      string     // for check constraints
}

// Sequence holds sequence metadata.
type Sequence struct {
	RelationOID RelationID
	TypeOID     TypeID
	Start       int64
	Increment   int64
	MinValue    int64
	MaxValue    int64
	Cycle       bool
	Cache       int64
}

// FunctionVolatility defines how a function's return value changes.
type FunctionVolatility uint8

const (
	// Immutable functions always return the same result for the same inputs.
	FunctionVolatilityImmutable FunctionVolatility = iota + 1
	// Stable functions return the same result for the same inputs within a single statement.
	FunctionVolatilityStable
	// Volatile functions can return different results even for the same inputs within a single statement.
	FunctionVolatilityVolatile
)

// Function represents a function in the catalog.
type Function struct {
	OID           FuncID
	SchemaOID     SchemaID
	Name          string
	ArgTypeOIDs   []TypeID
	ArgNames      []string
	Variadic      bool
	ReturnTypeOID TypeID
	Volatility    FunctionVolatility
	Strict        bool
}

// Operator maps an operator to its implementation function.
type Operator struct {
	OID           OpID
	SchemaOID     SchemaID
	Name          string
	LeftTypeOID   TypeID
	RightTypeOID  TypeID
	ReturnTypeOID TypeID
	FunctionOID   FuncID
	CommutatorOID OpID
	NegatorOID    OpID
}

type CastContext uint8

const (
	CastContextImplicit CastContext = iota + 1

	CastContextAssignment
	CastContextExplicit
)

// Cast represents a pair of coercible types and the function to cast between them.
type Cast struct {
	SourceTypeOID TypeID
	TargetTypeOID TypeID
	FunctionOID   FuncID
	Context       CastContext
}

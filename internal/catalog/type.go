package catalog

type TypeID uint32

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
	OID        TypeID
	Name       string
	Category   TypeCategory
	Preferred  bool   // for overload resolution
	ElemTypeID TypeID // for array types
}

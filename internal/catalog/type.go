package catalog

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

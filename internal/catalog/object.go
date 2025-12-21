package catalog

import "go/types"

type (
	TypeID     uint32
	FuncID     uint32
	OpID       uint32
	RelationID uint32
	SequenceID uint32
)

type Volatility uint8

const (
	VolImmutable Volatility = iota
	VolStable
	VolVolatile
)

type Type struct {
	OID  TypeID
	Name string
	Category
}

type Function struct {
	OID        FuncID
	Name       string
	ArgTypes   []types.Type
	ArgNames   []string
	Variadic   bool
	ReturnType types.Type
	Volatility Volatility
}

type Operator struct {
	OID        OpID
	Name       string
	LeftType   types.Type
	RightType  types.Type
	ReturnType types.Type
	Procedure  FuncID
}

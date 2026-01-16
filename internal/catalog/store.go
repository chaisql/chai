package catalog

import (
	"fmt"
	"sync/atomic"
)

type catalogStore struct {
	idGenerator       atomic.Uint32
	oidSet            map[uint32]struct{}
	Schemas           map[SchemaID]Schema
	SchemasByName     map[string]SchemaID
	Collations        map[CollationID]Collation
	CollationsByName  map[QualifiedNameKey]CollationID
	Types             map[TypeID]Type
	TypesByName       map[QualifiedNameKey]TypeID
	Relations         map[RelationID]Relation
	RelationsByName   map[QualifiedNameKey]RelationID
	Attributes        map[RelationID]map[uint16]Attribute
	AttributesByName  map[RelationID]map[string]uint16
	Indexes           map[RelationID]map[IndexID]Index
	IndexesByName     map[RelationID]map[string]IndexID
	Constraints       map[RelationID]map[ConstraintID]Constraint
	ConstraintsByName map[RelationID]map[string]ConstraintID
	Sequences         map[RelationID]Sequence
	Functions         map[FunctionID]Function
	FunctionsByName   map[QualifiedNameKey][]FunctionID
	Operators         map[OpID]Operator
	OperatorsBySig    map[OperatorSigKey]OpID
	Casts             map[CastKey]Cast
}

func newCatalogStore() *catalogStore {
	return &catalogStore{
		oidSet:            make(map[uint32]struct{}),
		Schemas:           make(map[SchemaID]Schema),
		SchemasByName:     make(map[string]SchemaID),
		Collations:        make(map[CollationID]Collation),
		CollationsByName:  make(map[QualifiedNameKey]CollationID),
		Types:             make(map[TypeID]Type),
		TypesByName:       make(map[QualifiedNameKey]TypeID),
		Relations:         make(map[RelationID]Relation),
		RelationsByName:   make(map[QualifiedNameKey]RelationID),
		Attributes:        make(map[RelationID]map[uint16]Attribute),
		AttributesByName:  make(map[RelationID]map[string]uint16),
		Indexes:           make(map[RelationID]map[IndexID]Index),
		IndexesByName:     make(map[RelationID]map[string]IndexID),
		Constraints:       make(map[RelationID]map[ConstraintID]Constraint),
		ConstraintsByName: make(map[RelationID]map[string]ConstraintID),
		Sequences:         make(map[RelationID]Sequence),
		Functions:         make(map[FunctionID]Function),
		FunctionsByName:   make(map[QualifiedNameKey][]FunctionID),
		Operators:         make(map[OpID]Operator),
		OperatorsBySig:    make(map[OperatorSigKey]OpID),
		Casts:             make(map[CastKey]Cast),
	}
}

func (s *catalogStore) reserveOID(oid uint32) {
	if _, exists := s.oidSet[oid]; exists {
		panic(fmt.Sprintf("OID %d already reserved", oid))
	}
	s.oidSet[oid] = struct{}{}
}

func (s *catalogStore) addSchema(schema Schema) {
	s.reserveOID(uint32(schema.OID))
	s.Schemas[schema.OID] = schema
	s.SchemasByName[schema.Name.Normalized()] = schema.OID
}

func (s *catalogStore) addCollation(collation Collation) {
	s.reserveOID(uint32(collation.OID))
	s.Collations[collation.OID] = collation
	s.CollationsByName[QualifiedNameKey{
		SchemaOID: collation.SchemaOID,
		Name:      collation.Name,
	}] = collation.OID
}

func (s *catalogStore) addType(typ Type) {
	s.reserveOID(uint32(typ.OID))
	s.Types[typ.OID] = typ
	s.TypesByName[QualifiedNameKey{
		SchemaOID: typ.SchemaOID,
		Name:      typ.Name,
	}] = typ.OID
}

func (s *catalogStore) addRelation(relation Relation) {
	s.reserveOID(uint32(relation.OID))
	s.Relations[relation.OID] = relation
	s.RelationsByName[QualifiedNameKey{
		SchemaOID: relation.SchemaOID,
		Name:      relation.Name,
	}] = relation.OID
}

func (s *catalogStore) addAttribute(attr Attribute) {
	if s.Attributes[attr.RelationOID] == nil {
		s.Attributes[attr.RelationOID] = make(map[uint16]Attribute)
	}
	if s.AttributesByName[attr.RelationOID] == nil {
		s.AttributesByName[attr.RelationOID] = make(map[string]uint16)
	}
	s.Attributes[attr.RelationOID][attr.AttNum] = attr
	s.AttributesByName[attr.RelationOID][attr.Name] = attr.AttNum
}

func (s *catalogStore) addIndex(index Index) {
	s.reserveOID(uint32(index.OID))
	if s.Indexes[index.RelationOID] == nil {
		s.Indexes[index.RelationOID] = make(map[IndexID]Index)
	}
	if s.IndexesByName[index.RelationOID] == nil {
		s.IndexesByName[index.RelationOID] = make(map[string]IndexID)
	}
	s.Indexes[index.RelationOID][index.OID] = index
	s.IndexesByName[index.RelationOID][index.Name] = index.OID
}

func (s *catalogStore) addConstraint(constraint Constraint) {
	s.reserveOID(uint32(constraint.OID))
	if s.Constraints[constraint.RelationOID] == nil {
		s.Constraints[constraint.RelationOID] = make(map[ConstraintID]Constraint)
	}
	if s.ConstraintsByName[constraint.RelationOID] == nil {
		s.ConstraintsByName[constraint.RelationOID] = make(map[string]ConstraintID)
	}
	s.Constraints[constraint.RelationOID][constraint.OID] = constraint
	s.ConstraintsByName[constraint.RelationOID][constraint.Name] = constraint.OID
}

func (s *catalogStore) addSequence(seq Sequence) {
	s.Sequences[seq.RelationOID] = seq
}

func (s *catalogStore) addFunction(fn Function) {
	s.reserveOID(uint32(fn.OID))
	s.Functions[fn.OID] = fn
	key := QualifiedNameKey{
		SchemaOID: fn.SchemaOID,
		Name:      fn.Name,
	}
	s.FunctionsByName[key] = append(s.FunctionsByName[key], fn.OID)
}

func (s *catalogStore) addOperator(op Operator) {
	s.reserveOID(uint32(op.OID))
	s.Operators[op.OID] = op
	key := OperatorSigKey{
		SchemaOID: op.SchemaOID,
		Name:      op.Name,
		Left:      op.LeftTypeOID,
		Right:     op.RightTypeOID,
	}
	s.OperatorsBySig[key] = op.OID
}

func (s *catalogStore) addCast(cast Cast) {
	s.Casts[CastKey{
		Source: cast.SourceTypeOID,
		Target: cast.TargetTypeOID,
	}] = cast
}

package catalog

import "slices"

// QualifiedName represents schema-qualified (or unqualified) names.
// If Schema is empty, resolution uses SearchPath.
type QualifiedName struct {
	Schema Ident
	Name   Ident
}

func (qn QualifiedName) IsQualified() bool {
	return qn.Schema.Normalized() != ""
}

// SearchPath is the ordered list of schema names used for resolution.
type SearchPath []Ident

type Catalog struct {
	Schemas           map[SchemaID]Schema
	SchemasByName     map[string]SchemaID
	Types             map[TypeID]Type
	TypesByName       map[QualifiedNameKey]TypeID
	Collations        map[CollationID]Collation
	CollationsByName  map[QualifiedNameKey]CollationID
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

type QualifiedNameKey struct {
	SchemaOID SchemaID
	Name      string
}

type Ident struct {
	Raw    string
	Folded string
	Quoted bool
}

func (i Ident) Normalized() string {
	if i.Quoted {
		return i.Raw
	}
	return i.Folded
}

func (i Ident) String() string {
	return i.Raw
}

type OperatorSigKey struct {
	SchemaOID SchemaID
	Name      string
	Left      TypeID
	Right     TypeID
}

type CastKey struct {
	Source TypeID
	Target TypeID
}

// ResolveSchema finds a schema by name.
func (c *Catalog) ResolveSchema(schema Ident) (Schema, bool, error) {
	oid, ok := c.SchemasByName[schema.Normalized()]
	if !ok {
		return Schema{}, false, nil
	}
	sch, ok := c.Schemas[oid]
	return sch, ok, nil
}

// GetSchema finds a schema by OID.
func (c *Catalog) GetSchema(schema SchemaID) (Schema, bool) {
	sch, ok := c.Schemas[schema]
	return sch, ok
}

// ResolveType resolves a type by (possibly qualified) name and search_path.
func (c *Catalog) ResolveType(sp SearchPath, name QualifiedName) (Type, bool, error) {
	return resolveObject(c, c.TypesByName, c.Types, sp, name)
}

// GetType finds a type by OID.
func (c *Catalog) GetType(typ TypeID) (Type, bool) {
	t, ok := c.Types[typ]
	return t, ok
}

// ResolveCollation resolves a collation by name and search_path.
func (c *Catalog) ResolveCollation(sp SearchPath, name QualifiedName) (Collation, bool, error) {
	return resolveObject(c, c.CollationsByName, c.Collations, sp, name)
}

// GetCollation finds a collation by OID.
func (c *Catalog) GetCollation(col CollationID) (Collation, bool) {
	cl, ok := c.Collations[col]
	return cl, ok
}

// ResolveRelation resolves a relation (table/view/mview/sequence) by name and search_path.
func (c *Catalog) ResolveRelation(sp SearchPath, name QualifiedName) (Relation, bool, error) {
	return resolveObject(c, c.RelationsByName, c.Relations, sp, name)
}

// GetRelation finds a relation by OID.
func (c *Catalog) GetRelation(rel RelationID) (Relation, bool) {
	r, ok := c.Relations[rel]
	return r, ok
}

// ListAttributes returns user-visible columns (optionally including dropped).
func (c *Catalog) ListAttributes(rel RelationID, includeDropped bool) (map[uint16]Attribute, error) {
	attrs, ok := c.Attributes[rel]
	if !ok {
		return nil, NewRelationNotFoundError(rel)
	}
	if includeDropped {
		return attrs, nil
	}

	var hasDropped bool
	for _, attr := range attrs {
		if attr.Dropped {
			hasDropped = true
			break
		}
	}
	if !hasDropped {
		return attrs, nil
	}

	res := make(map[uint16]Attribute)
	for _, attr := range attrs {
		if !attr.Dropped {
			res[attr.AttNum] = attr
		}
	}

	return res, nil
}

// LookupAttribute finds a column by name in a relation.
func (c *Catalog) LookupAttribute(rel RelationID, col Ident) (Attribute, bool, error) {
	attrsByName, ok := c.AttributesByName[rel]
	if !ok {
		return Attribute{}, false, NewRelationNotFoundError(rel)
	}
	attNum, ok := attrsByName[col.Normalized()]
	if !ok {
		return Attribute{}, false, nil
	}
	attrs, ok := c.Attributes[rel]
	if !ok {
		return Attribute{}, false, NewColumnNotFoundError(rel, col.String())
	}
	return attrs[attNum], true, nil
}

// ListIndexes returns all indexes defined on a relation.
func (c *Catalog) ListIndexes(rel RelationID) (map[IndexID]Index, error) {
	indexes, ok := c.Indexes[rel]
	if !ok {
		return nil, NewRelationNotFoundError(rel)
	}

	return indexes, nil
}

// LookupIndex finds an index by name in a relation.
func (c *Catalog) LookupIndex(rel RelationID, name Ident) (Index, bool, error) {
	indexesByName, ok := c.IndexesByName[rel]
	if !ok {
		return Index{}, false, NewRelationNotFoundError(rel)
	}

	indexID, ok := indexesByName[name.Normalized()]
	if !ok {
		return Index{}, false, nil
	}

	indexes, ok := c.Indexes[rel]
	if !ok {
		return Index{}, false, NewIndexNotFoundError(rel, name.String())
	}

	index, ok := indexes[indexID]
	return index, ok, nil
}

// ListConstraints returns all constraints defined on a relation.
func (c *Catalog) ListConstraints(rel RelationID) (map[ConstraintID]Constraint, error) {
	constraints, ok := c.Constraints[rel]
	if !ok {
		return nil, NewRelationNotFoundError(rel)
	}

	return constraints, nil
}

// LookupConstraint finds a constraint by name in a relation.
func (c *Catalog) LookupConstraint(rel RelationID, name Ident) (Constraint, bool, error) {
	constraintsByName, ok := c.ConstraintsByName[rel]
	if !ok {
		return Constraint{}, false, NewRelationNotFoundError(rel)
	}

	constraintID, ok := constraintsByName[name.Normalized()]
	if !ok {
		return Constraint{}, false, nil
	}

	constraints, ok := c.Constraints[rel]
	if !ok {
		return Constraint{}, false, NewConstraintNotFoundError(rel, name.String())
	}

	constraint, ok := constraints[constraintID]
	return constraint, ok, nil
}

// GetSequence finds a sequence metadata by relation OID.
func (c *Catalog) GetSequence(rel RelationID) (Sequence, bool) {
	seq, ok := c.Sequences[rel]
	return seq, ok
}

func (c *Catalog) ResolveFunction(sp SearchPath, name QualifiedName, argTypes []TypeID) (Function, bool, error) {
	var sch Schema
	var found bool
	var err error

	// If schema is specified, resolve directly.
	if name.IsQualified() {
		sch, found, err = c.ResolveSchema(name.Schema)
		if err != nil {
			return Function{}, false, err
		}
		if !found {
			return Function{}, false, NewSchemaNotFoundError(name.Schema.Normalized())
		}

		funcIDs, ok := c.FunctionsByName[QualifiedNameKey{SchemaOID: sch.OID, Name: name.Name.Normalized()}]
		if !ok {
			return Function{}, false, nil
		}

		for _, fid := range funcIDs {
			fn := c.Functions[fid]
			if slices.Equal(fn.ArgTypeOIDs, argTypes) {
				return fn, true, nil
			}
		}

		return Function{}, false, nil
	}

	// Otherwise, search through the search path.
	for _, schema := range sp {
		sch, found, err = c.ResolveSchema(schema)
		if err != nil {
			return Function{}, false, err
		}
		if !found {
			continue
		}

		funcIDs, ok := c.FunctionsByName[QualifiedNameKey{SchemaOID: sch.OID, Name: name.Name.Normalized()}]
		if !ok {
			continue
		}

		for _, fid := range funcIDs {
			fn := c.Functions[fid]
			if slices.Equal(fn.ArgTypeOIDs, argTypes) {
				return fn, true, nil
			}
		}
	}

	return Function{}, false, nil
}

// GetFunction finds a function by OID.
func (c *Catalog) GetFunction(funcID FunctionID) (Function, bool) {
	fn, ok := c.Functions[funcID]
	return fn, ok
}

// ResolveOperator resolves an operator by (possibly qualified) name, left type, right type and search_path.
func (c *Catalog) ResolveOperator(sp SearchPath, name QualifiedName, left TypeID, right TypeID) (Operator, bool, error) {
	var sch Schema
	var found bool
	var err error

	// If schema is specified, resolve directly.
	if name.IsQualified() {
		sch, found, err = c.ResolveSchema(name.Schema)
		if err != nil {
			return Operator{}, false, err
		}
		if !found {
			return Operator{}, false, NewSchemaNotFoundError(name.Schema.Normalized())
		}

		opID, ok := c.OperatorsBySig[OperatorSigKey{SchemaOID: sch.OID, Name: name.Name.Normalized(), Left: left, Right: right}]
		if !ok {
			return Operator{}, false, nil
		}

		op, ok := c.Operators[opID]
		return op, ok, nil
	}

	// Otherwise, search through the search path.
	for _, schema := range sp {
		sch, found, err = c.ResolveSchema(schema)
		if err != nil {
			return Operator{}, false, err
		}
		if !found {
			continue
		}

		opID, ok := c.OperatorsBySig[OperatorSigKey{SchemaOID: sch.OID, Name: name.Name.Normalized(), Left: left, Right: right}]
		if !ok {
			continue
		}

		op, ok := c.Operators[opID]
		return op, ok, nil
	}

	return Operator{}, false, nil
}

// GetOperator finds an operator by OID.
func (c *Catalog) GetOperator(opID OpID) (Operator, bool) {
	op, ok := c.Operators[opID]
	return op, ok
}

// ResolveCast resolves a cast by source and target type OIDs.
func (c *Catalog) ResolveCast(source TypeID, target TypeID) (Cast, bool) {
	cast, ok := c.Casts[CastKey{Source: source, Target: target}]
	return cast, ok
}

func resolveObject[T any, U comparable](c *Catalog, byName map[QualifiedNameKey]U, byOID map[U]T, sp SearchPath, name QualifiedName) (T, bool, error) {
	var sch Schema
	var found bool
	var err error

	// If schema is specified, resolve directly.
	if name.IsQualified() {
		sch, found, err = c.ResolveSchema(name.Schema)
		if err != nil {
			var zero T
			return zero, false, err
		}
		if !found {
			var zero T
			return zero, false, NewSchemaNotFoundError(name.Schema.Normalized())
		}
		oid, ok := byName[QualifiedNameKey{SchemaOID: sch.OID, Name: name.Name.Normalized()}]
		if !ok {
			var zero T
			return zero, false, nil
		}

		obj, ok := byOID[oid]
		return obj, ok, nil
	}

	// Otherwise, search through the search path.
	for _, schema := range sp {
		sch, found, err = c.ResolveSchema(schema)
		if err != nil {
			var zero T
			return zero, false, err
		}
		if !found {
			continue
		}

		// Check if type exists in this schema.
		oid, ok := byName[QualifiedNameKey{SchemaOID: sch.OID, Name: name.Name.Normalized()}]
		if ok {
			obj, ok := byOID[oid]
			return obj, ok, nil
		}
	}

	var zero T
	return zero, false, nil
}

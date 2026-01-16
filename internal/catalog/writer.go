package catalog

import "fmt"

type Writer struct {
	catalog *Catalog
}

func NewWriter(catalog *Catalog) *Writer {
	return &Writer{
		catalog: catalog,
	}
}

func (w *Writer) CreateSchema(schema Schema) {
	w.catalog.store.addSchema(schema)
}

func (w *Writer) CreateType(typ Type) {
	w.catalog.store.addType(typ)
}

func (w *Writer) CreateCollation(collation Collation) {
	w.catalog.store.addCollation(collation)
}

func (w *Writer) CreateTable(name QualifiedName, columns []Attribute, constraints []Constraint) (*Relation, error) {
	var sch Schema
	if name.Schema.Normalized() == "" {
		sch = Schema{
			OID:  PublicSchemaOID,
			Name: Ident{Raw: PublicSchemaName, Folded: PublicSchemaName, Quoted: false},
		}
	} else {
		var found bool
		var err error

		sch, found, err = w.catalog.ResolveSchema(name.Schema)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("schema %q not found", name.Schema.Raw)
		}
	}

	relOID := RelationID(w.catalog.store.idGenerator.Add(1))

	relation := Relation{
		OID:       relOID,
		SchemaOID: sch.OID,
		Name:      name.Name.Normalized(),
		Kind:      RelationKindTable,
	}

	w.catalog.store.addRelation(relation)

	for i, col := range columns {
		col.RelationOID = relOID
		col.AttNum = uint16(i + 1)
		w.catalog.store.addAttribute(col)
	}

	for _, cons := range constraints {
		cons.RelationOID = relOID
		cons.OID = ConstraintID(w.catalog.store.idGenerator.Add(1))
		w.catalog.store.addConstraint(cons)
	}

	return &relation, nil
}

package catalog

import (
	"strconv"

	"github.com/cockroachdb/errors"
)

type SchemaNotFoundError struct {
	Schema string
}

func NewSchemaNotFoundError(schema string) error {
	return errors.WithStack(&SchemaNotFoundError{Schema: schema})
}

func (e *SchemaNotFoundError) Error() string {
	return "schema not found: " + e.Schema
}

type RelationNotFoundError struct {
	Relation RelationID
}

func NewRelationNotFoundError(relation RelationID) error {
	return errors.WithStack(&RelationNotFoundError{Relation: relation})
}

func (e *RelationNotFoundError) Error() string {
	return "relation not found: " + strconv.Itoa(int(e.Relation))
}

type ColumnNotFoundError struct {
	Relation RelationID
	Column   string
}

func NewColumnNotFoundError(relation RelationID, column string) error {
	return errors.WithStack(&ColumnNotFoundError{Relation: relation, Column: column})
}

func (e *ColumnNotFoundError) Error() string {
	return "column not found: " + e.Column + " in relation " + strconv.Itoa(int(e.Relation))
}

type IndexNotFoundError struct {
	Relation RelationID
	Index    string
}

func NewIndexNotFoundError(relation RelationID, index string) error {
	return errors.WithStack(&IndexNotFoundError{Relation: relation, Index: index})
}

func (e *IndexNotFoundError) Error() string {
	return "index not found: " + e.Index + " in relation " + strconv.Itoa(int(e.Relation))
}

type ConstraintNotFoundError struct {
	Relation   RelationID
	Constraint string
}

func NewConstraintNotFoundError(relation RelationID, constraint string) error {
	return errors.WithStack(&ConstraintNotFoundError{Relation: relation, Constraint: constraint})
}

func (e *ConstraintNotFoundError) Error() string {
	return "constraint not found: " + e.Constraint + " in relation " + strconv.Itoa(int(e.Relation))
}

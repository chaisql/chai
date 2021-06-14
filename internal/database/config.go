package database

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
)

// TableInfo contains information about a table.
type TableInfo struct {
	// name of the table.
	TableName string
	// name of the store associated with the table.
	StoreName []byte
	ReadOnly  bool

	FieldConstraints FieldConstraints
}

// GetPrimaryKey returns the field constraint of the primary key.
// Returns nil if there is no primary key.
func (ti *TableInfo) GetPrimaryKey() *FieldConstraint {
	for _, f := range ti.FieldConstraints {
		if f.IsPrimaryKey {
			return f
		}
	}

	return nil
}

// String returns a SQL representation.
func (ti *TableInfo) String() string {
	var s strings.Builder

	stringutil.Fprintf(&s, "CREATE TABLE %s", ti.TableName)
	if len(ti.FieldConstraints) > 0 {
		s.WriteString(" (")
	}

	for i, fc := range ti.FieldConstraints {
		if fc.IsInferred {
			continue
		}

		if i > 0 {
			s.WriteString(", ")
		}

		s.WriteString(fc.String())
	}

	if len(ti.FieldConstraints) > 0 {
		s.WriteString(")")
	}

	return s.String()
}

// Clone creates another tableInfo with the same values.
func (ti *TableInfo) Clone() *TableInfo {
	cp := *ti
	cp.FieldConstraints = nil
	cp.FieldConstraints = append(cp.FieldConstraints, ti.FieldConstraints...)
	return &cp
}

// IndexInfo holds the configuration of an index.
type IndexInfo struct {
	TableName string
	// name of the store associated with the index.
	StoreName []byte
	IndexName string
	Paths     []document.Path

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	// If set, the index is typed and only accepts values of those types.
	Types []document.ValueType

	// If set, this index has been created from a table constraint
	// i.e CREATE TABLE tbl(a INT UNIQUE)
	// The path refers to the path this index is related to.
	ConstraintPath document.Path
}

// String returns a SQL representation.
func (i *IndexInfo) String() string {
	var s strings.Builder

	s.WriteString("CREATE ")
	if i.Unique {
		s.WriteString("UNIQUE ")
	}

	stringutil.Fprintf(&s, "INDEX %s ON %s (", i.IndexName, i.TableName)

	for i, p := range i.Paths {
		if i > 0 {
			s.WriteString(", ")
		}

		// Path
		s.WriteString(p.String())
	}

	s.WriteString(")")

	return s.String()
}

// Clone returns a copy of the index information.
func (i IndexInfo) Clone() *IndexInfo {
	c := i

	c.Paths = make([]document.Path, len(i.Paths))
	for i, p := range i.Paths {
		c.Paths[i] = p.Clone()
	}

	c.Types = make([]document.ValueType, len(i.Types))
	copy(c.Types, i.Types)

	return &c
}

type Indexes []*Index

func (i Indexes) GetIndex(name string) *Index {
	for _, idx := range i {
		if idx.Info.IndexName == name {
			return idx
		}
	}

	return nil
}

func (i Indexes) GetIndexByPath(p document.Path) *Index {
	for _, idx := range i {
		if idx.Info.Paths[0].IsEqual(p) {
			return idx
		}
	}

	return nil
}

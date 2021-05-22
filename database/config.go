package database

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/stringutil"
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

// ToDocument turns ti into a document.
func (ti *TableInfo) ToDocument() document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("sql", document.NewTextValue(ti.String()))
	buf.Add("table_name", document.NewTextValue(ti.TableName))
	buf.Add("store_name", document.NewBlobValue(ti.StoreName))
	return buf
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

		// Path
		s.WriteString(fc.Path.String())

		// Type
		if fc.Type != 0 {
			stringutil.Fprintf(&s, " %s", strings.ToUpper(fc.Type.String()))
		}

		// Not null
		if fc.IsNotNull {
			s.WriteString(" NOT NULL")
		}

		// Default value
		if fc.HasDefaultValue() {
			stringutil.Fprintf(&s, " DEFAULT %s", fc.DefaultValue.String())
		}

		// Primary key
		if fc.IsPrimaryKey {
			s.WriteString(" PRIMARY KEY")
		}

		// Unique must not be written as it an index is already created for it
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
	IndexName string
	Paths     []document.Path

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	// If set, the index is typed and only accepts values of those types.
	Types []document.ValueType
}

// ToDocument creates a document from an IndexConfig.
func (i *IndexInfo) ToDocument() document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("sql", document.NewTextValue(i.String()))
	buf.Add("index_name", document.NewTextValue(i.IndexName))
	buf.Add("table_name", document.NewTextValue(i.TableName))

	return buf
}

// String returns a SQL representation.
func (i *IndexInfo) String() string {
	var s strings.Builder

	stringutil.Fprintf(&s, "CREATE INDEX %s ON %s (", i.IndexName, i.TableName)

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

func arrayToPath(a document.Array) (document.Path, error) {
	var path document.Path

	err := a.Iterate(func(_ int, value document.Value) error {
		if value.Type == document.TextValue {
			path = append(path, document.PathFragment{FieldName: value.V.(string)})
		} else {
			path = append(path, document.PathFragment{ArrayIndex: int(value.V.(int64))})
		}
		return nil
	})

	return path, err
}

func pathToArray(path document.Path) document.Array {
	abuf := document.NewValueBuffer()
	for _, p := range path {
		if p.FieldName != "" {
			abuf = abuf.Append(document.NewTextValue(p.FieldName))
		} else {
			abuf = abuf.Append(document.NewIntegerValue(int64(p.ArrayIndex)))
		}
	}

	return abuf
}

package database

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/internal/types"
)

// TableInfo contains information about a table.
type TableInfo struct {
	// name of the table.
	TableName string
	// namespace of the store associated with the table.
	StoreNamespace tree.Namespace
	ReadOnly       bool

	// Name of the rowid sequence if any.
	RowidSequenceName string

	FieldConstraints FieldConstraints
	TableConstraints TableConstraints
}

func (ti *TableInfo) AddFieldConstraint(newFc *FieldConstraint) error {
	if ti.FieldConstraints.ByField == nil {
		ti.FieldConstraints.ByField = make(map[string]*FieldConstraint)
	}

	return ti.FieldConstraints.Add(newFc)
}

func (ti *TableInfo) AddTableConstraint(newTc *TableConstraint) error {
	// ensure the field paths exist
	for _, p := range newTc.Paths {
		if ti.GetFieldConstraintForPath(p) == nil {
			return fmt.Errorf("field %q does not exist for table %q", p, ti.TableName)
		}
	}

	// ensure paths are not duplicated
	// i.e. PRIMARY KEY (a, a) is not allowed
	m := make(map[string]bool)
	for _, p := range newTc.Paths {
		ps := p.String()
		if _, ok := m[ps]; ok {
			return fmt.Errorf("duplicate path %q for constraint", ps)
		}
		m[ps] = true
	}

	switch {
	case newTc.PrimaryKey:
		// ensure there is only one primary key
		for _, tc := range ti.TableConstraints {
			if tc.PrimaryKey {
				return fmt.Errorf("multiple primary keys for table %q are not allowed", ti.TableName)
			}
		}

		// add NOT NULL constraint to paths
		for _, p := range newTc.Paths {
			fc := ti.GetFieldConstraintForPath(p)
			fc.IsNotNull = true
		}

		// generate name if not provided
		if newTc.Name == "" {
			newTc.Name = ti.TableName + "_pk"
		}
	case newTc.Check != nil:
		// generate name if not provided
		if newTc.Name == "" {
			var i int
			for _, tc := range ti.TableConstraints {
				if tc.Check != nil {
					i++
				}
			}

			name := ti.TableName + "_check"
			if i > 0 {
				name += strconv.Itoa(i)
			}

			newTc.Name = name
		}
	case newTc.Unique:
		// ensure there is only one unique constraint for the same paths
		for _, tc := range ti.TableConstraints {
			if tc.Unique && tc.Paths.IsEqual(newTc.Paths) {
				return errors.Errorf("duplicate UNIQUE table contraint on %q", newTc.Paths)
			}
		}

		// generate name if not provided
		if newTc.Name == "" {
			newTc.Name = fmt.Sprintf("%s_%s_unique", ti.TableName, pathsToIndexName(newTc.Paths))
		}
	default:
		return errors.New("invalid table constraint")
	}

	// ensure the name is unique
	for _, tc := range ti.TableConstraints {
		if tc.Name == newTc.Name {
			return errors.Errorf("duplicate table constraint name %q", newTc.Name)
		}
	}

	ti.TableConstraints = append(ti.TableConstraints, newTc)
	return nil
}

func (ti *TableInfo) GetPrimaryKey() *PrimaryKey {
	var pk PrimaryKey

	for _, tc := range ti.TableConstraints {
		if !tc.PrimaryKey {
			continue
		}

		pk.Paths = tc.Paths
		pk.SortOrder = tc.SortOrder

		for _, pp := range tc.Paths {
			fc := ti.GetFieldConstraintForPath(pp)
			if fc != nil {
				pk.Types = append(pk.Types, fc.Type)
			} else {
				pk.Types = append(pk.Types, 0)
			}
		}

		return &pk
	}

	return nil
}

func (ti *TableInfo) PrimaryKeySortOrder() tree.SortOrder {
	pk := ti.GetPrimaryKey()
	if pk == nil {
		return 0
	}

	return pk.SortOrder
}

func (ti *TableInfo) GetFieldConstraintForPath(p object.Path) *FieldConstraint {
	return ti.FieldConstraints.GetFieldConstraintForPath(p)
}

func (ti *TableInfo) EncodeKey(key *tree.Key) ([]byte, error) {
	var order tree.SortOrder
	pk := ti.GetPrimaryKey()
	if pk != nil {
		order = pk.SortOrder
	}
	return key.Encode(ti.StoreNamespace, order)
}

// String returns a SQL representation.
func (ti *TableInfo) String() string {
	var s strings.Builder

	fmt.Fprintf(&s, "CREATE TABLE %s", stringutil.NormalizeIdentifier(ti.TableName, '`'))
	if len(ti.FieldConstraints.Ordered) > 0 || len(ti.TableConstraints) > 0 || ti.FieldConstraints.AllowExtraFields {
		s.WriteString(" (")
	}

	var hasConstraints bool
	for i, fc := range ti.FieldConstraints.Ordered {
		if i > 0 {
			s.WriteString(", ")
		}

		s.WriteString(fc.String())

		hasConstraints = true
	}

	for i, tc := range ti.TableConstraints {
		if i > 0 || hasConstraints {
			s.WriteString(", ")
		}

		s.WriteString(tc.String())
		hasConstraints = true
	}

	if ti.FieldConstraints.AllowExtraFields {
		if hasConstraints {
			s.WriteString(", ")
		}
		s.WriteString("...")
		hasConstraints = true
	}

	if hasConstraints {
		s.WriteString(")")
	}

	return s.String()
}

// Clone creates another tableInfo with the same values.
func (ti *TableInfo) Clone() *TableInfo {
	cp := *ti
	cp.FieldConstraints.Ordered = nil
	cp.FieldConstraints.ByField = make(map[string]*FieldConstraint)
	cp.TableConstraints = nil
	cp.FieldConstraints.Ordered = append(cp.FieldConstraints.Ordered, ti.FieldConstraints.Ordered...)
	for i := range ti.FieldConstraints.Ordered {
		cp.FieldConstraints.ByField[ti.FieldConstraints.Ordered[i].Field] = ti.FieldConstraints.Ordered[i]
	}
	cp.TableConstraints = append(cp.TableConstraints, ti.TableConstraints...)
	return &cp
}

type PrimaryKey struct {
	Paths     object.Paths
	Types     []types.ValueType
	SortOrder tree.SortOrder
}

// IndexInfo holds the configuration of an index.
type IndexInfo struct {
	// namespace of the store associated with the index.
	StoreNamespace tree.Namespace
	IndexName      string
	Paths          []object.Path

	// Sort order of each indexed field.
	KeySortOrder tree.SortOrder

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	// If set, this index has been created from a table constraint
	// i.e CREATE TABLE tbl(a INT UNIQUE)
	// The path refers to the path this index is related to.
	Owner Owner
}

// String returns a SQL representation.
func (idx *IndexInfo) String() string {
	var s strings.Builder

	s.WriteString("CREATE ")
	if idx.Unique {
		s.WriteString("UNIQUE ")
	}

	fmt.Fprintf(&s, "INDEX %s ON %s (", stringutil.NormalizeIdentifier(idx.IndexName, '`'), stringutil.NormalizeIdentifier(idx.Owner.TableName, '`'))

	for i, p := range idx.Paths {
		if i > 0 {
			s.WriteString(", ")
		}

		// Path
		s.WriteString(p.String())

		if idx.KeySortOrder.IsDesc(i) {
			s.WriteString(" DESC")
		}
	}

	s.WriteString(")")

	return s.String()
}

// Clone returns a copy of the index information.
func (i IndexInfo) Clone() *IndexInfo {
	c := i

	c.Paths = make([]object.Path, len(i.Paths))
	for i, p := range i.Paths {
		c.Paths[i] = p.Clone()
	}

	return &c
}

// SequenceInfo holds the configuration of a sequence.
type SequenceInfo struct {
	Name        string
	IncrementBy int64
	Min, Max    int64
	Start       int64
	Cache       uint64
	Cycle       bool
	Owner       Owner
}

// String returns a SQL representation.
func (s *SequenceInfo) String() string {
	var b strings.Builder

	b.WriteString("CREATE SEQUENCE ")
	b.WriteString(stringutil.NormalizeIdentifier(s.Name, '`'))

	asc := s.IncrementBy > 0

	if s.IncrementBy != 1 {
		fmt.Fprintf(&b, " INCREMENT BY %d", s.IncrementBy)
	}

	if (asc && s.Min != 1) || (!asc && s.Min != math.MinInt64) {
		fmt.Fprintf(&b, " MINVALUE %d", s.Min)
	}

	if (asc && s.Max != math.MaxInt64) || (!asc && s.Max != -1) {
		fmt.Fprintf(&b, " MAXVALUE %d", s.Max)
	}

	if (asc && s.Start != s.Min) || (!asc && s.Start != s.Max) {
		fmt.Fprintf(&b, " START WITH %d", s.Start)
	}

	if s.Cache != 1 {
		fmt.Fprintf(&b, " CACHE %d", s.Cache)
	}

	if s.Cycle {
		b.WriteString(" CYCLE")
	}

	return b.String()
}

// Clone returns a copy of the sequence information.
func (s SequenceInfo) Clone() *SequenceInfo {
	return &s
}

// Owner is used to determine who owns a relation.
// If the relation has been created by a table (for rowids for example),
// only the TableName is filled.
// If it has been created by a field constraint (for identities for example), the
// path must also be filled.
type Owner struct {
	TableName string
	Paths     object.Paths
}

package database

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/chaisql/chai/internal/stringutil"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
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

	ColumnConstraints ColumnConstraints
	TableConstraints  TableConstraints

	PrimaryKey *PrimaryKey
}

func (ti *TableInfo) AddColumnConstraint(newCc *ColumnConstraint) error {
	if ti.ColumnConstraints.ByColumn == nil {
		ti.ColumnConstraints.ByColumn = make(map[string]*ColumnConstraint)
	}

	return ti.ColumnConstraints.Add(newCc)
}

func (ti *TableInfo) AddTableConstraint(newTc *TableConstraint) error {
	// ensure the field paths exist
	for _, c := range newTc.Columns {
		if ti.GetColumnConstraint(c) == nil {
			return fmt.Errorf("column %q does not exist for table %q", c, ti.TableName)
		}
	}

	// ensure paths are not duplicated
	// i.e. PRIMARY KEY (a, a) is not allowed
	m := make(map[string]bool)
	for _, c := range newTc.Columns {
		ps := c
		if _, ok := m[ps]; ok {
			return fmt.Errorf("duplicate column %q for constraint", ps)
		}
		m[ps] = true
	}

	switch {
	case newTc.PrimaryKey:
		// ensure there is only one primary key
		if ti.PrimaryKey != nil {
			return fmt.Errorf("multiple primary keys for table %q are not allowed", ti.TableName)
		}

		// add NOT NULL constraint to columns
		for _, p := range newTc.Columns {
			fc := ti.GetColumnConstraint(p)
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
			if tc.Unique && slices.Equal(tc.Columns, newTc.Columns) {
				return errors.Errorf("duplicate UNIQUE table contraint on %q", newTc.Columns)
			}
		}

		// generate name if not provided
		if newTc.Name == "" {
			newTc.Name = fmt.Sprintf("%s_%s_unique", ti.TableName, columnsToIndexName(newTc.Columns))
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

	ti.BuildPrimaryKey()
	return nil
}

// Validate ensures the constraints are valid.
func (ti *TableInfo) Validate() error {
	// ensure the primary key is valid
	if ti.PrimaryKey != nil {
		if len(ti.PrimaryKey.Columns) != len(ti.PrimaryKey.Types) {
			return errors.New("invalid primary key")
		}
	}

	// ensure the constraints are valid
	for _, tc := range ti.TableConstraints {
		if tc.Check != nil {
			if err := tc.Check.Validate(ti); err != nil {
				return err
			}
		}
	}

	return nil
}

func (ti *TableInfo) BuildPrimaryKey() {
	var pk PrimaryKey

	for _, tc := range ti.TableConstraints {
		if !tc.PrimaryKey {
			continue
		}

		pk.Columns = tc.Columns
		pk.SortOrder = tc.SortOrder

		for _, pp := range tc.Columns {
			fc := ti.GetColumnConstraint(pp)
			if fc != nil {
				pk.Types = append(pk.Types, fc.Type)
			} else {
				pk.Types = append(pk.Types, 0)
			}
		}

		ti.PrimaryKey = &pk
	}
}

func (ti *TableInfo) PrimaryKeySortOrder() tree.SortOrder {
	if ti.PrimaryKey == nil {
		return 0
	}

	return ti.PrimaryKey.SortOrder
}

func (ti *TableInfo) GetColumnConstraint(column string) *ColumnConstraint {
	return ti.ColumnConstraints.GetColumnConstraint(column)
}

func (ti *TableInfo) EncodeKey(key *tree.Key) ([]byte, error) {
	var order tree.SortOrder
	if ti.PrimaryKey != nil {
		order = ti.PrimaryKey.SortOrder
	}

	return key.Encode(ti.StoreNamespace, order)
}

// String returns a SQL representation.
func (ti *TableInfo) String() string {
	var s strings.Builder

	fmt.Fprintf(&s, "CREATE TABLE %s (", stringutil.NormalizeIdentifier(ti.TableName, '`'))

	for i, fc := range ti.ColumnConstraints.Ordered {
		if i > 0 {
			s.WriteString(", ")
		}

		s.WriteString(fc.String())
	}

	for i, tc := range ti.TableConstraints {
		if i == 0 && len(ti.ColumnConstraints.Ordered) > 0 {
			s.WriteString(", ")
		}
		if i > 0 {
			s.WriteString(", ")
		}

		s.WriteString(tc.String())
	}

	s.WriteString(")")

	return s.String()
}

// Clone creates another tableInfo with the same values.
func (ti *TableInfo) Clone() *TableInfo {
	cp := *ti
	cp.ColumnConstraints.Ordered = nil
	cp.ColumnConstraints.ByColumn = make(map[string]*ColumnConstraint)
	cp.TableConstraints = nil
	cp.ColumnConstraints.Ordered = append(cp.ColumnConstraints.Ordered, ti.ColumnConstraints.Ordered...)
	for i := range ti.ColumnConstraints.Ordered {
		cp.ColumnConstraints.ByColumn[ti.ColumnConstraints.Ordered[i].Column] = ti.ColumnConstraints.Ordered[i]
	}
	cp.TableConstraints = append(cp.TableConstraints, ti.TableConstraints...)
	return &cp
}

type PrimaryKey struct {
	Columns   []string
	Types     []types.Type
	SortOrder tree.SortOrder
}

// IndexInfo holds the configuration of an index.
type IndexInfo struct {
	// namespace of the store associated with the index.
	StoreNamespace tree.Namespace
	IndexName      string
	Columns        []string

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

	for i, p := range idx.Columns {
		if i > 0 {
			s.WriteString(", ")
		}

		// Column
		s.WriteString(p)

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

	c.Columns = make([]string, len(i.Columns))
	copy(c.Columns, i.Columns)

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
	Columns   []string
}

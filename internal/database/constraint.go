package database

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stringutil"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// ColumnConstraint describes constraints on a particular column.
type ColumnConstraint struct {
	Position     int
	Column       string
	Type         types.Type
	IsNotNull    bool
	DefaultValue TableExpression
}

func (f *ColumnConstraint) IsEmpty() bool {
	return f.Column == "" && f.Type.IsAny() && !f.IsNotNull && f.DefaultValue == nil
}

func (f *ColumnConstraint) String() string {
	var s strings.Builder

	s.WriteString(f.Column)
	s.WriteString(" ")
	s.WriteString(strings.ToUpper(f.Type.String()))

	if f.IsNotNull {
		s.WriteString(" NOT NULL")
	}

	if f.DefaultValue != nil {
		s.WriteString(" DEFAULT ")
		s.WriteString(f.DefaultValue.String())
	}

	return s.String()
}

// ColumnConstraints is a list of column constraints.
type ColumnConstraints struct {
	Ordered  []*ColumnConstraint
	ByColumn map[string]*ColumnConstraint
}

func NewColumnConstraints(constraints ...*ColumnConstraint) (ColumnConstraints, error) {
	var fc ColumnConstraints
	for _, c := range constraints {
		if err := fc.Add(c); err != nil {
			return ColumnConstraints{}, err
		}
	}
	return fc, nil
}

func MustNewColumnConstraints(constraints ...*ColumnConstraint) ColumnConstraints {
	fc, err := NewColumnConstraints(constraints...)
	if err != nil {
		panic(err)
	}
	return fc
}

// Add a column constraint to the list. If another constraint exists for the same path
// and they are equal, an error is returned.
func (f *ColumnConstraints) Add(newCc *ColumnConstraint) error {
	if f.ByColumn == nil {
		f.ByColumn = make(map[string]*ColumnConstraint)
	}

	if c, ok := f.ByColumn[newCc.Column]; ok {
		return fmt.Errorf("conflicting constraints: %q and %q: %#v", c.String(), newCc.String(), f.ByColumn)
	}

	// ensure default value type is compatible
	if newCc.DefaultValue != nil {
		// first, try to evaluate the default value
		v, err := newCc.DefaultValue.Eval(nil, nil)
		// if there is no error, check if the default value can be converted to the type of the constraint
		if err == nil {
			_, err = v.CastAs(newCc.Type)
			if err != nil {
				return fmt.Errorf("default value %q cannot be converted to type %q", newCc.DefaultValue, newCc.Type)
			}
		} else {
			// if there is an error, we know we are using a function that returns an integer (like nextval)
			// which is the only one compatible for the moment.
			// Integers can be converted to other integers, doubles, texts and bools.
			// TODO: rework
			switch newCc.Type {
			case types.TypeInteger, types.TypeBigint, types.TypeDoublePrecision, types.TypeText:
			default:
				return fmt.Errorf("default value %q cannot be converted to type %q", newCc.DefaultValue, newCc.Type)
			}
		}
	}

	newCc.Position = len(f.Ordered)
	f.Ordered = append(f.Ordered, newCc)
	f.ByColumn[newCc.Column] = newCc
	return nil
}

func (f ColumnConstraints) GetColumnConstraint(column string) *ColumnConstraint {
	return f.ByColumn[column]
}

type TableExpression interface {
	Eval(tx *Transaction, o row.Row) (types.Value, error)
	Validate(info *TableInfo) error
	String() string
}

// A TableConstraint represent a constraint specific to a table
// and not necessarily to a single column.
type TableConstraint struct {
	Name       string
	Columns    []string
	Check      TableExpression
	Unique     bool
	PrimaryKey bool
	SortOrder  tree.SortOrder
}

func (t *TableConstraint) String() string {
	var sb strings.Builder

	sb.WriteString("CONSTRAINT ")
	sb.WriteString(stringutil.NormalizeIdentifier(t.Name, '"'))

	switch {
	case t.Check != nil:
		sb.WriteString(" CHECK (")
		sb.WriteString(t.Check.String())
		sb.WriteString(")")
	case t.PrimaryKey:
		sb.WriteString(" PRIMARY KEY (")
		for i, c := range t.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(c)

			if t.SortOrder.IsDesc(i) {
				sb.WriteString(" DESC")
			}
		}
		sb.WriteString(")")
	case t.Unique:
		sb.WriteString(" UNIQUE (")
		for i, c := range t.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(c)

			if t.SortOrder.IsDesc(i) {
				sb.WriteString(" DESC")
			}
		}
		sb.WriteString(")")
	}

	return sb.String()
}

// TableConstraints holds the list of CHECK constraints.
type TableConstraints []*TableConstraint

// ValidateRow checks all the table constraint for the given row.
func (t *TableConstraints) ValidateRow(tx *Transaction, r row.Row) error {
	for _, tc := range *t {
		if tc.Check == nil {
			continue
		}

		v, err := tc.Check.Eval(tx, r)
		if err != nil {
			return err
		}
		var ok bool
		switch v.Type() {
		case types.TypeBoolean:
			ok = types.AsBool(v)
		case types.TypeInteger, types.TypeBigint:
			ok = types.AsInt64(v) != 0
		case types.TypeDoublePrecision:
			ok = types.AsFloat64(v) != 0
		case types.TypeNull:
			ok = true
		}

		if !ok {
			return fmt.Errorf("row violates check constraint %q", tc.Name)
		}
	}

	return nil
}

type ConstraintViolationError struct {
	Constraint string
	Columns    []string
	Key        *tree.Key
}

func (c ConstraintViolationError) Error() string {
	return fmt.Sprintf("%s constraint error: %s", c.Constraint, c.Columns)
}

func IsConstraintViolationError(err error) bool {
	return errors.Is(err, (*ConstraintViolationError)(nil))
}

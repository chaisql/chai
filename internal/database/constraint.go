package database

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/stringutil"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// FieldConstraint describes constraints on a particular field.
type FieldConstraint struct {
	Position      int
	Field         string
	Type          types.ValueType
	IsNotNull     bool
	DefaultValue  TableExpression
	AnonymousType *AnonymousType
}

func (f *FieldConstraint) IsEmpty() bool {
	return f.Field == "" && f.Type.IsAny() && !f.IsNotNull && f.DefaultValue == nil
}

func (f *FieldConstraint) String() string {
	var s strings.Builder

	s.WriteString(f.Field)
	if f.Type != types.ObjectValue {
		s.WriteString(" ")
		s.WriteString(strings.ToUpper(f.Type.String()))
	} else if f.AnonymousType != nil {
		s.WriteString(" ")
		s.WriteString(f.AnonymousType.String())
	} else {
		s.WriteString(" OBJECT (...)")
	}

	if f.IsNotNull {
		s.WriteString(" NOT NULL")
	}

	if f.DefaultValue != nil {
		s.WriteString(" DEFAULT ")
		s.WriteString(f.DefaultValue.String())
	}

	return s.String()
}

// FieldConstraints is a list of field constraints.
type FieldConstraints struct {
	Ordered          []*FieldConstraint
	ByField          map[string]*FieldConstraint
	AllowExtraFields bool
}

func NewFieldConstraints(constraints ...*FieldConstraint) (FieldConstraints, error) {
	var fc FieldConstraints
	for _, c := range constraints {
		if err := fc.Add(c); err != nil {
			return FieldConstraints{}, err
		}
	}
	return fc, nil
}

func MustNewFieldConstraints(constraints ...*FieldConstraint) FieldConstraints {
	fc, err := NewFieldConstraints(constraints...)
	if err != nil {
		panic(err)
	}
	return fc
}

// Add a field constraint to the list. If another constraint exists for the same path
// and they are equal, an error is returned.
func (f *FieldConstraints) Add(newFc *FieldConstraint) error {
	if f.ByField == nil {
		f.ByField = make(map[string]*FieldConstraint)
	}

	if c, ok := f.ByField[newFc.Field]; ok {
		return fmt.Errorf("conflicting constraints: %q and %q: %#v", c.String(), newFc.String(), f.ByField)
	}

	// ensure default value type is compatible
	if newFc.DefaultValue != nil && !newFc.Type.IsAny() {
		// first, try to evaluate the default value
		v, err := newFc.DefaultValue.Eval(nil, nil)
		// if there is no error, check if the default value can be converted to the type of the constraint
		if err == nil {
			_, err = object.CastAs(v, newFc.Type)
			if err != nil {
				return fmt.Errorf("default value %q cannot be converted to type %q", newFc.DefaultValue, newFc.Type)
			}
		} else {
			// if there is an error, we know we are using a function that returns an integer (NEXT VALUE FOR)
			// which is the only one compatible for the moment.
			// Integers can be converted to other integers, doubles, texts and bools.
			switch newFc.Type {
			case types.IntegerValue, types.DoubleValue, types.TextValue, types.BooleanValue:
			default:
				return fmt.Errorf("default value %q cannot be converted to type %q", newFc.DefaultValue, newFc.Type)
			}
		}
	}

	newFc.Position = len(f.Ordered)
	f.Ordered = append(f.Ordered, newFc)
	f.ByField[newFc.Field] = newFc
	return nil
}

// ConversionFunc is called when the type of a value is different than the expected type
// and the value needs to be converted.
type ConversionFunc func(v types.Value, path object.Path, targetType types.ValueType) (types.Value, error)

// CastConversion is a ConversionFunc that casts the value to the target type.
func CastConversion(v types.Value, path object.Path, targetType types.ValueType) (types.Value, error) {
	return object.CastAs(v, targetType)
}

// ConvertValueAtPath converts the value using the field constraints that are applicable
// at the given path.
func (f FieldConstraints) ConvertValueAtPath(path object.Path, v types.Value, conversionFn ConversionFunc) (types.Value, error) {
	switch v.Type() {
	case types.ArrayValue:
		vb, err := f.convertArrayAtPath(path, types.As[types.Array](v), conversionFn)
		return types.NewArrayValue(vb), err
	case types.ObjectValue:
		fb, err := f.convertObjectAtPath(path, types.As[types.Object](v), conversionFn)
		return types.NewObjectValue(fb), err
	}
	return f.convertScalarAtPath(path, v, conversionFn)
}

// convert the value using field constraints type information.
// if there is a type constraint on a path, apply it.
// if a value is an integer and has no constraint, convert it to double.
// if a value is a timestamp and has no constraint, convert it to text.
func (f FieldConstraints) convertScalarAtPath(path object.Path, v types.Value, conversionFn ConversionFunc) (types.Value, error) {
	fc := f.GetFieldConstraintForPath(path)
	if fc != nil {
		// check if the constraint enforces a particular type
		// and if so convert the value to the new type.
		if fc.Type != 0 {
			newV, err := conversionFn(v, path, fc.Type)
			if err != nil {
				return v, err
			}

			return newV, nil
		}
	}

	// no constraint have been found for this path.
	// check if this is an integer and convert it to double.
	if v.Type() == types.IntegerValue {
		newV, _ := object.CastAsDouble(v)
		return newV, nil
	}

	if v.Type() == types.TimestampValue {
		newV, _ := object.CastAsText(v)
		return newV, nil
	}

	return v, nil
}

func (f FieldConstraints) GetFieldConstraintForPath(path object.Path) *FieldConstraint {
	cur := f
	for i := range path {
		fc, ok := cur.ByField[path[i].FieldName]
		if !ok {
			break
		}

		if i == len(path)-1 {
			return fc
		}

		if fc.AnonymousType == nil {
			return nil
		}

		cur = fc.AnonymousType.FieldConstraints
	}

	return nil
}

func (f FieldConstraints) convertObjectAtPath(path object.Path, d types.Object, conversionFn ConversionFunc) (*object.FieldBuffer, error) {
	fb, ok := d.(*object.FieldBuffer)
	if !ok {
		fb = object.NewFieldBuffer()
		err := fb.Copy(d)
		if err != nil {
			return nil, err
		}
	}

	err := fb.Apply(func(p object.Path, v types.Value) (types.Value, error) {
		return f.convertScalarAtPath(append(path, p...), v, conversionFn)
	})

	return fb, err
}

func (f FieldConstraints) convertArrayAtPath(path object.Path, a types.Array, conversionFn ConversionFunc) (*object.ValueBuffer, error) {
	vb := object.NewValueBuffer()
	err := vb.Copy(a)
	if err != nil {
		return nil, err
	}

	err = vb.Apply(func(p object.Path, v types.Value) (types.Value, error) {
		return f.convertScalarAtPath(append(path, p...), v, conversionFn)
	})

	return vb, err
}

type TableExpression interface {
	Eval(tx *Transaction, o types.Object) (types.Value, error)
	String() string
}

// A TableConstraint represent a constraint specific to a table
// and not necessarily to a single field path.
type TableConstraint struct {
	Name       string
	Paths      object.Paths
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
		for i, pt := range t.Paths {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(pt.String())

			if t.SortOrder.IsDesc(i) {
				sb.WriteString(" DESC")
			}
		}
		sb.WriteString(")")
	case t.Unique:
		sb.WriteString(" UNIQUE (")
		for i, pt := range t.Paths {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(pt.String())

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

// ValidateRow checks all the table constraints for the given row.
func (t *TableConstraints) ValidateRow(tx *Transaction, r Row) error {
	for _, tc := range *t {
		if tc.Check == nil {
			continue
		}

		v, err := tc.Check.Eval(tx, r.Object())
		if err != nil {
			return err
		}
		var ok bool
		switch v.Type() {
		case types.BooleanValue:
			ok = types.As[bool](v)
		case types.IntegerValue:
			ok = types.As[int64](v) != 0
		case types.DoubleValue:
			ok = types.As[float64](v) != 0
		case types.NullValue:
			ok = true
		}

		if !ok {
			return fmt.Errorf("row violates check constraint %q", tc.Name)
		}
	}

	return nil
}

type AnonymousType struct {
	FieldConstraints FieldConstraints
}

func (an *AnonymousType) AddFieldConstraint(newFc *FieldConstraint) error {
	if an.FieldConstraints.ByField == nil {
		an.FieldConstraints.ByField = make(map[string]*FieldConstraint)
	}

	return an.FieldConstraints.Add(newFc)
}

func (an *AnonymousType) String() string {
	var sb strings.Builder

	sb.WriteString("(")

	hasConstraints := false
	for i, fc := range an.FieldConstraints.Ordered {
		if i > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(fc.String())
		hasConstraints = true
	}

	if an.FieldConstraints.AllowExtraFields {
		if hasConstraints {
			sb.WriteString(", ")
		}
		sb.WriteString("...")
	}

	sb.WriteString(")")

	return sb.String()
}

type ConstraintViolationError struct {
	Constraint string
	Paths      []object.Path
	Key        *tree.Key
}

func (c ConstraintViolationError) Error() string {
	return fmt.Sprintf("%s constraint error: %s", c.Constraint, c.Paths)
}

func IsConstraintViolationError(err error) bool {
	return errors.Is(err, (*ConstraintViolationError)(nil))
}

package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/types"
)

// FieldConstraint describes constraints on a particular field.
type FieldConstraint struct {
	Path         document.Path
	Type         types.ValueType
	IsNotNull    bool
	DefaultValue TableExpression
	IsInferred   bool
	InferredBy   []document.Path
}

// IsEqual compares f with other member by member.
// Inference is not compared.
func (f *FieldConstraint) IsEqual(other *FieldConstraint) bool {
	if !f.Path.IsEqual(other.Path) {
		return false
	}

	if f.Type != other.Type {
		return false
	}

	if f.IsNotNull != other.IsNotNull {
		return false
	}

	if f.HasDefaultValue() != other.HasDefaultValue() {
		return false
	}

	if f.HasDefaultValue() {
		if !f.DefaultValue.IsEqual(other.DefaultValue) {
			return false
		}
	}

	return true
}

func (f *FieldConstraint) IsEmpty() bool {
	return f.Type.IsAny() && !f.IsNotNull && f.DefaultValue == nil
}

func (f *FieldConstraint) String() string {
	var s strings.Builder

	s.WriteString(f.Path.String())
	s.WriteString(" ")
	s.WriteString(strings.ToUpper(f.Type.String()))

	if f.IsNotNull {
		s.WriteString(" NOT NULL")
	}

	if f.HasDefaultValue() {
		s.WriteString(" DEFAULT ")
		s.WriteString(f.DefaultValue.String())
	}

	return s.String()
}

// MergeInferred adds the other.InferredBy to f.InferredBy and ensures there are no duplicates.
func (f *FieldConstraint) MergeInferred(other *FieldConstraint) {
	for _, by := range other.InferredBy {
		duplicate := false
		for _, fby := range f.InferredBy {
			if fby.IsEqual(by) {
				duplicate = true
				break
			}
		}

		if !duplicate {
			f.InferredBy = append(f.InferredBy, by)
		}
	}
}

// HasDefaultValue returns this field contains a default value constraint.
func (f *FieldConstraint) HasDefaultValue() bool {
	return f.DefaultValue != nil
}

// FieldConstraints is a list of field constraints.
type FieldConstraints []*FieldConstraint

// NewFieldConstraints takes user-defined field constraints, validates them, infers additional
// constraints if needed, and returns a valid FieldConstraints type that can be assigned to a table.
func NewFieldConstraints(userConstraints []*FieldConstraint) (FieldConstraints, error) {
	return FieldConstraints(userConstraints).Infer()
}

// Get a field constraint by path. Returns nil if not found.
func (f FieldConstraints) Get(path document.Path) *FieldConstraint {
	for _, fc := range f {
		if fc.Path.IsEqual(path) {
			return fc
		}
	}

	return nil
}

// Infer additional constraints based on user defined ones.
// For example, given the following table:
//   CREATE TABLE foo (
//      a.b[1] TEXT,
//      c.e DEFAULT 10
//    )
// this function will return a TableInfo that behaves as if the table
// had been created like this:
//   CREATE TABLE foo(
//      a DOCUMENT
//      a.b ARRAY
//      a.b[0] TEXT
//      c DOCUMENT DEFAULT {}
//      c.d DEFAULT 10
//   )
func (f FieldConstraints) Infer() (FieldConstraints, error) {
	newConstraints := make(FieldConstraints, 0, len(f))

	for _, fc := range f {
		// loop over all the path fragments and
		// create intermediary inferred constraints.
		if len(fc.Path) > 1 {
			for i := range fc.Path {
				// stop before reaching the last fragment
				// which will be added outside of this loop
				if i+1 == len(fc.Path) {
					break
				}

				newFc := FieldConstraint{
					Path:       fc.Path[:i+1],
					IsInferred: true,
					InferredBy: []document.Path{fc.Path},
				}
				if fc.Path[i+1].FieldName != "" {
					newFc.Type = types.DocumentValue
					if fc.HasDefaultValue() {
						newFc.DefaultValue = &inferredTableExpression{v: types.NewDocumentValue(document.NewFieldBuffer())}
					}
				} else {
					newFc.Type = types.ArrayValue
				}

				err := newConstraints.Add(&newFc)
				if err != nil {
					return nil, err
				}
			}
		}

		// add the non inferred path to the list
		// and ensure there are no conflicts with
		// existing ones.
		err := newConstraints.Add(fc)
		if err != nil {
			return nil, err
		}
	}

	return newConstraints, nil
}

// Add a field constraint to the list. If another constraint exists for the same path
// and they are equal, newFc will be ignored. Otherwise an error will be returned.
// If newFc has been inferred by another constraint and another constraint exists with the same
// path, their InferredBy member will be merged.
func (f *FieldConstraints) Add(newFc *FieldConstraint) error {
	for i, c := range *f {
		if c.Path.IsEqual(newFc.Path) {
			// if both non inferred, they are duplicate
			if !newFc.IsInferred && !c.IsInferred {
				return fmt.Errorf("conflicting constraints: %q and %q", c.String(), newFc.String())
			}

			// determine which one is inferred
			inferredFc, nonInferredFc := c, newFc
			if newFc.IsInferred {
				inferredFc, nonInferredFc = nonInferredFc, inferredFc
			}

			// the inferred one may has less constraints than the user-defined one
			inferredFc.DefaultValue = nonInferredFc.DefaultValue
			inferredFc.IsNotNull = nonInferredFc.IsNotNull
			// inferredFc.IsPrimaryKey = nonInferredFc.IsPrimaryKey

			// detect if constraints are different
			if !c.IsEqual(newFc) {
				return fmt.Errorf("conflicting constraints: %q and %q", c.String(), newFc.String())
			}

			// validate default value
			err := f.validateDefaultValue(newFc)
			if err != nil {
				return err
			}

			// if both inferred, merge the InferredBy member
			if newFc.IsInferred && c.IsInferred {
				c.MergeInferred(newFc)
				return nil
			}

			// if existing one is not inferred, ignore newFc
			if newFc.IsInferred && !c.IsInferred {
				return nil
			}

			// if existing one is inferred, and newFc is not,
			// replace it
			(*f)[i] = newFc
			return nil
		}
	}

	err := f.validateDefaultValue(newFc)
	if err != nil {
		return err
	}

	*f = append(*f, newFc)
	return nil
}

func (f *FieldConstraints) validateDefaultValue(newFc *FieldConstraint) error {
	// ensure there is no default value on array indexes and their child nodes
	if newFc.DefaultValue != nil {
		for _, frag := range newFc.Path {
			// an empty fieldname means this is an array index
			if frag.FieldName == "" {
				return fmt.Errorf("default value is not allowed on array indexes and their child nodes (%q)", newFc.Path)
			}
		}
	}

	// ensure default value type is compatible
	if newFc.DefaultValue != nil && !newFc.Type.IsAny() {
		// first, try to evaluate the default value
		v, err := newFc.DefaultValue.Eval(nil, nil)
		// if there is no error, check if the default value can be converted to the type of the constraint
		if err == nil {
			_, err = document.CastAs(v, newFc.Type)
			if err != nil {
				return fmt.Errorf("default value %q cannot be converted to type %q", newFc.DefaultValue, newFc.Type)
			}
		} else {
			// if there is an error, we know we are using a function that returns an integer (NEXT VALUE FOR)
			// which is the only one compatible for the moment.
			// Integers can be converted to other integers, doubles, texts and bools.
			switch newFc.Type {
			case types.IntegerValue, types.DoubleValue, types.TextValue, types.BoolValue:
			default:
				return fmt.Errorf("default value %q cannot be converted to type %q", newFc.DefaultValue, newFc.Type)
			}
		}
	}

	return nil
}

// ValidateDocument calls Convert then ensures the document validates against the field constraints.
func (f FieldConstraints) ValidateDocument(tx *Transaction, fb *document.FieldBuffer) (*document.FieldBuffer, error) {
	// generate default values for all fields
	for _, fc := range f {
		if fc.DefaultValue == nil {
			continue
		}

		_, err := fc.Path.GetValueFromDocument(fb)
		if err == nil {
			continue
		}

		if !errors.Is(err, types.ErrFieldNotFound) {
			return nil, err
		}

		v, err := fc.DefaultValue.Eval(tx, nil)
		if err != nil {
			return nil, err
		}
		err = fb.Set(fc.Path, v)
		if err != nil {
			return nil, err
		}
	}

	fb, err := f.ConvertDocument(fb)
	if err != nil {
		return nil, err
	}

	// ensure no field is missing
	for _, fc := range f {
		if !fc.IsNotNull {
			continue
		}

		v, err := fc.Path.GetValueFromDocument(fb)
		if err == nil {
			// if field is found, it has already been converted
			// to the right type above.
			// check if it is required but null.
			if v.Type() == types.NullValue {
				return nil, &errs.ConstraintViolationError{Constraint: "NOT NULL", Paths: []document.Path{fc.Path}}
			}

			continue
		}

		if !errors.Is(err, types.ErrFieldNotFound) {
			return nil, err
		}

		return nil, &errs.ConstraintViolationError{Constraint: "NOT NULL", Paths: []document.Path{fc.Path}}
	}

	return fb, nil
}

// ConvertDocument the document using the field constraints.
// It converts any path that has a field constraint on it into the specified type using CAST.
// If there is no constraint on an integer field or value, it converts it into a double.
// Default values on missing fields are not applied.
func (f FieldConstraints) ConvertDocument(d types.Document) (*document.FieldBuffer, error) {
	return f.convertDocumentAtPath(nil, d, CastConversion)
}

// ConversionFunc is called when the type of a value is different than the expected type
// and the value needs to be converted.
type ConversionFunc func(v types.Value, path document.Path, targetType types.ValueType) (types.Value, error)

// CastConversion is a ConversionFunc that casts the value to the target type.
func CastConversion(v types.Value, path document.Path, targetType types.ValueType) (types.Value, error) {
	return document.CastAs(v, targetType)
}

// ConvertValueAtPath converts the value using the field constraints that are applicable
// at the given path.
func (f FieldConstraints) ConvertValueAtPath(path document.Path, v types.Value, conversionFn ConversionFunc) (types.Value, error) {
	switch v.Type() {
	case types.ArrayValue:
		vb, err := f.convertArrayAtPath(path, v.V().(types.Array), conversionFn)
		return types.NewArrayValue(vb), err
	case types.DocumentValue:
		fb, err := f.convertDocumentAtPath(path, v.V().(types.Document), conversionFn)
		return types.NewDocumentValue(fb), err
	}
	return f.convertScalarAtPath(path, v, conversionFn)
}

// convert the value using field constraints type information.
// if there is a type constraint on a path, apply it.
// if a value is an integer and has no constraint, convert it to double.
func (f FieldConstraints) convertScalarAtPath(path document.Path, v types.Value, conversionFn ConversionFunc) (types.Value, error) {
	for _, fc := range f {
		if !fc.Path.IsEqual(path) {
			continue
		}

		// check if the constraint enforce a particular type
		// and if so convert the value to the new type.
		if fc.Type != 0 {
			newV, err := conversionFn(v, fc.Path, fc.Type)
			if err != nil {
				return v, err
			}

			return newV, nil
		}
		break
	}

	// no constraint have been found for this path.
	// check if this is an integer and convert it to double.
	if v.Type() == types.IntegerValue {
		newV, _ := document.CastAsDouble(v)
		return newV, nil
	}

	return v, nil
}

func (f FieldConstraints) convertDocumentAtPath(path document.Path, d types.Document, conversionFn ConversionFunc) (*document.FieldBuffer, error) {
	fb, ok := d.(*document.FieldBuffer)
	if !ok {
		fb = document.NewFieldBuffer()
		err := fb.Copy(d)
		if err != nil {
			return nil, err
		}
	}

	err := fb.Apply(func(p document.Path, v types.Value) (types.Value, error) {
		return f.convertScalarAtPath(append(path, p...), v, conversionFn)
	})

	return fb, err
}

func (f FieldConstraints) convertArrayAtPath(path document.Path, a types.Array, conversionFn ConversionFunc) (*document.ValueBuffer, error) {
	vb := document.NewValueBuffer()
	err := vb.Copy(a)
	if err != nil {
		return nil, err
	}

	err = vb.Apply(func(p document.Path, v types.Value) (types.Value, error) {
		return f.convertScalarAtPath(append(path, p...), v, conversionFn)
	})

	return vb, err
}

type TableExpression interface {
	Bind(catalog *Catalog)
	Eval(tx *Transaction, d types.Document) (types.Value, error)
	IsEqual(other TableExpression) bool
	String() string
}

type inferredTableExpression struct {
	v types.Value
}

func (t *inferredTableExpression) Eval(tx *Transaction, d types.Document) (types.Value, error) {
	return t.v, nil
}

func (t *inferredTableExpression) Bind(catalog *Catalog) {}

func (t *inferredTableExpression) IsEqual(other TableExpression) bool {
	if t == nil {
		return other == nil
	}
	if other == nil {
		return false
	}
	o, ok := other.(*inferredTableExpression)
	if !ok {
		return false
	}
	eq, _ := types.IsEqual(t.v, o.v)
	return eq
}

func (t *inferredTableExpression) String() string {
	return t.v.String()
}

// A TableConstraint represent a constraint specific to a table
// and not necessarily to a single field path.
type TableConstraint struct {
	Name       string
	Paths      document.Paths
	Check      TableExpression
	Unique     bool
	PrimaryKey bool
}

func (t *TableConstraint) String() string {
	if t.Check != nil {
		return fmt.Sprintf("CHECK (%s)", t.Check)
	}

	if t.PrimaryKey {
		return fmt.Sprintf("PRIMARY KEY (%s)", t.Paths)
	}

	if t.Unique {
		return fmt.Sprintf("UNIQUE (%s)", t.Paths)
	}

	return ""
}

// TableConstraints holds the list of CHECK constraints.
type TableConstraints []*TableConstraint

// ValidateDocument checks all the table constraint for the given document.
func (t *TableConstraints) ValidateDocument(tx *Transaction, fb *document.FieldBuffer) error {
	for _, tc := range *t {
		if tc.Check == nil {
			continue
		}

		v, err := tc.Check.Eval(tx, fb)
		if err != nil {
			return err
		}
		var ok bool
		switch v.Type() {
		case types.BoolValue:
			ok = v.V().(bool)
		case types.IntegerValue:
			ok = v.V().(int64) != 0
		case types.DoubleValue:
			ok = v.V().(float64) != 0
		case types.NullValue:
			ok = true
		}

		if !ok {
			return fmt.Errorf("document violates check constraint %q", tc.Name)
		}
	}

	return nil
}

func (t *TableConstraints) AddCheck(tableName string, e TableExpression) {
	var i int
	for _, tc := range *t {
		if tc.Check != nil {
			i++
		}
	}

	name := tableName + "_" + "check"
	if i > 0 {
		name += strconv.Itoa(i)
	}

	*t = append(*t, &TableConstraint{
		Name:  name,
		Check: e,
	})
}

func (t *TableConstraints) AddPrimaryKey(tableName string, p document.Paths) error {
	for _, tc := range *t {
		if tc.PrimaryKey {
			return fmt.Errorf("multiple primary keys for table %q are not allowed", tableName)
		}
	}

	*t = append(*t, &TableConstraint{
		Name:       tableName + "_" + "pk",
		Paths:      p,
		PrimaryKey: true,
	})

	return nil
}

// AddUnique adds a unique constraint to the table.
// If the constraint is already present, it is ignored.
func (t *TableConstraints) AddUnique(tableName string, p document.Paths) {
	for _, tc := range *t {
		if tc.Unique && tc.Paths.IsEqual(p) {
			return
		}
	}

	*t = append(*t, &TableConstraint{
		Name:   fmt.Sprintf("%s_%s_unique", tableName, p.String()),
		Paths:  p,
		Unique: true,
	})
}

func (t *TableConstraints) Merge(other TableConstraints) error {
	for _, tc := range other {
		if tc.PrimaryKey {
			if err := t.AddPrimaryKey(tc.Name, tc.Paths); err != nil {
				return err
			}
		} else if tc.Unique {
			t.AddUnique(tc.Name, tc.Paths)
		} else if tc.Check != nil {
			t.AddCheck(tc.Name, tc.Check)
		}
	}

	return nil
}

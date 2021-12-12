package stream

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// A PathsSetOperator filters duplicate documents.
type PathsSetOperator struct {
	baseOperator
	Path document.Path
	E    expr.Expr
}

// PathsSet filters duplicate documents based on one or more expressions.
func PathsSet(path document.Path, e expr.Expr) *PathsSetOperator {
	return &PathsSetOperator{
		Path: path,
		E:    e,
	}
}

// Iterate implements the Operator interface.
func (op *PathsSetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		v, err := op.E.Eval(out)
		if err != nil && !errors.Is(err, document.ErrFieldNotFound) {
			return err
		}

		fb.Reset()
		err = fb.ScanDocument(d)
		if err != nil {
			return err
		}

		err = fb.Set(op.Path, v)
		if errors.Is(err, document.ErrFieldNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *PathsSetOperator) String() string {
	return stringutil.Sprintf("paths.Set(%s, %s)", op.Path, op.E)
}

// A PathsUnsetOperator filters duplicate documents.
type PathsUnsetOperator struct {
	baseOperator
	Field string
}

// PathsUnset filters duplicate documents based on one or more expressions.
func PathsUnset(field string) *PathsUnsetOperator {
	return &PathsUnsetOperator{
		Field: field,
	}
}

// Iterate implements the Operator interface.
func (op *PathsUnsetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		_, err := d.GetByField(op.Field)
		if err != nil {
			if !errors.Is(err, document.ErrFieldNotFound) {
				return err
			}

			return f(out)
		}

		err = fb.ScanDocument(d)
		if err != nil {
			return err
		}

		err = fb.Delete(document.NewPath(op.Field))
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *PathsUnsetOperator) String() string {
	return stringutil.Sprintf("paths.Unset(%s)", op.Field)
}

// An PathsRenameOperator iterates over all fields of the incoming document in order and renames them.
type PathsRenameOperator struct {
	baseOperator
	FieldNames []string
}

// PathsRename iterates over all fields of the incoming document in order and renames them.
// If the number of fields of the incoming document doesn't match the number of expected fields,
// it returns an error.
func PathsRename(fieldNames ...string) *PathsRenameOperator {
	return &PathsRenameOperator{
		FieldNames: fieldNames,
	}
}

// Iterate implements the Operator interface.
func (op *PathsRenameOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		var i int
		err := d.Iterate(func(field string, value types.Value) error {
			// if there are too many fields in the incoming document
			if i >= len(op.FieldNames) {
				n, err := document.Length(d)
				if err != nil {
					return err
				}
				return stringutil.Errorf("%d values for %d fields", n, len(op.FieldNames))
			}

			fb.Add(op.FieldNames[i], value)
			i++
			return nil
		})
		if err != nil {
			return err
		}

		// if there are too few fields in the incoming document
		if i < len(op.FieldNames) {
			n, err := document.Length(d)
			if err != nil {
				return err
			}
			return stringutil.Errorf("%d values for %d fields", n, len(op.FieldNames))
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *PathsRenameOperator) String() string {
	return stringutil.Sprintf("paths.Rename(%s)", strings.Join(op.FieldNames, ", "))
}

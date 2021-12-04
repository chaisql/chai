package stream

import (
	"strings"

	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// ErrInvalidResult is returned when an expression supposed to evaluate to a document
// returns something else.
var ErrInvalidResult = errors.New("expression must evaluate to a document")

// An Operator is used to modify a stream.
// It takes an environment containing the current value as well as any other metadata
// created by other operatorsand returns a new environment which will be passed to the next operator.
// If it returns a nil environment, the env will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Stream operators can be reused, and thus, any state or side effect should be kept within the Op closure
// unless the nature of the operator prevents that.
type Operator interface {
	Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error
	SetPrev(prev Operator)
	SetNext(next Operator)
	GetNext() Operator
	GetPrev() Operator
	String() string
}

// An OperatorFunc is the function that will receive each value of the stream.
type OperatorFunc func(func(env *environment.Environment) error) error

func Pipe(ops ...Operator) Operator {
	for i := len(ops) - 1; i > 0; i-- {
		ops[i].SetPrev(ops[i-1])
		ops[i-1].SetNext(ops[i])
	}

	return ops[len(ops)-1]
}

type baseOperator struct {
	Prev Operator
	Next Operator
}

func (op *baseOperator) SetPrev(o Operator) {
	op.Prev = o
}

func (op *baseOperator) SetNext(o Operator) {
	op.Next = o
}

func (op *baseOperator) GetPrev() Operator {
	return op.Prev
}

func (op *baseOperator) GetNext() Operator {
	return op.Next
}

// A MapOperator applies an expression on each value of the stream and returns a new value.
type MapOperator struct {
	baseOperator
	E expr.Expr
}

// Map evaluates e on each value of the stream and outputs the result.
func Map(e expr.Expr) *MapOperator {
	return &MapOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *MapOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.E.Eval(out)
		if err != nil {
			return err
		}

		if v.Type() != types.DocumentValue {
			return errors.Wrap(ErrInvalidResult)
		}

		newEnv.SetDocument(v.V().(types.Document))
		newEnv.SetOuter(out)
		return f(&newEnv)
	})
}

func (op *MapOperator) String() string {
	return stringutil.Sprintf("map(%s)", op.E)
}

// A FilterOperator filters values based on a given expression.
type FilterOperator struct {
	baseOperator
	E expr.Expr
}

// Filter evaluates e for each incoming value and filters any value whose result is not truthy.
func Filter(e expr.Expr) *FilterOperator {
	return &FilterOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *FilterOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.E.Eval(out)
		if err != nil {
			return err
		}

		ok, err := types.IsTruthy(v)
		if err != nil || !ok {
			return err
		}

		return f(out)
	})
}

func (op *FilterOperator) String() string {
	return stringutil.Sprintf("filter(%s)", op.E)
}

// A TakeOperator closes the stream after a certain number of values.
type TakeOperator struct {
	baseOperator
	N int64
}

// Take closes the stream after n values have passed through the operator.
func Take(n int64) *TakeOperator {
	return &TakeOperator{N: n}
}

// Iterate implements the Operator interface.
func (op *TakeOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var count int64
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if count < op.N {
			count++
			return f(out)
		}

		return errors.Wrap(ErrStreamClosed)
	})
}

func (op *TakeOperator) String() string {
	return stringutil.Sprintf("take(%d)", op.N)
}

// A SkipOperator skips the n first values of the stream.
type SkipOperator struct {
	baseOperator
	N int64
}

// Skip ignores the first n values of the stream.
func Skip(n int64) *SkipOperator {
	return &SkipOperator{N: n}
}

// Iterate implements the Operator interface.
func (op *SkipOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var skipped int64

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if skipped < op.N {
			skipped++
			return nil
		}

		return f(out)
	})
}

func (op *SkipOperator) String() string {
	return stringutil.Sprintf("skip(%d)", op.N)
}

// A SetOperator filters duplicate documents.
type SetOperator struct {
	baseOperator
	Path document.Path
	E    expr.Expr
}

// Set filters duplicate documents based on one or more expressions.
func Set(path document.Path, e expr.Expr) *SetOperator {
	return &SetOperator{
		Path: path,
		E:    e,
	}
}

// Iterate implements the Operator interface.
func (op *SetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
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

func (op *SetOperator) String() string {
	return stringutil.Sprintf("set(%s, %s)", op.Path, op.E)
}

// A UnsetOperator filters duplicate documents.
type UnsetOperator struct {
	baseOperator
	Field string
}

// Unset filters duplicate documents based on one or more expressions.
func Unset(field string) *UnsetOperator {
	return &UnsetOperator{
		Field: field,
	}
}

// Iterate implements the Operator interface.
func (op *UnsetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
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

func (op *UnsetOperator) String() string {
	return stringutil.Sprintf("unset(%s)", op.Field)
}

// An IterRenameOperator iterates over all fields of the incoming document in order and renames them.
type IterRenameOperator struct {
	baseOperator
	FieldNames []string
}

// IterRename iterates over all fields of the incoming document in order and renames them.
// If the number of fields of the incoming document doesn't match the number of expected fields,
// it returns an error.
func IterRename(fieldNames ...string) *IterRenameOperator {
	return &IterRenameOperator{
		FieldNames: fieldNames,
	}
}

// Iterate implements the Operator interface.
func (op *IterRenameOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
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

func (op *IterRenameOperator) String() string {
	return stringutil.Sprintf("iterRename(%s)", strings.Join(op.FieldNames, ", "))
}

type DoOperator struct {
	baseOperator
	F func(out *environment.Environment) error
}

func Do(f func(out *environment.Environment) error) *DoOperator {
	return &DoOperator{
		F: f,
	}
}

func NoOp() *DoOperator {
	return noOp
}

var noOp = &DoOperator{
	F: doNothing,
}

func doNothing(out *environment.Environment) error {
	return nil
}

func (op *DoOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	if op.Prev == nil {
		return nil
	}

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		err := op.F(out)
		if err != nil {
			return err
		}

		return f(out)
	})
}

func (op *DoOperator) String() string {
	return "do()"
}

type EmitOperator struct {
	baseOperator
	env *environment.Environment
}

func Emit(env *environment.Environment) *EmitOperator {
	return &EmitOperator{
		env: env,
	}
}

func (op *EmitOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	return f(op.env)
}

func (op *EmitOperator) String() string {
	return "emit()"
}

// HandleConflictOperator handles any conflicts that occur during the iteration.
type HandleConflictOperator struct {
	baseOperator

	OnConflict *Stream
}

func HandleConflict(onConflict *Stream) *HandleConflictOperator {
	return &HandleConflictOperator{
		OnConflict: onConflict,
	}
}

func (op *HandleConflictOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		err := fn(out)
		if err != nil {
			if cerr, ok := err.(*errs.ConstraintViolationError); ok {
				newEnv.SetOuter(out)
				newEnv.Set(environment.DocPKKey, types.NewBlobValue(cerr.Key))

				err = New(Emit(&newEnv)).Pipe(op.OnConflict.First()).Iterate(in, func(out *environment.Environment) error { return nil })
			}
		}
		return err
	})
}

func (op *HandleConflictOperator) String() string {
	return stringutil.Sprintf("HandleConflict(%s)", op.OnConflict)
}

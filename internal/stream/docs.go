package stream

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

type DocsEmitOperator struct {
	baseOperator
	Exprs []expr.Expr
}

// DocsEmit creates an operator that iterates over the given expressions.
// Each expression must evaluate to a document.
func DocsEmit(exprs ...expr.Expr) *DocsEmitOperator {
	return &DocsEmitOperator{Exprs: exprs}
}

func (op *DocsEmitOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, e := range op.Exprs {
		v, err := e.Eval(in)
		if err != nil {
			return err
		}
		if v.Type() != types.DocumentValue {
			return errors.WithStack(ErrInvalidResult)
		}

		newEnv.SetDocument(types.As[types.Document](v))
		err = fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *DocsEmitOperator) String() string {
	var sb strings.Builder

	sb.WriteString("docs.Emit(")
	for i, e := range op.Exprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.(fmt.Stringer).String())
	}
	sb.WriteByte(')')

	return sb.String()
}

// A DocsProjectOperator applies an expression on each value of the stream and returns a new value.
type DocsProjectOperator struct {
	baseOperator
	Exprs []expr.Expr
}

// DocsProject creates a ProjectOperator.
func DocsProject(exprs ...expr.Expr) *DocsProjectOperator {
	return &DocsProjectOperator{Exprs: exprs}
}

// Iterate implements the Operator interface.
func (op *DocsProjectOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var mask MaskDocument
	var newEnv environment.Environment

	if op.Prev == nil {
		mask.Env = in
		mask.Exprs = op.Exprs
		newEnv.SetDocument(&mask)
		newEnv.SetOuter(in)
		return f(&newEnv)
	}

	return op.Prev.Iterate(in, func(env *environment.Environment) error {
		mask.Env = env
		mask.Exprs = op.Exprs
		newEnv.SetDocument(&mask)
		newEnv.SetOuter(env)
		return f(&newEnv)
	})
}

func (op *DocsProjectOperator) String() string {
	var b strings.Builder

	b.WriteString("docs.Project(")
	for i, e := range op.Exprs {
		b.WriteString(e.(fmt.Stringer).String())
		if i+1 < len(op.Exprs) {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	return b.String()
}

type MaskDocument struct {
	Env   *environment.Environment
	Exprs []expr.Expr
}

func (d *MaskDocument) GetByField(field string) (v types.Value, err error) {
	for _, e := range d.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			d, ok := d.Env.GetDocument()
			if !ok {
				continue
			}

			v, err = d.GetByField(field)
			if errors.Is(err, types.ErrFieldNotFound) {
				continue
			}
			return
		}

		if ne, ok := e.(*expr.NamedExpr); ok && ne.Name() == field {
			return e.Eval(d.Env)
		}

		if e.(fmt.Stringer).String() == field {
			return e.Eval(d.Env)
		}
	}

	err = types.ErrFieldNotFound
	return
}

func (d *MaskDocument) Iterate(fn func(field string, value types.Value) error) error {
	for _, e := range d.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			d, ok := d.Env.GetDocument()
			if !ok {
				return nil
			}

			err := d.Iterate(fn)
			if err != nil {
				return err
			}

			continue
		}

		var field string
		if ne, ok := e.(*expr.NamedExpr); ok {
			field = ne.Name()
		} else {
			field = e.(fmt.Stringer).String()
		}

		v, err := e.Eval(d.Env)
		if err != nil {
			return err
		}

		err = fn(field, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *MaskDocument) String() string {
	b, _ := types.NewDocumentValue(d).MarshalText()
	return string(b)
}

func (d *MaskDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(d)
}

// A DocsFilterOperator filters values based on a given expression.
type DocsFilterOperator struct {
	baseOperator
	Expr expr.Expr
}

// DocsFilter evaluates e for each incoming value and filters any value whose result is not truthy.
func DocsFilter(e expr.Expr) *DocsFilterOperator {
	return &DocsFilterOperator{Expr: e}
}

// Iterate implements the Operator interface.
func (op *DocsFilterOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.Expr.Eval(out)
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

func (op *DocsFilterOperator) String() string {
	return fmt.Sprintf("docs.Filter(%s)", op.Expr)
}

// A DocsTakeOperator closes the stream after a certain number of values.
type DocsTakeOperator struct {
	baseOperator
	E expr.Expr
}

// DocsTake closes the stream after n values have passed through the operator.
func DocsTake(e expr.Expr) *DocsTakeOperator {
	return &DocsTakeOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *DocsTakeOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	v, err := op.E.Eval(in)
	if err != nil {
		return err
	}

	if !v.Type().IsNumber() {
		return fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type())
	}

	v, err = document.CastAsInteger(v)
	if err != nil {
		return err
	}

	n := types.As[int64](v)
	var count int64
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if count < n {
			count++
			return f(out)
		}

		return errors.WithStack(ErrStreamClosed)
	})
}

func (op *DocsTakeOperator) String() string {
	return fmt.Sprintf("docs.Take(%s)", op.E)
}

// A DocsSkipOperator skips the n first values of the stream.
type DocsSkipOperator struct {
	baseOperator
	E expr.Expr
}

// DocsSkip ignores the first n values of the stream.
func DocsSkip(e expr.Expr) *DocsSkipOperator {
	return &DocsSkipOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *DocsSkipOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	v, err := op.E.Eval(in)
	if err != nil {
		return err
	}

	if !v.Type().IsNumber() {
		return fmt.Errorf("offset expression must evaluate to a number, got %q", v.Type())
	}

	v, err = document.CastAsInteger(v)
	if err != nil {
		return err
	}

	n := types.As[int64](v)
	var skipped int64

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if skipped < n {
			skipped++
			return nil
		}

		return f(out)
	})
}

func (op *DocsSkipOperator) String() string {
	return fmt.Sprintf("docs.Skip(%s)", op.E)
}

type DocsGroupAggregateOperator struct {
	baseOperator
	Builders []expr.AggregatorBuilder
	E        expr.Expr
}

// DocsGroupAggregate consumes the incoming stream and outputs one value per group.
// It assumes the stream is sorted by groupBy.
func DocsGroupAggregate(groupBy expr.Expr, builders ...expr.AggregatorBuilder) *DocsGroupAggregateOperator {
	return &DocsGroupAggregateOperator{E: groupBy, Builders: builders}
}

func (op *DocsGroupAggregateOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var lastGroup types.Value
	var ga *groupAggregator

	var groupExpr string
	if op.E != nil {
		groupExpr = op.E.String()
	}

	err := op.Prev.Iterate(in, func(out *environment.Environment) error {
		if op.E == nil {
			if ga == nil {
				ga = newGroupAggregator(nil, groupExpr, op.Builders)
			}

			return ga.Aggregate(out)
		}

		group, err := op.E.Eval(out)
		if err != nil {
			return err
		}

		// handle the first document of the stream
		if lastGroup == nil {
			lastGroup, err = document.CloneValue(group)
			if err != nil {
				return err
			}
			ga = newGroupAggregator(lastGroup, groupExpr, op.Builders)
			return ga.Aggregate(out)
		}

		ok, err := types.IsEqual(lastGroup, group)
		if err != nil {
			return err
		}
		if ok {
			return ga.Aggregate(out)
		}

		// if the document is from a different group, we flush the previous group, emit it and start a new group
		e, err := ga.Flush(out)
		if err != nil {
			return err
		}
		err = f(e)
		if err != nil {
			return err
		}

		lastGroup, err = document.CloneValue(group)
		if err != nil {
			return err
		}

		ga = newGroupAggregator(lastGroup, groupExpr, op.Builders)
		return ga.Aggregate(out)
	})
	if err != nil {
		return err
	}

	// if s is empty, we create a default group so that aggregators will
	// return their default initial value.
	// Ex: For `SELECT COUNT(*) FROM foo`, if `foo` is empty
	// we want the following result:
	// {"COUNT(*)": 0}
	if ga == nil {
		ga = newGroupAggregator(nil, "", op.Builders)
	}

	e, err := ga.Flush(in)
	if err != nil {
		return err
	}
	return f(e)
}

func (op *DocsGroupAggregateOperator) String() string {
	var sb strings.Builder

	sb.WriteString("docs.GroupAggregate(")
	if op.E != nil {
		sb.WriteString(op.E.String())
	} else {
		sb.WriteString("NULL")
	}

	for _, agg := range op.Builders {
		sb.WriteString(", ")
		sb.WriteString(agg.(fmt.Stringer).String())
	}

	sb.WriteString(")")
	return sb.String()
}

// a groupAggregator is an aggregator for a whole group of documents.
// It applies all the aggregators for each documents and returns a new document with the
// result of the aggregation.
type groupAggregator struct {
	group       types.Value
	groupExpr   string
	aggregators []expr.Aggregator
}

func newGroupAggregator(group types.Value, groupExpr string, builders []expr.AggregatorBuilder) *groupAggregator {
	newAggregators := make([]expr.Aggregator, len(builders))
	for i, b := range builders {
		newAggregators[i] = b.Aggregator()
	}

	return &groupAggregator{
		aggregators: newAggregators,
		group:       group,
		groupExpr:   groupExpr,
	}
}

func (g *groupAggregator) Aggregate(env *environment.Environment) error {
	for _, agg := range g.aggregators {
		err := agg.Aggregate(env)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *groupAggregator) Flush(env *environment.Environment) (*environment.Environment, error) {
	fb := document.NewFieldBuffer()

	// add the current group to the document
	if g.groupExpr != "" {
		fb.Add(g.groupExpr, g.group)
	}

	for _, agg := range g.aggregators {
		v, err := agg.Eval(env)
		if err != nil {
			return nil, err
		}
		fb.Add(fmt.Sprintf("%s", agg), v)
	}

	var newEnv environment.Environment
	newEnv.SetOuter(env)
	newEnv.SetDocument(fb)

	return &newEnv, nil
}

// A DocsTempTreeSortOperator consumes every value of the stream and outputs them in order.
type DocsTempTreeSortOperator struct {
	baseOperator
	Expr expr.Expr
	Desc bool
}

// DocsTempTreeSort consumes every value of the stream, sorts them by the given expr and outputs them in order.
// It creates a temporary index and uses it to sort the stream.
func DocsTempTreeSort(e expr.Expr) *DocsTempTreeSortOperator {
	return &DocsTempTreeSortOperator{Expr: e}
}

// DocsTempTreeSortReverse does the same as TempTreeSort but in descending order.
func DocsTempTreeSortReverse(e expr.Expr) *DocsTempTreeSortOperator {
	return &DocsTempTreeSortOperator{Expr: e, Desc: true}
}

func (op *DocsTempTreeSortOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	db := in.GetDB()

	catalog := in.GetCatalog()
	tns := catalog.GetFreeTransientNamespace()
	tr, cleanup, err := tree.NewTransient(db.DB, tns)
	if err != nil {
		return err
	}
	defer cleanup()

	var counter int64

	var buf []byte
	err = op.Prev.Iterate(in, func(out *environment.Environment) error {
		buf = buf[:0]
		// evaluate the sort expression
		v, err := op.Expr.Eval(out)
		if err != nil {
			return err
		}

		doc, ok := out.GetDocument()
		if !ok {
			panic("missing document")
		}

		tableName, _ := out.Get(environment.TableKey)

		var encKey []byte
		key, ok := out.GetKey()
		if ok {
			encKey = key.Encoded
		}

		tk := tree.NewKey(v, tableName, types.NewBlobValue(encKey), types.NewIntegerValue(counter))

		counter++

		buf, err = encoding.EncodeDocument(buf, doc)
		if err != nil {
			return err
		}
		return tr.Put(tk, buf)
	})
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	return tr.IterateOnRange(nil, op.Desc, func(k *tree.Key, data []byte) error {
		kv, err := k.Decode()
		if err != nil {
			return err
		}

		tableName := kv[1]
		if tableName.Type() != types.NullValue {
			newEnv.Set(environment.TableKey, tableName)
		}

		docKey := kv[2]
		if docKey.Type() != types.NullValue {
			newEnv.SetKey(tree.NewEncodedKey(types.As[[]byte](docKey)))
		}

		newEnv.SetDocument(encoding.DecodeDocument(data, false /* intAsDouble */))

		return fn(&newEnv)
	})
}

func (op *DocsTempTreeSortOperator) String() string {
	if op.Desc {
		return fmt.Sprintf("docs.TempTreeSortReverse(%s)", op.Expr)
	}

	return fmt.Sprintf("docs.TempTreeSort(%s)", op.Expr)
}

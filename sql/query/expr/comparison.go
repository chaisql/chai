package expr

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/index"
	"github.com/genjidb/genji/sql/scanner"
)

// A cmpOp is a comparison operator.
type cmpOp struct {
	*simpleOperator
}

// newCmpOp creates a comparison operator.
func newCmpOp(a, b Expr, t scanner.Token) *cmpOp {
	return &cmpOp{&simpleOperator{a, b, t}}
}

type eqOp struct {
	*cmpOp
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) Expr {
	return eqOp{newCmpOp(a, b, scanner.EQ)}
}

var errStop = errors.New("errStop")

func (op eqOp) IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error {
	err := idx.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
		ok, err := v.IsEqual(val)
		if err != nil {
			return err
		}

		if ok {
			r, err := tb.GetDocument(key)
			if err != nil {
				return err
			}

			return fn(r)
		}

		return errStop
	})

	if err != nil && err != errStop {
		return err
	}

	return nil
}

func (op eqOp) IteratePK(tb *database.Table, v document.Value, pkType document.ValueType, fn func(d document.Document) error) error {
	v, err := v.CastAs(pkType)
	if err != nil {
		return nil
	}

	data, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}

	val, err := tb.Store.Get(data)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return nil
		}

		return err
	}
	return fn(msgpack.EncodedDocument(val))
}

func (op eqOp) String() string {
	return fmt.Sprintf("%v = %v", op.a, op.b)
}

type neqOp struct {
	*cmpOp
}

// Neq creates an expression that returns true if a equals b.
func Neq(a, b Expr) Expr {
	return neqOp{newCmpOp(a, b, scanner.NEQ)}
}

func (op neqOp) String() string {
	return fmt.Sprintf("%v != %v", op.a, op.b)
}

type gtOp struct {
	*cmpOp
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) Expr {
	return gtOp{newCmpOp(a, b, scanner.GT)}
}

func (op gtOp) IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error {
	err := idx.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
		ok, err := v.IsEqual(val)
		if err != nil {
			return err
		}

		if ok {
			return nil
		}

		r, err := tb.GetDocument(key)
		if err != nil {
			return err
		}

		return fn(r)
	})

	if err != nil && err != errStop {
		return err
	}

	return nil
}

func (op gtOp) IteratePK(tb *database.Table, v document.Value, pkType document.ValueType, fn func(d document.Document) error) error {
	v, err := v.CastAs(pkType)
	if err != nil {
		return err
	}

	var d msgpack.EncodedDocument

	data, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}

	it := tb.Store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	for it.Seek(data); it.Valid(); it.Next() {
		d, err = it.Item().ValueCopy(d)
		if err != nil {
			return err
		}
		if bytes.Equal(data, d) {
			return nil
		}

		err = fn(&d)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op gtOp) String() string {
	return fmt.Sprintf("%v > %v", op.a, op.b)
}

type gteOp struct {
	*cmpOp
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return gteOp{newCmpOp(a, b, scanner.GTE)}
}

func (op gteOp) IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error {
	err := idx.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
		r, err := tb.GetDocument(key)
		if err != nil {
			return err
		}

		return fn(r)
	})

	if err != nil && err != errStop {
		return err
	}

	return nil
}

func (op gteOp) IteratePK(tb *database.Table, v document.Value, pkType document.ValueType, fn func(d document.Document) error) error {
	v, err := v.CastAs(pkType)
	if err != nil {
		return err
	}

	var d msgpack.EncodedDocument

	data, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}

	it := tb.Store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	for it.Seek(data); it.Valid(); it.Next() {
		d, err = it.Item().ValueCopy(d)
		if err != nil {
			return err
		}

		err = fn(&d)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op gteOp) String() string {
	return fmt.Sprintf("%v >= %v", op.a, op.b)
}

type ltOp struct {
	*cmpOp
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) Expr {
	return ltOp{newCmpOp(a, b, scanner.LT)}
}

func (op ltOp) IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error {
	err := idx.AscendGreaterOrEqual(index.EmptyPivot(v.Type), func(val document.Value, key []byte) error {
		ok, err := v.IsLesserThanOrEqual(val)
		if err != nil {
			return err
		}

		if ok {
			return errStop
		}

		r, err := tb.GetDocument(key)
		if err != nil {
			return err
		}

		return fn(r)
	})

	if err != nil && err != errStop {
		return err
	}

	return nil
}

func (op ltOp) IteratePK(tb *database.Table, v document.Value, pkType document.ValueType, fn func(d document.Document) error) error {
	v, err := v.CastAs(pkType)
	if err != nil {
		return err
	}

	var d msgpack.EncodedDocument

	data, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}

	it := tb.Store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	for it.Seek(nil); it.Valid(); it.Next() {
		d, err = it.Item().ValueCopy(d)
		if err != nil {
			return err
		}
		if bytes.Compare(data, d) <= 0 {
			break
		}

		err = fn(&d)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op ltOp) String() string {
	return fmt.Sprintf("%v < %v", op.a, op.b)
}

type lteOp struct {
	*cmpOp
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) Expr {
	return lteOp{newCmpOp(a, b, scanner.LTE)}
}

func (op lteOp) IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error {
	err := idx.AscendGreaterOrEqual(index.EmptyPivot(v.Type), func(val document.Value, key []byte) error {
		ok, err := v.IsLesserThan(val)
		if err != nil {
			return err
		}

		if ok {
			return errStop
		}

		r, err := tb.GetDocument(key)
		if err != nil {
			return err
		}

		return fn(r)
	})

	if err != nil && err != errStop {
		return err
	}

	return nil
}

func (op lteOp) IteratePK(tb *database.Table, v document.Value, pkType document.ValueType, fn func(d document.Document) error) error {
	v, err := v.CastAs(pkType)
	if err != nil {
		return err
	}

	var d msgpack.EncodedDocument

	data, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}

	it := tb.Store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	for it.Seek(nil); it.Valid(); it.Next() {
		d, err = it.Item().ValueCopy(d)
		if err != nil {
			return err
		}
		if bytes.Compare(data, d) < 0 {
			break
		}

		err = fn(&d)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op lteOp) String() string {
	return fmt.Sprintf("%v <= %v", op.a, op.b)
}

// Eval compares a and b together using the operator specified when constructing the CmpOp
// and returns the result of the comparison.
// Comparing with NULL always evaluates to NULL.
func (op cmpOp) Eval(ctx EvalStack) (document.Value, error) {
	v1, v2, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return falseLitteral, err
	}

	if v1.Type == document.NullValue || v2.Type == document.NullValue {
		return nullLitteral, nil
	}

	ok, err := op.compare(v1, v2)
	if ok {
		return trueLitteral, err
	}

	return falseLitteral, err
}

func (op cmpOp) compare(l, r document.Value) (bool, error) {
	switch op.Tok {
	case scanner.EQ:
		return l.IsEqual(r)
	case scanner.NEQ:
		return l.IsNotEqual(r)
	case scanner.GT:
		return l.IsGreaterThan(r)
	case scanner.GTE:
		return l.IsGreaterThanOrEqual(r)
	case scanner.LT:
		return l.IsLesserThan(r)
	case scanner.LTE:
		return l.IsLesserThanOrEqual(r)
	default:
		panic(fmt.Sprintf("unknown token %v", op.Tok))
	}
}

// IsComparisonOperator returns true if e is one of
// =, !=, >, >=, <, <=, IS, IS NOT, IN, or NOT IN operators.
func IsComparisonOperator(op Operator) bool {
	switch op.(type) {
	case eqOp, neqOp, gtOp, gteOp, ltOp, lteOp,
		isOp, isNotOp, inOp, notInOp:
		return true
	}

	return false
}

// IsAndOperator reports if e is the AND operator.
func IsAndOperator(op Operator) bool {
	_, ok := op.(*AndOp)
	return ok
}

// IsOrOperator reports if e is the OR operator.
func IsOrOperator(e Expr) bool {
	_, ok := e.(*OrOp)
	return ok
}

// IsInOperator reports if e is the IN operator.
func IsInOperator(e Expr) bool {
	_, ok := e.(inOp)
	return ok
}

type inOp struct {
	*simpleOperator
}

// In creates an expression that evaluates to the result of a IN b.
func In(a, b Expr) Expr {
	return inOp{&simpleOperator{a, b, scanner.IN}}
}

func (op inOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	if a.Type == document.NullValue || b.Type == document.NullValue {
		return nullLitteral, nil
	}

	if b.Type != document.ArrayValue {
		return falseLitteral, nil
	}

	ok, err := document.ArrayContains(b.V.(document.Array), a)
	if err != nil {
		return nullLitteral, err
	}

	if ok {
		return trueLitteral, nil
	}
	return falseLitteral, nil
}

func (op inOp) IterateIndex(idx index.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error {
	if v.Type != document.ArrayValue {
		return errors.New("IN operator takes an array")
	}

	var eq eqOp
	return v.V.(document.Array).Iterate(func(i int, value document.Value) error {
		return eq.IterateIndex(idx, tb, value, fn)
	})
}

// IteratePK implements the query.pkIterator interface. It expects v to be an array,
// iterates over it, and for each value, gets it from the underlying store of tb.
func (op inOp) IteratePK(tb *database.Table, v document.Value, pkType document.ValueType, fn func(d document.Document) error) error {
	if v.Type != document.ArrayValue {
		return errors.New("IN operator takes an array")
	}

	var d msgpack.EncodedDocument

	return v.V.(document.Array).Iterate(func(i int, value document.Value) error {
		val, err := value.CastAs(pkType)
		if err != nil {
			return nil
		}

		data, err := encoding.EncodeValue(val)
		if err != nil {
			return err
		}

		d, err = tb.Store.Get(data)
		if err != nil {
			if err == engine.ErrKeyNotFound {
				return nil
			}

			return err
		}
		return fn(d)
	})
}

func (op inOp) String() string {
	return fmt.Sprintf("%v IN %v", op.a, op.b)
}

type notInOp struct {
	inOp
}

// NotIn creates an expression that evaluates to the result of a NOT IN b.
func NotIn(a, b Expr) Expr {
	return &notInOp{inOp{&simpleOperator{a, b, scanner.IN}}}
}

func (op notInOp) Eval(ctx EvalStack) (document.Value, error) {
	v, err := op.inOp.Eval(ctx)
	if err != nil {
		return v, err
	}
	if v == trueLitteral {
		return falseLitteral, nil
	}
	if v == falseLitteral {
		return trueLitteral, nil
	}
	return v, nil
}

func (op notInOp) String() string {
	return fmt.Sprintf("%v NOT IN %v", op.a, op.b)
}

type isOp struct {
	*simpleOperator
}

// Is creates an expression that evaluates to the result of a IS b.
func Is(a, b Expr) Expr {
	return &isOp{&simpleOperator{a, b, scanner.IN}}
}

func (op isOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	ok, err := a.IsEqual(b)
	if err != nil {
		return nullLitteral, err
	}
	if ok {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}

func (op isOp) String() string {
	return fmt.Sprintf("%v IS %v", op.a, op.b)
}

type isNotOp struct {
	*simpleOperator
}

// IsNot creates an expression that evaluates to the result of a IS NOT b.
func IsNot(a, b Expr) Expr {
	return &isNotOp{&simpleOperator{a, b, scanner.IN}}
}

func (op isNotOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	ok, err := a.IsNotEqual(b)
	if err != nil {
		return nullLitteral, err
	}
	if ok {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}

func (op isNotOp) String() string {
	return fmt.Sprintf("%v IS NOT %v", op.a, op.b)
}

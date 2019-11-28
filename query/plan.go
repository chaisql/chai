package query

import (
	"bytes"
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/scanner"
	"github.com/asdine/genji/value"
)

type queryPlan struct {
	scanTable bool
	field     *queryPlanField
}

type queryPlanField struct {
	indexedField FieldSelector
	op           scanner.Token
	e            Expr
	uniqueIndex  bool
	isPrimaryKey bool
}

func newQueryOptimizer(tx *database.Transaction, t *database.Table) queryOptimizer {
	return queryOptimizer{
		tx: tx,
		t:  t,
	}
}

// queryOptimizer is a really dumb query optimizer. gotta start somewhere. please don't be mad at me.
type queryOptimizer struct {
	tx        *database.Transaction
	t         *database.Table
	whereExpr Expr
	args      []driver.NamedValue
	cfg       *database.TableConfig
	indexes   map[string]database.Index
}

func (qo *queryOptimizer) optimizeQuery() (document.Stream, error) {
	qp := qo.buildQueryPlan()
	if qp.scanTable {
		return document.NewStream(qo.t), nil
	}

	if qp.field.isPrimaryKey {
		return document.NewStream(pkIterator{
			tx:   qo.tx,
			tb:   qo.t,
			cfg:  qo.cfg,
			args: qo.args,
			op:   qp.field.op,
			e:    qp.field.e,
		}), nil
	}

	return document.NewStream(indexIterator{
		tx:    qo.tx,
		tb:    qo.t,
		args:  qo.args,
		op:    qp.field.op,
		e:     qp.field.e,
		index: qo.indexes[qp.field.indexedField.Name()],
	}), nil
}

func (qo *queryOptimizer) buildQueryPlan() queryPlan {
	var qp queryPlan

	qp.field = qo.analyseExpr(qo.whereExpr)
	if qp.field == nil {
		qp.scanTable = true
	}

	return qp
}

func (qo *queryOptimizer) analyseExpr(e Expr) *queryPlanField {
	switch t := e.(type) {
	case CmpOp:
		ok, fs, e := cmpOpCanUseIndex(&t)
		if !ok || !evaluatesToScalarOrParam(e) {
			return nil
		}

		idx, ok := qo.indexes[fs.Name()]
		if ok {
			return &queryPlanField{
				indexedField: fs,
				op:           t.Token,
				e:            e,
				uniqueIndex:  idx.Unique,
			}
		}

		if qo.cfg.PrimaryKeyName == fs.Name() {
			return &queryPlanField{
				indexedField: fs,
				op:           t.Token,
				e:            e,
				uniqueIndex:  true,
				isPrimaryKey: true,
			}
		}

		return nil

	case *AndOp:
		nodeL := qo.analyseExpr(t.LeftHand())
		nodeR := qo.analyseExpr(t.LeftHand())

		if nodeL == nil && nodeR == nil {
			return nil
		}

		if nodeL != nil && nodeL.uniqueIndex {
			return nodeL
		}

		if nodeR != nil && nodeR.uniqueIndex {
			return nodeR
		}

		return nodeL
	}

	return nil
}

func cmpOpCanUseIndex(cmp *CmpOp) (bool, FieldSelector, Expr) {
	switch cmp.Token {
	case scanner.EQ, scanner.GT, scanner.GTE, scanner.LT, scanner.LTE:
	default:
		return false, "", nil
	}

	lf, leftIsField := cmp.LeftHand().(FieldSelector)
	rf, rightIsField := cmp.RightHand().(FieldSelector)

	// field OP expr
	if leftIsField && !rightIsField {
		return true, lf, cmp.RightHand()
	}

	// expr OP field
	if rightIsField && !leftIsField {
		return true, rf, cmp.LeftHand()
	}

	return false, "", nil
}

func evaluatesToScalarOrParam(e Expr) bool {
	switch e.(type) {
	case LiteralValue:
		return true
	case NamedParam, PositionalParam:
		return true
	}

	return false
}

type indexIterator struct {
	tx    *database.Transaction
	tb    *database.Table
	args  []driver.NamedValue
	index index.Index
	op    scanner.Token
	e     Expr
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(r document.Document) error) error {
	v, err := it.e.Eval(EvalStack{
		Tx:     it.tx,
		Params: it.args,
	})
	if err != nil {
		return err
	}

	if v.IsList {
		return errors.New("expression doesn't evaluate to scalar")
	}

	var data []byte
	if value.IsNumber(v.Value.Type) {
		x, err := v.Value.DecodeToFloat64()
		if err != nil {
			return err
		}

		data = value.NewFloat64(x).Data
	} else {
		data = v.Value.Data
	}

	switch it.op {
	case scanner.EQ:
		err = it.index.AscendGreaterOrEqual(&v.Value.Value, func(val value.Value, key []byte) error {
			if bytes.Equal(data, val.Data) {
				r, err := it.tb.GetRecord(key)
				if err != nil {
					return err
				}

				return fn(r)
			}

			return errStop
		})
	case scanner.GT:
		err = it.index.AscendGreaterOrEqual(&v.Value.Value, func(val value.Value, key []byte) error {
			if bytes.Equal(data, val.Data) {
				return nil
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.GTE:
		err = it.index.AscendGreaterOrEqual(&v.Value.Value, func(val value.Value, key []byte) error {
			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LT:
		err = it.index.AscendGreaterOrEqual(index.EmptyPivot(v.Value.Type), func(val value.Value, key []byte) error {
			if bytes.Compare(data, val.Data) <= 0 {
				return errStop
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LTE:
		err = it.index.AscendGreaterOrEqual(index.EmptyPivot(v.Value.Type), func(val value.Value, key []byte) error {
			if bytes.Compare(data, val.Data) < 0 {
				return errStop
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	}

	if err != nil && err != errStop {
		return err
	}

	return nil
}

type pkIterator struct {
	tx   *database.Transaction
	tb   *database.Table
	cfg  *database.TableConfig
	args []driver.NamedValue
	op   scanner.Token
	e    Expr
}

func (it pkIterator) Iterate(fn func(r document.Document) error) error {
	v, err := it.e.Eval(EvalStack{
		Tx:     it.tx,
		Params: it.args,
	})
	if err != nil {
		return err
	}

	if v.IsList {
		return errors.New("expression doesn't evaluate to scalar")
	}

	data := v.Value.Value.Data
	if value.IsNumber(v.Value.Type) {
		vv, err := v.Value.ConvertTo(it.cfg.PrimaryKeyType)
		if err != nil {
			return err
		}
		data = vv.Data
	}

	switch it.op {
	case scanner.EQ:
		val, err := it.tb.Store.Get(data)
		if err != nil {
			if err == engine.ErrKeyNotFound {
				return nil
			}

			return err
		}
		return fn(document.EncodedRecord(val))
	case scanner.GT:
		err = it.tb.Store.AscendGreaterOrEqual(v.Value.Data, func(key, val []byte) error {
			if bytes.Equal(data, val) {
				return nil
			}

			return fn(document.EncodedRecord(val))
		})
	case scanner.GTE:
		err = it.tb.Store.AscendGreaterOrEqual(data, func(key, val []byte) error {
			return fn(document.EncodedRecord(val))
		})
	case scanner.LT:
		err = it.tb.Store.AscendGreaterOrEqual(nil, func(key, val []byte) error {
			if bytes.Compare(data, val) <= 0 {
				return errStop
			}

			return fn(document.EncodedRecord(val))
		})
	case scanner.LTE:
		err = it.tb.Store.AscendGreaterOrEqual(nil, func(key, val []byte) error {
			if bytes.Compare(data, val) < 0 {
				return errStop
			}

			return fn(document.EncodedRecord(val))
		})
	}

	if err != nil && err != errStop {
		return err
	}

	return nil
}

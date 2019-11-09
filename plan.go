package genji

import (
	"bytes"
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

type queryPlan struct {
	scanTable bool
	field     *queryPlanField
}

type queryPlanField struct {
	indexedField fieldSelector
	op           scanner.Token
	e            expr
	uniqueIndex  bool
	isPrimaryKey bool
}

func newQueryOptimizer(tx *Tx, t *Table) queryOptimizer {
	return queryOptimizer{
		tx: tx,
		t:  t,
	}
}

// queryOptimizer is a really dumb query optimizer. gotta start somewhere. please don't be mad at me.
type queryOptimizer struct {
	tx        *Tx
	t         *Table
	stat      parserStat
	whereExpr expr
	args      []driver.NamedValue
	cfg       *TableConfig
	indexes   map[string]Index
}

func (qo *queryOptimizer) optimizeQuery() (record.Stream, error) {
	qp := qo.buildQueryPlan()
	if qp.scanTable {
		return record.NewStream(qo.t), nil
	}

	if qp.field.isPrimaryKey {
		return record.NewStream(pkIterator{
			tx:   qo.tx,
			tb:   qo.t,
			cfg:  qo.cfg,
			args: qo.args,
			op:   qp.field.op,
			e:    qp.field.e,
		}), nil
	}

	return record.NewStream(indexIterator{
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

func (qo *queryOptimizer) analyseExpr(e expr) *queryPlanField {
	switch t := e.(type) {
	case cmpOp:
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

	case *andOp:
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

func cmpOpCanUseIndex(cmp *cmpOp) (bool, fieldSelector, expr) {
	lf, leftIsField := cmp.LeftHand().(fieldSelector)
	rf, rightIsField := cmp.RightHand().(fieldSelector)

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

func evaluatesToScalarOrParam(e expr) bool {
	switch e.(type) {
	case litteralValue:
		return true
	case namedParam, positionalParam:
		return true
	}

	return false
}

type indexIterator struct {
	tx    *Tx
	tb    *Table
	args  []driver.NamedValue
	index index.Index
	op    scanner.Token
	e     expr
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(r record.Record) error) error {
	v, err := it.e.Eval(evalStack{
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
		err = it.index.AscendGreaterOrEqual(v.Value.Value, func(val value.Value, key []byte) error {
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
		err = it.index.AscendGreaterOrEqual(v.Value.Value, func(val value.Value, key []byte) error {
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
		err = it.index.AscendGreaterOrEqual(v.Value.Value, func(val value.Value, key []byte) error {
			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LT:
		err = it.index.DescendLessOrEqual(v.Value.Value, func(val value.Value, key []byte) error {
			if bytes.Equal(data, val.Data) {
				return nil
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LTE:
		err = it.index.DescendLessOrEqual(v.Value.Value, func(val value.Value, key []byte) error {
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
	tx   *Tx
	tb   *Table
	cfg  *TableConfig
	args []driver.NamedValue
	op   scanner.Token
	e    expr
}

func (it pkIterator) Iterate(fn func(r record.Record) error) error {
	v, err := it.e.Eval(evalStack{
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
		vv, err := v.Value.DecodeTo(it.cfg.PrimaryKeyType)
		if err != nil {
			return err
		}
		data = vv.Data
	}

	switch it.op {
	case scanner.EQ:
		val, err := it.tb.store.Get(v.Value.Data)
		if err != nil {
			if err == engine.ErrKeyNotFound {
				return nil
			}

			return err
		}
		return fn(record.EncodedRecord(val))
	case scanner.GT:
		err = it.tb.store.AscendGreaterOrEqual(v.Value.Data, func(key, val []byte) error {
			if bytes.Equal(data, val) {
				return nil
			}

			return fn(record.EncodedRecord(val))
		})
	case scanner.GTE:
		err = it.tb.store.AscendGreaterOrEqual(data, func(key, val []byte) error {
			return fn(record.EncodedRecord(val))
		})
	case scanner.LT:
		err = it.tb.store.AscendGreaterOrEqual(nil, func(key, val []byte) error {
			if bytes.Compare(data, val) <= 0 {
				return errStop
			}

			return fn(record.EncodedRecord(val))
		})
	case scanner.LTE:
		err = it.tb.store.AscendGreaterOrEqual(nil, func(key, val []byte) error {
			if bytes.Compare(data, val) < 0 {
				return errStop
			}

			return fn(record.EncodedRecord(val))
		})
	}

	if err != nil && err != errStop {
		return err
	}

	return nil
}

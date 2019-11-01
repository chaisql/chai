package genji

import (
	"bytes"
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/index"
	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

type queryPlan struct {
	scanTable bool
	tree      *queryPlanNode
}

type queryPlanNode struct {
	indexedField fieldSelector
	op           scanner.Token
	e            expr
	uniqueIndex  bool
}

func newQueryOptimizer(tx *Tx, t *Table) queryOptimizer {
	return queryOptimizer{
		tx: tx,
		t:  t,
	}
}

// queryOptimizer is a really dumb query optimizer. gotta start somewhere. please don't be mad at me.
type queryOptimizer struct {
	tx *Tx
	t  *Table
}

func (qo queryOptimizer) optimizeQuery(whereExpr expr, args []driver.NamedValue) (record.Stream, error) {
	indexes, err := qo.t.Indexes()
	if err != nil {
		return record.Stream{}, err
	}

	qp := buildQueryPlan(indexes, whereExpr)
	if qp.scanTable {
		return record.NewStream(qo.t), nil
	}

	return record.NewStream(indexIterator{
		tx:    qo.tx,
		tb:    qo.t,
		args:  args,
		op:    qp.tree.op,
		e:     qp.tree.e,
		index: indexes[qp.tree.indexedField.Name()],
	}), nil
}

func buildQueryPlan(indexes map[string]Index, e expr) queryPlan {
	var qp queryPlan

	qp.tree = analyseExpr(indexes, e)
	if qp.tree == nil {
		qp.scanTable = true
	}

	return qp
}

func analyseExpr(indexes map[string]Index, e expr) *queryPlanNode {
	switch t := e.(type) {
	case cmpOp:
		ok, fs, e := cmpOpCanUseIndex(&t)
		if !ok || !evaluatesToScalarOrParam(e) {
			return nil
		}

		idx, ok := indexes[fs.Name()]
		if !ok {
			return nil
		}

		return &queryPlanNode{
			indexedField: fs,
			op:           t.Token,
			e:            e,
			uniqueIndex:  idx.Unique,
		}
	case *andOp:
		nodeL := analyseExpr(indexes, t.LeftHand())
		nodeR := analyseExpr(indexes, t.LeftHand())

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
		cmp.RightHand()
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
			if bytes.Equal(v.Value.Data, val.Data) {
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

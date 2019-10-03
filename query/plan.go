package query

import (
	"bytes"
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/scanner"
	"github.com/asdine/genji/record"
)

type queryPlan struct {
	scanTable bool
	tree      *queryPlanNode
}

type queryPlanNode struct {
	indexedField FieldSelector
	op           scanner.Token
	e            expr.Expr
	uniqueIndex  bool
}

func newQueryOptimizer(tx *database.Tx, ts TableSelector) queryOptimizer {
	return queryOptimizer{
		tx: tx,
		ts: ts,
	}
}

// queryOptimizer is a really dumb query optimizer. gotta start somewhere. please don't be mad at me.
type queryOptimizer struct {
	tx *database.Tx
	ts TableSelector
}

func (qo queryOptimizer) optimizeQuery(whereExpr expr.Expr, args []driver.NamedValue) (TableSelector, error) {
	tb, err := qo.tx.GetTable(qo.ts.TableName())
	if err != nil {
		return nil, err
	}

	indexes, err := tb.Indexes()
	if err != nil {
		return nil, err
	}

	qp := buildQueryPlan(indexes, whereExpr)
	if qp.scanTable {
		return qo.ts, nil
	}

	return indexTableSelector{
		TableSelector: qo.ts,
		args:          args,
		op:            qp.tree.op,
		e:             qp.tree.e,
		index:         indexes[qp.tree.indexedField.Name()],
	}, nil
}

func buildQueryPlan(indexes map[string]index.Index, e expr.Expr) queryPlan {
	var qp queryPlan

	qp.tree = analyseExpr(indexes, e)
	if qp.tree == nil {
		qp.scanTable = true
	}

	return qp
}

func analyseExpr(indexes map[string]index.Index, e expr.Expr) *queryPlanNode {
	switch t := e.(type) {
	case expr.CmpOp:
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
			uniqueIndex:  idx.Config().Unique,
		}
	case *expr.AndOp:
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

func cmpOpCanUseIndex(cmp *expr.CmpOp) (bool, FieldSelector, expr.Expr) {
	lf, leftIsField := cmp.LeftHand().(FieldSelector)
	rf, rightIsField := cmp.RightHand().(FieldSelector)

	// field OP expr
	if leftIsField && !rightIsField {
		cmp.RightHand()
		return true, lf, cmp.RightHand()
	}

	// expr OP field
	if rightIsField && !leftIsField {
		return true, rf, cmp.LeftHand()
	}

	return false, nil, nil
}

func evaluatesToScalarOrParam(e expr.Expr) bool {
	switch e.(type) {
	case expr.LitteralValue:
		return true
	case expr.NamedParam, expr.PositionalParam:
		return true
	}

	return false
}

type indexTableSelector struct {
	TableSelector
	args  []driver.NamedValue
	index index.Index
	op    scanner.Token
	e     expr.Expr
}

func (i indexTableSelector) SelectTable(tx *database.Tx) (record.Iterator, error) {
	tb, err := tx.GetTable(i.TableSelector.TableName())
	if err != nil {
		return nil, err
	}

	return indexIterator{
		tx:    tx,
		tb:    tb,
		args:  i.args,
		index: i.index,
		op:    i.op,
		e:     i.e,
	}, nil
}

type indexIterator struct {
	tx    *database.Tx
	tb    *database.Table
	args  []driver.NamedValue
	index index.Index
	op    scanner.Token
	e     expr.Expr
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(r record.Record) error) error {
	v, err := it.e.Eval(expr.EvalStack{
		Tx:     it.tx,
		Params: it.args,
	})
	if err != nil {
		return err
	}

	lv, ok := v.(expr.LitteralValue)
	if !ok {
		return errors.New("expression doesn't evaluate to scalar")
	}

	switch it.op {
	case scanner.EQ:
		err = it.index.AscendGreaterOrEqual(lv.Data, func(value []byte, key []byte) error {
			if bytes.Equal(lv.Data, value) {
				r, err := it.tb.GetRecord(key)
				if err != nil {
					return err
				}

				return fn(r)
			}

			return errStop
		})
	case scanner.GT:
		err = it.index.AscendGreaterOrEqual(lv.Data, func(value []byte, key []byte) error {
			if bytes.Equal(lv.Data, value) {
				return nil
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.GTE:
		err = it.index.AscendGreaterOrEqual(lv.Data, func(value []byte, key []byte) error {
			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LT:
		err = it.index.DescendLessOrEqual(lv.Data, func(value []byte, key []byte) error {
			if bytes.Equal(lv.Data, value) {
				return nil
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LTE:
		err = it.index.DescendLessOrEqual(lv.Data, func(value []byte, key []byte) error {
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

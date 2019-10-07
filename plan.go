package genji

import (
	"bytes"
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/scanner"
)

type queryPlan struct {
	scanTable bool
	tree      *queryPlanNode
}

type queryPlanNode struct {
	indexedField FieldSelector
	op           scanner.Token
	e            Expr
	uniqueIndex  bool
}

func newQueryOptimizer(tx *Tx, ts TableSelector) queryOptimizer {
	return queryOptimizer{
		tx: tx,
		ts: ts,
	}
}

// queryOptimizer is a really dumb query optimizer. gotta start somewhere. please don't be mad at me.
type queryOptimizer struct {
	tx *Tx
	ts TableSelector
}

func (qo queryOptimizer) optimizeQuery(whereExpr Expr, args []driver.NamedValue) (TableSelector, error) {
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

func buildQueryPlan(indexes map[string]index.Index, e Expr) queryPlan {
	var qp queryPlan

	qp.tree = analyseExpr(indexes, e)
	if qp.tree == nil {
		qp.scanTable = true
	}

	return qp
}

func analyseExpr(indexes map[string]index.Index, e Expr) *queryPlanNode {
	switch t := e.(type) {
	case CmpOp:
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
	case *AndOp:
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

func cmpOpCanUseIndex(cmp *CmpOp) (bool, FieldSelector, Expr) {
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

	return false, "", nil
}

func evaluatesToScalarOrParam(e Expr) bool {
	switch e.(type) {
	case LitteralValue:
		return true
	case NamedParam, PositionalParam:
		return true
	}

	return false
}

type indexTableSelector struct {
	TableSelector
	args  []driver.NamedValue
	index index.Index
	op    scanner.Token
	e     Expr
}

func (i indexTableSelector) SelectTable(tx *Tx) (record.Iterator, error) {
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
	tx    *Tx
	tb    *Table
	args  []driver.NamedValue
	index index.Index
	op    scanner.Token
	e     Expr
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(r record.Record) error) error {
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

	switch it.op {
	case scanner.EQ:
		err = it.index.AscendGreaterOrEqual(v.Value.Data, func(value []byte, key []byte) error {
			if bytes.Equal(v.Value.Data, value) {
				r, err := it.tb.GetRecord(key)
				if err != nil {
					return err
				}

				return fn(r)
			}

			return errStop
		})
	case scanner.GT:
		err = it.index.AscendGreaterOrEqual(v.Value.Data, func(value []byte, key []byte) error {
			if bytes.Equal(v.Value.Data, value) {
				return nil
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.GTE:
		err = it.index.AscendGreaterOrEqual(v.Value.Data, func(value []byte, key []byte) error {
			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LT:
		err = it.index.DescendLessOrEqual(v.Value.Data, func(value []byte, key []byte) error {
			if bytes.Equal(v.Value.Data, value) {
				return nil
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LTE:
		err = it.index.DescendLessOrEqual(v.Value.Data, func(value []byte, key []byte) error {
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

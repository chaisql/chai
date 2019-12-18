package query

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/sql/scanner"
)

type queryPlan struct {
	scanTable bool
	field     *queryPlanField
	sorted    bool
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
	tx               *database.Transaction
	t                *database.Table
	whereExpr        Expr
	args             []driver.NamedValue
	cfg              *database.TableConfig
	indexes          map[string]database.Index
	orderBy          FieldSelector
	orderByDirection scanner.Token
}

func (qo *queryOptimizer) optimizeQuery() (st document.Stream, cleanup func() error, err error) {
	qp := qo.buildQueryPlan()

	switch {
	case qp.scanTable:
		st = document.NewStream(qo.t)
	case qp.field.isPrimaryKey:
		st = document.NewStream(pkIterator{
			tx:               qo.tx,
			tb:               qo.t,
			cfg:              qo.cfg,
			args:             qo.args,
			op:               qp.field.op,
			e:                qp.field.e,
			orderByDirection: qo.orderByDirection,
		})
	default:
		st = document.NewStream(indexIterator{
			tx:               qo.tx,
			tb:               qo.t,
			args:             qo.args,
			op:               qp.field.op,
			e:                qp.field.e,
			index:            qo.indexes[qp.field.indexedField.Name()],
			orderByDirection: qo.orderByDirection,
		})
	}

	if len(qo.orderBy) != 0 && !qp.sorted {
		st, cleanup, err = sortIterator(qo.tx, st, document.ValuePath(qo.orderBy), qo.orderByDirection)
	}

	return
}

func (qo *queryOptimizer) buildQueryPlan() queryPlan {
	var qp queryPlan

	qp.field = qo.analyseExpr(qo.whereExpr)
	if qp.field == nil {
		if len(qo.orderBy) != 0 {
			_, ok := qo.indexes[qo.orderBy.Name()]
			if ok || qo.cfg.PrimaryKeyName == qo.orderBy.Name() {
				qp.field = &queryPlanField{
					indexedField: qo.orderBy,
					isPrimaryKey: qo.cfg.PrimaryKeyName == qo.orderBy.Name(),
				}
				qp.sorted = true

				return qp
			}
		}

		qp.scanTable = true
	}

	return qp
}

// analyseExpr is a recursive function that scans each node the e Expr tree.
// If it contains a comparison operator, it checks if this operator and its operands
// can benefit from using an index. This check is done in the cmpOpCanUseIndex function.
// If it contains an AND operator it checks if one of the operands can use an index.
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
		return false, nil, nil
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

	return false, nil, nil
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
	tx               *database.Transaction
	tb               *database.Table
	args             []driver.NamedValue
	index            index.Index
	op               scanner.Token
	e                Expr
	orderByDirection scanner.Token
}

var errStop = errors.New("stop")

func (it indexIterator) Iterate(fn func(d document.Document) error) error {
	if it.e == nil {
		var err error

		if it.orderByDirection == scanner.DESC {
			err = it.index.DescendLessOrEqual(nil, func(val document.Value, key []byte) error {
				r, err := it.tb.GetRecord(key)
				if err != nil {
					return err
				}

				return fn(r)
			})
		} else {
			err = it.index.AscendGreaterOrEqual(nil, func(val document.Value, key []byte) error {
				r, err := it.tb.GetRecord(key)
				if err != nil {
					return err
				}

				return fn(r)
			})
		}

		return err
	}

	v, err := it.e.Eval(EvalStack{
		Tx:     it.tx,
		Params: it.args,
	})
	if err != nil {
		return err
	}

	if v.Type.IsNumber() {
		v, err = v.ConvertTo(document.Float64Value)
		if err != nil {
			return err
		}
	}

	switch it.op {
	case scanner.EQ:
		err = it.index.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
			ok, err := v.IsEqual(val)
			if err != nil {
				return err
			}

			if ok {
				r, err := it.tb.GetRecord(key)
				if err != nil {
					return err
				}

				return fn(r)
			}

			return errStop
		})
	case scanner.GT:
		err = it.index.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
			ok, err := v.IsEqual(val)
			if err != nil {
				return err
			}

			if ok {
				return nil
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.GTE:
		err = it.index.AscendGreaterOrEqual(&index.Pivot{Value: v}, func(val document.Value, key []byte) error {
			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LT:
		err = it.index.AscendGreaterOrEqual(index.EmptyPivot(v.Type), func(val document.Value, key []byte) error {
			ok, err := v.IsLesserThanOrEqual(val)
			if err != nil {
				return err
			}

			if ok {
				return errStop
			}

			r, err := it.tb.GetRecord(key)
			if err != nil {
				return err
			}

			return fn(r)
		})
	case scanner.LTE:
		err = it.index.AscendGreaterOrEqual(index.EmptyPivot(v.Type), func(val document.Value, key []byte) error {
			ok, err := v.IsLesserThan(val)
			if err != nil {
				return err
			}

			if ok {
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
	tx               *database.Transaction
	tb               *database.Table
	cfg              *database.TableConfig
	args             []driver.NamedValue
	op               scanner.Token
	e                Expr
	orderByDirection scanner.Token
}

func (it pkIterator) Iterate(fn func(d document.Document) error) error {
	if it.e == nil {
		var err error

		if it.orderByDirection == scanner.DESC {
			err = it.tb.Store.DescendLessOrEqual(nil, func(k []byte, v []byte) error {
				return fn(encoding.EncodedDocument(v))
			})
		} else {
			err = it.tb.Store.AscendGreaterOrEqual(nil, func(k []byte, v []byte) error {
				return fn(encoding.EncodedDocument(v))
			})
		}

		return err
	}

	v, err := it.e.Eval(EvalStack{
		Tx:     it.tx,
		Params: it.args,
	})
	if err != nil {
		return err
	}

	if v.Type.IsNumber() {
		v, err = v.ConvertTo(it.cfg.PrimaryKeyType)
		if err != nil {
			return err
		}
	}

	data, err := encoding.EncodeValue(v)
	if err != nil {
		return err
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
		return fn(encoding.EncodedDocument(val))
	case scanner.GT:
		err = it.tb.Store.AscendGreaterOrEqual(data, func(key, val []byte) error {
			if bytes.Equal(data, val) {
				return nil
			}

			return fn(encoding.EncodedDocument(val))
		})
	case scanner.GTE:
		err = it.tb.Store.AscendGreaterOrEqual(data, func(key, val []byte) error {
			return fn(encoding.EncodedDocument(val))
		})
	case scanner.LT:
		err = it.tb.Store.AscendGreaterOrEqual(nil, func(key, val []byte) error {
			if bytes.Compare(data, val) <= 0 {
				return errStop
			}

			return fn(encoding.EncodedDocument(val))
		})
	case scanner.LTE:
		err = it.tb.Store.AscendGreaterOrEqual(nil, func(key, val []byte) error {
			if bytes.Compare(data, val) < 0 {
				return errStop
			}

			return fn(encoding.EncodedDocument(val))
		})
	}

	if err != nil && err != errStop {
		return err
	}

	return nil
}

func sortIterator(tx *database.Transaction, it document.Iterator, path document.ValuePath, direction scanner.Token) (st document.Stream, cleanup func() error, err error) {
	err = tx.Promote()
	if err != nil {
		return
	}

	tempIdxName := fmt.Sprintf("__genji.temp_%d", time.Now().UTC().UnixNano())
	idx := index.NewListIndex(tx.Tx, tempIdxName)

	err = it.Iterate(func(d document.Document) error {
		v, err := path.GetValue(d)
		if err != nil && err != document.ErrFieldNotFound {
			return err
		}

		if err == document.ErrFieldNotFound {
			v = document.NewNullValue()
		}

		data, err := encoding.EncodeDocument(d)
		if err != nil {
			return err
		}

		return idx.Set(v, data)
	})
	if err != nil {
		idx.Truncate()
		return
	}

	st = document.NewStream(&sortedIterator{idx, direction})
	cleanup = idx.Truncate

	return
}

type sortedIterator struct {
	idx       index.Index
	direction scanner.Token
}

func (s *sortedIterator) Iterate(fn func(d document.Document) error) error {
	if s.direction == scanner.DESC {
		return s.idx.DescendLessOrEqual(nil, func(_ document.Value, data []byte) error {
			return fn(encoding.EncodedDocument(data))
		})
	}

	return s.idx.AscendGreaterOrEqual(nil, func(_ document.Value, data []byte) error {
		return fn(encoding.EncodedDocument(data))
	})
}

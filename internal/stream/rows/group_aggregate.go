package rows

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

type GroupAggregateOperator struct {
	stream.BaseOperator
	Builders []expr.AggregatorBuilder
	E        expr.Expr
}

// GroupAggregate consumes the incoming stream and outputs one value per group.
// It assumes the stream is sorted by the groupBy expression.
func GroupAggregate(groupBy expr.Expr, builders ...expr.AggregatorBuilder) *GroupAggregateOperator {
	return &GroupAggregateOperator{E: groupBy, Builders: builders}
}

func (op *GroupAggregateOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	var groupExpr string
	if op.E != nil {
		groupExpr = op.E.String()
	}

	return &GroupAggregatorIterator{
		prev:      prev,
		builders:  op.Builders,
		e:         op.E,
		env:       in,
		groupExpr: groupExpr,
	}, nil
}

func (op *GroupAggregateOperator) Columns(env *environment.Environment) ([]string, error) {
	columns := make([]string, 0, len(op.Builders)+1)
	if op.E != nil {
		columns = append(columns, op.E.String())
	}

	for _, agg := range op.Builders {
		columns = append(columns, agg.String())
	}

	return columns, nil
}

func (op *GroupAggregateOperator) String() string {
	var sb strings.Builder

	sb.WriteString("rows.GroupAggregate(")
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

// a groupAggregator is an aggregator for a whole group of objects.
// It applies all the aggregators for each objects and returns a new object with the
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

func (g *groupAggregator) Flush(env *environment.Environment) (*database.BasicRow, error) {
	cb := row.NewColumnBuffer()

	// add the current group to the object
	if g.groupExpr != "" {
		cb.Add(g.groupExpr, g.group)
	}

	for _, agg := range g.aggregators {
		v, err := agg.Eval(env)
		if err != nil {
			return nil, err
		}
		cb.Add(agg.String(), v)
	}

	var br database.BasicRow
	br.ResetWith("", nil, cb)
	return &br, nil
}

type GroupAggregatorIterator struct {
	prev stream.Iterator

	builders []expr.AggregatorBuilder
	e        expr.Expr
	env      *environment.Environment

	err       error
	row       database.Row
	lastGroup types.Value
	ga        *groupAggregator
	groupExpr string
	done      bool
}

func (it *GroupAggregatorIterator) Close() error {
	return it.prev.Close()
}

func (it *GroupAggregatorIterator) Next() bool {
	it.err = nil

	if it.done {
		return false
	}

	hasMore, err := it.iterateOnPrev()
	if err != nil {
		it.err = err
		return false
	}

	if hasMore {
		return true
	}

	it.done = true

	// if ga is empty, we create a default group so that aggregators will
	// return their default initial value.
	// Ex: For `SELECT COUNT(*) FROM foo`, if `foo` is empty
	// we want the following result:
	// {"COUNT(*)": 0}
	if it.ga == nil {
		it.ga = newGroupAggregator(nil, "", it.builders)
	}

	it.row, it.err = it.ga.Flush(it.env)
	return it.err == nil && it.row != nil
}

func (it *GroupAggregatorIterator) iterateOnPrev() (bool, error) {
	for it.prev.Next() {
		r, err := it.prev.Row()
		if err != nil {
			return false, err
		}

		env := it.env.Clone(r)

		if it.e == nil {
			if it.ga == nil {
				it.ga = newGroupAggregator(nil, it.groupExpr, it.builders)
			}

			err = it.ga.Aggregate(env)
			if err != nil {
				return false, err
			}

			continue
		}

		group, err := it.e.Eval(env)
		if errors.Is(err, types.ErrColumnNotFound) {
			group = types.NewNullValue()
			err = nil
		}
		if err != nil {
			return false, err
		}

		// handle the first object of the stream
		if it.lastGroup == nil {
			it.lastGroup = group
			it.ga = newGroupAggregator(it.lastGroup, it.groupExpr, it.builders)
			err = it.ga.Aggregate(env)
			if err != nil {
				return false, err
			}

			continue
		}

		ok, err := it.lastGroup.EQ(group)
		if err != nil {
			return false, err
		}
		if ok {
			err = it.ga.Aggregate(env)
			if err != nil {
				return false, err
			}

			continue
		}

		// if the object is from a different group, we flush the previous group, emit it and start a new group
		it.row, err = it.ga.Flush(env)
		if err != nil {
			return false, err
		}
		it.lastGroup = group

		it.ga = newGroupAggregator(it.lastGroup, it.groupExpr, it.builders)
		err = it.ga.Aggregate(env)
		if err != nil {
			return false, err
		}

		return true, err
	}

	return false, nil
}

func (it *GroupAggregatorIterator) Row() (database.Row, error) {
	return it.row, it.Error()
}

func (it *GroupAggregatorIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.prev.Error()
}

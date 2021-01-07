package stream

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// A HashAggregateOperator consumes the given stream and outputs one value per group.
// It reads the _group variable from the environment to determine witch group
// to assign each value. If no _group variable is available, it will assume all
// values are part of the same group and aggregate them into one value.
type HashAggregateOperator struct {
	Aggregators []HashAggregator
}

// A HashAggregator is an expression that
type HashAggregator interface {
	expr.Expr

	Aggregate(env *expr.Environment) error
	Name() string
	// Clone creates a new aggregator will its internal state initialized.
	Clone() HashAggregator
}

// HashAggregate consumes the incoming stream and outputs one value per group.
// It reads the _group variable from the environment to determine whitch group
// to assign each value. If no _group variable is available, it will assume all
// values are part of the same group and aggregate them into one value.
// HashAggregate assumes that the stream is not sorted per group and uses a hash map
// to group aggregates per _group value.
func HashAggregate(aggregators ...HashAggregator) *HashAggregateOperator {
	return &HashAggregateOperator{Aggregators: aggregators}
}

// Pipe stores s in the operator and return a new Stream with the HashAggregate operator appended. It implements the Piper interface.
func (op *HashAggregateOperator) Pipe(s Stream) Stream {
	return Stream{
		it: IteratorFunc(func(env *expr.Environment, fn func(env *expr.Environment) error) error {
			return op.iterate(s, env, fn)
		}),
	}
}

// Op implements the Operator interface but should never be called by Stream.
func (op *HashAggregateOperator) Op() (OperatorFunc, error) {
	return func(env *expr.Environment) (*expr.Environment, error) {
		return env, nil
	}, nil
}

func (op *HashAggregateOperator) iterate(s Stream, env *expr.Environment, fn func(env *expr.Environment) error) error {
	encGroup, err := newGroupEncoder()
	if err != nil {
		return err
	}

	// keep order of groups as they arrive to provide deterministic results.
	var encGroupNames []string

	// store a groupAggregator per group
	aggregators := make(map[string]*groupAggregator)

	// iterate over s and for each group, aggregate the incoming document
	err = s.Iterate(env, func(env *expr.Environment) error {
		// we extract the group name from the environment and encode it
		// to be used as a key to the aggregators map.
		groupName, err := encGroup(env)
		if err != nil {
			return err
		}

		// get the group aggregator from the map or create a new one.
		a, ok := aggregators[groupName]
		if !ok {
			a = newGroupAggregator(env, op.Aggregators)
			aggregators[groupName] = a
			encGroupNames = append(encGroupNames, groupName)
		}

		// call the aggregator for that group and aggregate the document.
		return a.Aggregate(env)
	})
	if err != nil {
		return err
	}

	// if s was empty, the aggregators map will be empty as well.
	// if so, we create one default group so that aggregators will
	// return their default initial value.
	// Ex: For `SELECT COUNT(*) FROM foo`, if `foo` is empty
	// we want the following result:
	// {"COUNT(*)": 0}
	if len(aggregators) == 0 {
		aggregators["_"] = newGroupAggregator(nil, op.Aggregators)
		encGroupNames = append(encGroupNames, "_")
	}

	// we loop over the groups in the order they arrived.
	for _, groupName := range encGroupNames {
		r := aggregators[groupName]
		e, err := r.Flush(env)
		if err != nil {
			return err
		}
		err = fn(e)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *HashAggregateOperator) String() string {
	var sb strings.Builder

	for i, agg := range op.Aggregators {
		sb.WriteString(agg.Name())
		if i+1 < len(op.Aggregators) {
			sb.WriteString(", ")
		}
	}

	return fmt.Sprintf("aggregate(%s)", sb.String())
}

// newGroupEncoder returns a function that encodes the _group environment variable using a document.ValueEncoder.
// If the _group variable doesn't exist, the group is set to null.
func newGroupEncoder() (func(env *expr.Environment) (string, error), error) {
	var b bytes.Buffer
	enc := document.NewValueEncoder(&b)
	nullValue := document.NewNullValue()
	err := enc.Encode(nullValue)
	if err != nil {
		return nil, err
	}
	nullGroupName := b.String()
	b.Reset()

	return func(env *expr.Environment) (string, error) {
		groupValue, ok := env.Get(document.NewPath(groupEnvKey))
		if !ok {
			return nullGroupName, nil
		}

		b.Reset()
		err := enc.Encode(groupValue)
		if err != nil {
			return "", err
		}

		return b.String(), nil
	}, nil
}

// a groupAggregator is an aggregator for a whole group of documents.
// It applies all the aggregators for each documents and returns a new document with the
// result of the aggregation.
type groupAggregator struct {
	env         *expr.Environment
	aggregators []HashAggregator
}

func newGroupAggregator(outerEnv *expr.Environment, aggregators []HashAggregator) *groupAggregator {
	var env expr.Environment
	env.Outer = outerEnv

	newAggregators := make([]HashAggregator, len(aggregators))
	for i, agg := range aggregators {
		newAggregators[i] = agg.Clone()
	}

	return &groupAggregator{
		env:         &env,
		aggregators: newAggregators,
	}
}

func (g *groupAggregator) Aggregate(env *expr.Environment) error {
	for _, agg := range g.aggregators {
		err := agg.Aggregate(env)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *groupAggregator) Flush(env *expr.Environment) (*expr.Environment, error) {
	fb := document.NewFieldBuffer()

	for _, agg := range g.aggregators {
		v, err := agg.Eval(env)
		if err != nil {
			return nil, err
		}
		fb.Add(agg.Name(), v)
	}

	var newEnv expr.Environment
	newEnv.Outer = env
	newEnv.SetDocument(fb)
	return &newEnv, nil
}

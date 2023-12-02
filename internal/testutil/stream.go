package testutil

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

type ResultStream struct {
	*parser.Parser
	env *environment.Environment
}

func (ds *ResultStream) Next() (types.Value, error) {
	exp, err := ds.Parser.ParseObject()
	if err != nil {
		return nil, err
	}

	return exp.Eval(ds.env)
}

func ParseResultStream(stream string) *ResultStream {
	p := parser.NewParser(strings.NewReader(stream))
	env := environment.New(nil)

	return &ResultStream{p, env}
}

func RequireStreamEq(t *testing.T, raw string, res *chai.Result, sorted bool) {
	t.Helper()
	RequireStreamEqf(t, raw, res, sorted, "")
}

func RequireStreamEqf(t *testing.T, raw string, res *chai.Result, sorted bool, msg string, args ...any) {
	t.Helper()
	objs := ParseResultStream(raw)

	want := object.NewValueBuffer()

	for {
		v, err := objs.Next()
		if err != nil {
			if perr, ok := err.(*parser.ParseError); ok {
				if perr.Found == "EOF" {
					break
				}
			} else if perr, ok := errors.Unwrap(err).(*parser.ParseError); ok {
				if perr.Found == "EOF" {
					break
				}
			}
		}
		require.NoError(t, err, append([]any{msg}, args...)...)

		v, err = object.CloneValue(v)
		require.NoError(t, err, append([]any{msg}, args...)...)
		want.Append(v)
	}

	got := object.NewValueBuffer()

	err := res.Iterate(func(r *chai.Row) error {
		var fb object.FieldBuffer
		err := fb.Copy(r.Object())
		assert.NoError(t, err)

		got.Append(types.NewObjectValue(&fb))
		return nil
	})
	assert.NoError(t, err)

	if sorted {
		swant := sortableValueBuffer(*want)
		sgot := sortableValueBuffer(*got)
		sort.Sort(&swant)
		sort.Sort(&sgot)
	}

	expected, err := types.MarshalTextIndent(types.NewArrayValue(want), "\n", "  ")
	assert.NoError(t, err)

	actual, err := types.MarshalTextIndent(types.NewArrayValue(got), "\n", "  ")
	assert.NoError(t, err)

	if msg != "" {
		require.Equal(t, string(expected), string(actual), append([]any{msg}, args...)...)
	} else {
		require.Equal(t, string(expected), string(actual))
	}
}

type sortableValueBuffer object.ValueBuffer

func (vb *sortableValueBuffer) Len() int {
	return len(vb.Values)
}

func (vb *sortableValueBuffer) Swap(i, j int) {
	vb.Values[i], vb.Values[j] = vb.Values[j], vb.Values[i]
}

func (vb *sortableValueBuffer) Less(i, j int) (ok bool) {
	it, jt := vb.Values[i].Type(), vb.Values[j].Type()
	if it == jt || (it.IsNumber() && jt.IsNumber()) {
		// TODO(asdine) make the types package work with static objects
		// to avoid having to deal with errors?
		ok, _ = types.IsLesserThan(vb.Values[i], vb.Values[j])
		return
	}

	return it < jt
}

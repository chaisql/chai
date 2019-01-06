package q_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query/q"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func createRecord(age int) record.Record {

	var fb record.FieldBuffer

	fb.Add(field.NewInt64("age", int64(age)))

	return &fb
}

func TestMatchers(t *testing.T) {
	type matcher interface {
		Match(record.Record) (bool, error)
	}

	tests := []struct {
		name    string
		record  record.Record
		matcher matcher
		match   bool
	}{
		{"eq", createRecord(10), q.EqInt(q.Field("age"), 10), true},
		{"gt/10>10", createRecord(10), q.GtInt(q.Field("age"), 10), false},
		{"gt/10>11", createRecord(10), q.GtInt(q.Field("age"), 11), false},
		{"gt/10>9", createRecord(10), q.GtInt(q.Field("age"), 9), true},
		{"gte/10>=10", createRecord(10), q.GteInt(q.Field("age"), 10), true},
		{"gte/10>=11", createRecord(10), q.GteInt(q.Field("age"), 11), false},
		{"gte/10>=9", createRecord(10), q.GteInt(q.Field("age"), 9), true},
		{"lt/10<10", createRecord(10), q.LtInt(q.Field("age"), 10), false},
		{"lt/10<11", createRecord(10), q.LtInt(q.Field("age"), 11), true},
		{"lt/10<9", createRecord(10), q.LtInt(q.Field("age"), 9), false},
		{"lte/10<=10", createRecord(10), q.LteInt(q.Field("age"), 10), true},
		{"lte/10<=11", createRecord(10), q.LteInt(q.Field("age"), 11), true},
		{"lte/10<=9", createRecord(10), q.LteInt(q.Field("age"), 9), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matched, err := test.matcher.Match(test.record)
			require.NoError(t, err)
			require.Equal(t, test.match, matched)
		})
	}
}

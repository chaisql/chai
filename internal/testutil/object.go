package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/object"
	"github.com/genjidb/genji/types"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

// MakeValue turns v into a types.Value.
func MakeValue(t testing.TB, v interface{}) types.Value {
	t.Helper()

	vv, err := object.NewValue(v)
	assert.NoError(t, err)
	return vv
}

func MakeArrayValue(t testing.TB, vs ...interface{}) types.Value {
	t.Helper()

	vvs := []types.Value{}
	for _, v := range vs {
		vvs = append(vvs, MakeValue(t, v))
	}

	vb := object.NewValueBuffer(vvs...)

	return types.NewArrayValue(vb)
}

// MakeObject creates an object from a json string.
func MakeObject(t testing.TB, jsonDoc string) types.Object {
	t.Helper()

	var fb object.FieldBuffer

	err := fb.UnmarshalJSON([]byte(jsonDoc))
	assert.NoError(t, err)

	return &fb
}

// MakeObjects creates a slice of objects from json strings.
func MakeObjects(t testing.TB, jsonDocs ...string) (docs Objs) {
	for _, jsonDoc := range jsonDocs {
		docs = append(docs, MakeObject(t, jsonDoc))
	}
	return
}

// MakeArray creates an array from a json string.
func MakeArray(t testing.TB, jsonArray string) types.Array {
	t.Helper()

	var vb object.ValueBuffer

	err := vb.UnmarshalJSON([]byte(jsonArray))
	assert.NoError(t, err)

	return &vb
}

func MakeValueBuffer(t testing.TB, jsonArray string) *object.ValueBuffer {
	t.Helper()

	var vb object.ValueBuffer

	err := vb.UnmarshalJSON([]byte(jsonArray))
	assert.NoError(t, err)

	return &vb
}

type Objs []types.Object

func (o Objs) RequireEqual(t testing.TB, others Objs) {
	t.Helper()

	require.Equal(t, len(o), len(others), fmt.Sprintf("expected len %d, got %d", len(o), len(others)))

	for i, d := range o {
		RequireObjEqual(t, d, others[i])
	}
}

// Dump a json representation of v to os.Stdout.
func Dump(t testing.TB, v interface{}) {
	t.Helper()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err := enc.Encode(v)
	assert.NoError(t, err)
}

func RequireJSONEq(t testing.TB, o any, expected string) {
	t.Helper()

	data, err := json.Marshal(o)
	assert.NoError(t, err)
	require.JSONEq(t, expected, string(data))
}

// IteratorToJSONArray encodes all the objects of an iterator to a JSON array.
func IteratorToJSONArray(w io.Writer, s database.RowIterator) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	first := true
	err := s.Iterate(func(r database.Row) error {
		if !first {
			buf.WriteString(", ")
		} else {
			first = false
		}

		data, err := r.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return err
	}

	buf.WriteByte(']')
	return buf.Flush()
}

func RequireObjEqual(t testing.TB, want, got types.Object) {
	t.Helper()

	tWant, err := types.MarshalTextIndent(types.NewObjectValue(object.WithSortedFields(want)), "\n", "  ")
	require.NoError(t, err)
	tGot, err := types.MarshalTextIndent(types.NewObjectValue(object.WithSortedFields(got)), "\n", "  ")
	require.NoError(t, err)

	if diff := cmp.Diff(string(tWant), string(tGot), cmp.Comparer(strings.EqualFold)); diff != "" {
		require.Failf(t, "mismatched objects, (-want, +got)", "%s", diff)
	}
}

func RequireArrayEqual(t testing.TB, want, got types.Array) {
	t.Helper()

	tWant, err := types.MarshalTextIndent(types.NewArrayValue(want), "\n", "  ")
	require.NoError(t, err)
	tGot, err := types.MarshalTextIndent(types.NewArrayValue(got), "\n", "  ")
	require.NoError(t, err)

	if diff := cmp.Diff(string(tWant), string(tGot), cmp.Comparer(strings.EqualFold)); diff != "" {
		require.Failf(t, "mismatched arrays, (-want, +got)", "%s", diff)
	}
}

func CloneObject(t testing.TB, d types.Object) *object.FieldBuffer {
	t.Helper()

	var newFb object.FieldBuffer

	err := newFb.Copy(d)
	assert.NoError(t, err)

	return &newFb
}

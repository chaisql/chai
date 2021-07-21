package functions_test

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
)

var doc types.Document = func() types.Document {
	return document.NewFromJSON([]byte(`{
		"a": 1,
		"b": {"foo bar": [1, 2]},
		"c": [1, {"foo": "bar"}, [1, 2]]
	}`))
}()

var docWithKey types.Document = func() types.Document {
	fb := document.NewFieldBuffer()
	err := fb.Copy(doc)
	if err != nil {
		panic(err)
	}

	fb.DecodedKey = types.NewIntegerValue(1)
	var buf bytes.Buffer
	err = types.NewValueEncoder(&buf).Encode(fb.DecodedKey)
	if err != nil {
		panic(err)
	}
	fb.EncodedKey = buf.Bytes()

	return fb
}()

var envWithDoc = environment.New(doc)
var envWithDocAndKey = environment.New(docWithKey)

func TestPk(t *testing.T) {
	tests := []struct {
		name string
		env  *environment.Environment
		res  types.Value
	}{
		{"empty env", &environment.Environment{}, types.NewNullValue()},
		{"env with doc", envWithDoc, types.NewNullValue()},
		{"env with doc and key", envWithDocAndKey, types.NewIntegerValue(1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testutil.TestExpr(t, "pk()", test.env, test.res, false)
		})
	}
}

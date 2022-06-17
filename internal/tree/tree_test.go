package tree_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

var key1 = func() *tree.Key {
	return tree.NewKey(
		types.NewBoolValue(true),
		types.NewIntegerValue(1),
	)
}()

var key2 = func() *tree.Key {
	return tree.NewKey(
		types.NewBoolValue(true),
		types.NewIntegerValue(2),
	)
}()

var doc = document.NewFromMap(map[string]bool{
	"a": true,
})

func TestTreeGet(t *testing.T) {
	tests := []struct {
		name  string
		key   *tree.Key
		d     types.Document
		Fails bool
	}{
		{"existing", key1, doc, false},
		{"non-existing", key2, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var ti database.TableInfo
			ti.FieldConstraints.AllowExtraFields = true
			tree := testutil.NewTestTree(t, 10)

			err := tree.Put(key1, []byte{1})
			assert.NoError(t, err)

			v, err := tree.Get(test.key)
			if test.Fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, []byte{1}, v)
			}
		})
	}
}

func TestTreeDelete(t *testing.T) {
	tests := []struct {
		name  string
		key   *tree.Key
		Fails bool
	}{
		{"existing", key1, false},
		{"non-existing", key2, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var ti database.TableInfo
			ti.FieldConstraints.AllowExtraFields = true

			tree := testutil.NewTestTree(t, 10)

			err := tree.Put(key1, []byte{1})
			assert.NoError(t, err)

			err = tree.Delete(test.key)
			if test.Fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTreeTruncate(t *testing.T) {
	t.Run("Should succeed if tree is empty", func(t *testing.T) {
		tree := testutil.NewTestTree(t, 10)

		err := tree.Truncate()
		assert.NoError(t, err)
	})

	t.Run("Should truncate the tree", func(t *testing.T) {
		tr := testutil.NewTestTree(t, 10)

		err := tr.Put(testutil.NewKey(t, types.NewTextValue("foo")), []byte("FOO"))
		assert.NoError(t, err)
		err = tr.Put(testutil.NewKey(t, types.NewTextValue("bar")), []byte("BAR"))
		assert.NoError(t, err)

		err = tr.Truncate()
		assert.NoError(t, err)

		err = tr.IterateOnRange(nil, false, func(k *tree.Key, b []byte) error {
			return fmt.Errorf("expected no keys")
		})
		assert.NoError(t, err)
	})
}

type Keys []*tree.Key

func (a Keys) Len() int      { return len(a) }
func (a Keys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Keys) Less(i, j int) bool {
	ae, _ := a[i].Encode(10)
	be, _ := a[j].Encode(10)
	return encoding.Compare(ae, be) < 0
}

func (a Keys) String() string {
	var buf strings.Builder

	for i, k := range a {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(k.String())
	}

	return buf.String()
}

func TestTreeIterateOnRange2(t *testing.T) {
	var keys Keys
	var boolKeys Keys

	// keys: [bool, bool]
	boolKeys = append(boolKeys, tree.NewKey(types.NewBoolValue(false), types.NewBoolValue(false)))
	boolKeys = append(boolKeys, tree.NewKey(types.NewBoolValue(false), types.NewBoolValue(true)))
	boolKeys = append(boolKeys, tree.NewKey(types.NewBoolValue(true), types.NewBoolValue(false)))
	boolKeys = append(boolKeys, tree.NewKey(types.NewBoolValue(true), types.NewBoolValue(true)))

	// keys: [int, text, double]
	for i := int64(0); i < 10; i++ {
		for j := 0; j < 10; j++ {
			for k := 0; k < 10; k++ {
				keys = append(keys, tree.NewKey(
					types.NewIntegerValue(i),
					types.NewTextValue(fmt.Sprintf("foo%d", j)),
					types.NewDoubleValue(float64(k)),
				))
			}
		}
	}

	// keys: [double, double] * 10
	for i := int64(0); i < 10; i++ {
		for j := 0; j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewDoubleValue(float64(i)),
				types.NewDoubleValue(float64(j)),
			))
		}
	}

	// keys: [text, text] * 10
	for i := int64(0); i < 10; i++ {
		for j := 0; j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewTextValue(fmt.Sprintf("bar%d", i)),
				types.NewTextValue(fmt.Sprintf("baz%d", j)),
			))
		}
	}

	// keys: [blob, blob] * 10
	for i := int64(0); i < 10; i++ {
		for j := 0; j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewBlobValue([]byte(fmt.Sprintf("bar%d", i))),
				types.NewBlobValue([]byte(fmt.Sprintf("baz%d", j))),
			))
		}
	}

	// keys: [array, array] * 10
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(i))),
				types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(j))),
			))
		}
	}

	// keys: [doc, doc] * 10
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(i))),
				types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(j))),
			))
		}
	}

	sort.Sort(keys)

	tt := testutil.NewTestTree(t, 10)

	for i, k := range keys {
		err := tt.Put(k, []byte{byte(i)})
		assert.NoError(t, err)
	}
	for i, k := range boolKeys {
		err := tt.Put(k, []byte{byte(i)})
		assert.NoError(t, err)
	}

	for _, reversed := range []bool{false, true} {
		tests := []struct {
			name      string
			min, max  *tree.Key
			exclusive bool
			keys      []*tree.Key
		}{
			// all
			{"all", nil, nil, false, append(boolKeys, keys...)},

			// arity: 1
			{"= 3", tree.NewKey(types.NewIntegerValue(3)), tree.NewKey(types.NewIntegerValue(3)), false, keys[300:400]},
			{">= 3", tree.NewKey(types.NewIntegerValue(3)), nil, false, keys[300:1000]},
			{"> 3", tree.NewKey(types.NewIntegerValue(3)), nil, true, keys[400:1000]},
			{"<= 3", nil, tree.NewKey(types.NewIntegerValue(3)), false, keys[0:400]},
			{"< 3", nil, tree.NewKey(types.NewIntegerValue(3)), true, keys[0:300]},
			{">= 3 AND <= 7", tree.NewKey(types.NewIntegerValue(3)), tree.NewKey(types.NewIntegerValue(7)), false, keys[300:800]},
			{"> 3 AND < 7", tree.NewKey(types.NewIntegerValue(3)), tree.NewKey(types.NewIntegerValue(7)), true, keys[400:700]},

			// arity 2
			{"= 3 AND = foo1", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), false, keys[310:320]},
			{"= 3 AND >= foo1", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), nil, false, keys[310:400]},
			{"= 3 AND > foo1", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), nil, true, keys[320:400]},
			{"= 3 AND <= foo1", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), false, keys[300:320]},
			{"= 3 AND < foo1", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), true, keys[300:310]},
			{"= 3 AND >= foo1 AND <= foo3", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo3")), false, keys[310:340]},

			// arity 3
			{"= 3 AND = foo1 AND = 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), false, keys[315:316]},
			{"= 3 AND = foo1 AND >= 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), nil, false, keys[315:320]},
			{"= 3 AND = foo1 AND > 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), nil, true, keys[316:320]},
			{"= 3 AND = foo1 AND <= 5.0", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), false, keys[310:316]},
			{"= 3 AND = foo1 AND < 5.0", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), true, keys[310:315]},

			// other types

			// bool
			{"= false", tree.NewKey(types.NewBoolValue(false)), tree.NewKey(types.NewBoolValue(false)), false, boolKeys[0:2]},
			{"= true", tree.NewKey(types.NewBoolValue(true)), tree.NewKey(types.NewBoolValue(true)), false, boolKeys[2:4]},
			{">= false", tree.NewKey(types.NewBoolValue(false)), nil, false, boolKeys},
			{"> false", tree.NewKey(types.NewBoolValue(false)), nil, true, boolKeys[2:]},
			{"<= false", nil, tree.NewKey(types.NewBoolValue(false)), false, boolKeys[:2]},
			{"< false", nil, tree.NewKey(types.NewBoolValue(false)), true, nil},
			{"< true", nil, tree.NewKey(types.NewBoolValue(true)), true, boolKeys[:2]},

			// double
			{"= 3.0", tree.NewKey(types.NewDoubleValue(3)), tree.NewKey(types.NewDoubleValue(3)), false, keys[1030:1040]},
			{">= 3.0", tree.NewKey(types.NewDoubleValue(3)), nil, false, keys[1030:1100]},
			{"> 3.0", tree.NewKey(types.NewDoubleValue(3)), nil, true, keys[1040:1100]},
			{"<= 3.0", nil, tree.NewKey(types.NewDoubleValue(3)), false, keys[1000:1040]},
			{"< 3.0", nil, tree.NewKey(types.NewDoubleValue(3)), true, keys[1000:1030]},

			// text
			{"= bar3", tree.NewKey(types.NewTextValue("bar3")), tree.NewKey(types.NewTextValue("bar3")), false, keys[1130:1140]},
			{">= bar3", tree.NewKey(types.NewTextValue("bar3")), nil, false, keys[1130:1200]},
			{"> bar3", tree.NewKey(types.NewTextValue("bar3")), nil, true, keys[1140:1200]},
			{"<= bar3", nil, tree.NewKey(types.NewTextValue("bar3")), false, keys[1100:1140]},
			{"< bar3", nil, tree.NewKey(types.NewTextValue("bar3")), true, keys[1100:1130]},

			// blob
			{"= bar3", tree.NewKey(types.NewBlobValue([]byte("bar3"))), tree.NewKey(types.NewBlobValue([]byte("bar3"))), false, keys[1230:1240]},
			{">= bar3", tree.NewKey(types.NewBlobValue([]byte("bar3"))), nil, false, keys[1230:1300]},
			{"> bar3", tree.NewKey(types.NewBlobValue([]byte("bar3"))), nil, true, keys[1240:1300]},
			{"<= bar3", nil, tree.NewKey(types.NewBlobValue([]byte("bar3"))), false, keys[1200:1240]},
			{"< bar3", nil, tree.NewKey(types.NewBlobValue([]byte("bar3"))), true, keys[1200:1230]},

			// array
			{"= [3]", tree.NewKey(types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(3)))), tree.NewKey(types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(3)))), false, keys[1330:1340]},
			{">= [3]", tree.NewKey(types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(3)))), nil, false, keys[1330:1400]},
			{"> [3]", tree.NewKey(types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(3)))), nil, true, keys[1340:1400]},
			{"<= [3]", nil, tree.NewKey(types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(3)))), false, keys[1300:1340]},
			{"< [3]", nil, tree.NewKey(types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(3)))), true, keys[1300:1330]},

			// document
			{"= {foo: 3}", tree.NewKey(types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), tree.NewKey(types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), false, keys[1430:1440]},
			{">= {foo: 3}", tree.NewKey(types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), nil, false, keys[1430:1500]},
			{"> {foo: 3}", tree.NewKey(types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), nil, true, keys[1440:1500]},
			{"<= {foo: 3}", nil, tree.NewKey(types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), false, keys[1400:1440]},
			{"< {foo: 3}", nil, tree.NewKey(types.NewDocumentValue(document.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), true, keys[1400:1430]},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%s/reversed=%v", test.name, reversed), func(t *testing.T) {
				var keys []string

				rng := tree.Range{
					Min:       test.min,
					Max:       test.max,
					Exclusive: test.exclusive,
				}

				err := tt.IterateOnRange(&rng, reversed, func(k *tree.Key, _ []byte) error {
					keys = append(keys, k.String())
					return nil
				})
				assert.NoError(t, err)

				var want []string
				if !reversed {
					for _, k := range test.keys {
						want = append(want, k.String())
					}
				} else {
					for i := len(test.keys) - 1; i >= 0; i-- {
						want = append(want, test.keys[i].String())
					}
				}

				require.Equal(t, want, keys)
			})
		}
	}
}

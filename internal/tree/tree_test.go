package tree_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/internal/types"
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

var doc = object.NewFromMap(map[string]bool{
	"a": true,
})

func TestTreeGet(t *testing.T) {
	tests := []struct {
		name  string
		key   *tree.Key
		d     types.Object
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
		{"non-existing", key2, false},
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

func TestTreeIterateOnRange(t *testing.T) {
	var keys []*tree.Key

	// keys: [bool, bool, int] * 100
	var c int64 // for unicity
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewBoolValue(i%2 == 0),
				types.NewBoolValue(j%2 == 0),
				types.NewIntegerValue(c),
			))
			c++
		}
	}

	// keys: [int, text, double] * 1000
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

	// keys: [double, double] * 100
	for i := int64(0); i < 10; i++ {
		for j := 0; j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewDoubleValue(float64(i)),
				types.NewDoubleValue(float64(j)),
			))
		}
	}

	// keys: [text, text] * 100
	for i := int64(0); i < 10; i++ {
		for j := 0; j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewTextValue(fmt.Sprintf("bar%d", i)),
				types.NewTextValue(fmt.Sprintf("baz%d", j)),
			))
		}
	}

	// keys: [blob, blob] * 100
	for i := int64(0); i < 10; i++ {
		for j := 0; j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewBlobValue([]byte(fmt.Sprintf("bar%d", i))),
				types.NewBlobValue([]byte(fmt.Sprintf("baz%d", j))),
			))
		}
	}

	// keys: [array, array] * 100
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(i))),
				types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(j))),
			))
		}
	}

	// keys: [doc, doc] * 100
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			keys = append(keys, tree.NewKey(
				types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(i))),
				types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(j))),
			))
		}
	}

	for _, reversed := range []bool{false, true} {
		tests := []struct {
			name      string
			min, max  *tree.Key
			exclusive bool
			from, to  int
			order     tree.SortOrder
		}{
			// all
			{"all", nil, nil, false, 0, 1600, 0},

			// arity: 1
			{"= 3", tree.NewKey(types.NewIntegerValue(3)), tree.NewKey(types.NewIntegerValue(3)), false, 400, 500, 0},
			{">= 3", tree.NewKey(types.NewIntegerValue(3)), nil, false, 400, 1100, 0},
			{"> 3", tree.NewKey(types.NewIntegerValue(3)), nil, true, 500, 1100, 0},
			{"<= 3", nil, tree.NewKey(types.NewIntegerValue(3)), false, 100, 500, 0},
			{"< 3", nil, tree.NewKey(types.NewIntegerValue(3)), true, 100, 400, 0},
			{">= 3 AND <= 7", tree.NewKey(types.NewIntegerValue(3)), tree.NewKey(types.NewIntegerValue(7)), false, 400, 900, 0},
			{"> 3 AND < 7", tree.NewKey(types.NewIntegerValue(3)), tree.NewKey(types.NewIntegerValue(7)), true, 500, 800, 0},

			// arity 1, order desc
			{"= 3 desc", tree.NewKey(types.NewIntegerValue(3)), tree.NewKey(types.NewIntegerValue(3)), false, 1100, 1200, tree.SortOrder(0).SetDesc(0)},
			{">= 3 desc", tree.NewKey(types.NewIntegerValue(3)), nil, false, 500, 1200, tree.SortOrder(0).SetDesc(0)},
			{"> 3 desc", tree.NewKey(types.NewIntegerValue(3)), nil, true, 500, 1100, tree.SortOrder(0).SetDesc(0)},
			{"<= 3 desc", nil, tree.NewKey(types.NewIntegerValue(3)), false, 1100, 1500, tree.SortOrder(0).SetDesc(0)},
			{"= 12 desc", tree.NewKey(types.NewIntegerValue(12)), tree.NewKey(types.NewIntegerValue(12)), false, 0, 0, tree.SortOrder(0).SetDesc(0)},

			// arity 2
			{"= 3 AND = foo1", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), false, 410, 420, 0},
			{"= 3 AND >= foo1", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), nil, false, 410, 500, 0},
			{"= 3 AND > foo1", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), nil, true, 420, 500, 0},
			{"= 3 AND <= foo1", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), false, 400, 420, 0},
			{"= 3 AND < foo1", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), true, 400, 410, 0},
			{"= 3 AND >= foo1 AND <= foo3", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo3")), false, 410, 440, 0},

			// arity 2 desc
			{"= 3 AND = foo1 desc", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), false, 1180, 1190, tree.SortOrder(0).SetDesc(0).SetDesc(1)},
			{"= 3 AND >= foo1 desc", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), nil, false, 1100, 1190, tree.SortOrder(0).SetDesc(0).SetDesc(1)},
			{"= 3 AND > foo1 desc", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), nil, true, 1100, 1180, tree.SortOrder(0).SetDesc(0).SetDesc(1)},
			{"= 3 AND <= foo1 desc", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), false, 1180, 1200, tree.SortOrder(0).SetDesc(0).SetDesc(1)},
			{"= 3 AND < foo1 desc", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), true, 1190, 1200, tree.SortOrder(0).SetDesc(0).SetDesc(1)},
			{"= 3 AND >= foo1 AND <= foo3 desc", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo3")), false, 1160, 1190, tree.SortOrder(0).SetDesc(0).SetDesc(1)},
			{"= 3 AND > foo1 AND < foo3 desc", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1")), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo3")), true, 1170, 1180, tree.SortOrder(0).SetDesc(0).SetDesc(1)},

			// arity 3
			{"= 3 AND = foo1 AND = 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), false, 415, 416, 0},
			{"= 3 AND = foo1 AND >= 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), nil, false, 415, 420, 0},
			{"= 3 AND = foo1 AND > 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), nil, true, 416, 420, 0},
			{"= 3 AND = foo1 AND <= 5.0", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), false, 410, 416, 0},
			{"= 3 AND = foo1 AND < 5.0", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), true, 410, 415, 0},

			// arity 3 desc
			{"= 3 AND = foo1 AND = 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), false, 1184, 1185, tree.SortOrder(0).SetDesc(0).SetDesc(1).SetDesc(2)},
			{"= 3 AND = foo1 AND >= 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), nil, false, 1180, 1185, tree.SortOrder(0).SetDesc(0).SetDesc(1).SetDesc(2)},
			{"= 3 AND = foo1 AND > 5.0", tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), nil, true, 1180, 1184, tree.SortOrder(0).SetDesc(0).SetDesc(1).SetDesc(2)},
			{"= 3 AND = foo1 AND <= 5.0", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), false, 1184, 1190, tree.SortOrder(0).SetDesc(0).SetDesc(1).SetDesc(2)},
			{"= 3 AND = foo1 AND < 5.0", nil, tree.NewKey(types.NewIntegerValue(3), types.NewTextValue("foo1"), types.NewDoubleValue(5)), true, 1185, 1190, tree.SortOrder(0).SetDesc(0).SetDesc(1).SetDesc(2)},

			// other types

			// bool
			{"= false", tree.NewKey(types.NewBoolValue(false)), tree.NewKey(types.NewBoolValue(false)), false, 0, 50, 0},
			{"= true", tree.NewKey(types.NewBoolValue(true)), tree.NewKey(types.NewBoolValue(true)), false, 50, 100, 0},
			{">= false", tree.NewKey(types.NewBoolValue(false)), nil, false, 0, 100, 0},
			{"> false", tree.NewKey(types.NewBoolValue(false)), nil, true, 50, 100, 0},
			{"<= false", nil, tree.NewKey(types.NewBoolValue(false)), false, 0, 50, 0},
			{"< false", nil, tree.NewKey(types.NewBoolValue(false)), true, 0, 0, 0},
			{"< true", nil, tree.NewKey(types.NewBoolValue(true)), true, 0, 50, 0},

			// bool desc
			{"= false desc", tree.NewKey(types.NewBoolValue(false)), tree.NewKey(types.NewBoolValue(false)), false, 1550, 1600, tree.SortOrder(0).SetDesc(0)},
			{"= true desc", tree.NewKey(types.NewBoolValue(true)), tree.NewKey(types.NewBoolValue(true)), false, 1500, 1550, tree.SortOrder(0).SetDesc(0)},
			{">= false desc", tree.NewKey(types.NewBoolValue(false)), nil, false, 1500, 1600, tree.SortOrder(0).SetDesc(0)},
			{"> false desc", tree.NewKey(types.NewBoolValue(false)), nil, true, 1500, 1550, tree.SortOrder(0).SetDesc(0)},
			{"<= false desc", nil, tree.NewKey(types.NewBoolValue(false)), false, 1550, 1600, tree.SortOrder(0).SetDesc(0)},
			{"< false desc", nil, tree.NewKey(types.NewBoolValue(false)), true, 0, 0, tree.SortOrder(0).SetDesc(0)},
			{"< true desc", nil, tree.NewKey(types.NewBoolValue(true)), true, 1550, 1600, tree.SortOrder(0).SetDesc(0)},

			// double
			{"= 3.0", tree.NewKey(types.NewDoubleValue(3)), tree.NewKey(types.NewDoubleValue(3)), false, 1130, 1140, 0},
			{">= 3.0", tree.NewKey(types.NewDoubleValue(3)), nil, false, 1130, 1200, 0},
			{"> 3.0", tree.NewKey(types.NewDoubleValue(3)), nil, true, 1140, 1200, 0},
			{"<= 3.0", nil, tree.NewKey(types.NewDoubleValue(3)), false, 1100, 1140, 0},
			{"< 3.0", nil, tree.NewKey(types.NewDoubleValue(3)), true, 1100, 1130, 0},

			// double desc
			{"= 3.0 desc", tree.NewKey(types.NewDoubleValue(3)), tree.NewKey(types.NewDoubleValue(3)), false, 460, 470, tree.SortOrder(0).SetDesc(0)},
			{">= 3.0 desc", tree.NewKey(types.NewDoubleValue(3)), nil, false, 400, 470, tree.SortOrder(0).SetDesc(0)},
			{"> 3.0 desc", tree.NewKey(types.NewDoubleValue(3)), nil, true, 400, 460, tree.SortOrder(0).SetDesc(0)},
			{"<= 3.0 desc", nil, tree.NewKey(types.NewDoubleValue(3)), false, 460, 500, tree.SortOrder(0).SetDesc(0)},
			{"< 3.0 desc", nil, tree.NewKey(types.NewDoubleValue(3)), true, 470, 500, tree.SortOrder(0).SetDesc(0)},

			// text
			{"= bar3", tree.NewKey(types.NewTextValue("bar3")), tree.NewKey(types.NewTextValue("bar3")), false, 1230, 1240, 0},
			{">= bar3", tree.NewKey(types.NewTextValue("bar3")), nil, false, 1230, 1300, 0},
			{"> bar3", tree.NewKey(types.NewTextValue("bar3")), nil, true, 1240, 1300, 0},
			{"<= bar3", nil, tree.NewKey(types.NewTextValue("bar3")), false, 1200, 1240, 0},
			{"< bar3", nil, tree.NewKey(types.NewTextValue("bar3")), true, 1200, 1230, 0},

			// text desc
			{"= bar3 desc", tree.NewKey(types.NewTextValue("bar3")), tree.NewKey(types.NewTextValue("bar3")), false, 360, 370, tree.SortOrder(0).SetDesc(0)},
			{">= bar3 desc", tree.NewKey(types.NewTextValue("bar3")), nil, false, 300, 370, tree.SortOrder(0).SetDesc(0)},
			{"> bar3 desc", tree.NewKey(types.NewTextValue("bar3")), nil, true, 300, 360, tree.SortOrder(0).SetDesc(0)},
			{"<= bar3 desc", nil, tree.NewKey(types.NewTextValue("bar3")), false, 360, 400, tree.SortOrder(0).SetDesc(0)},
			{"< bar3 desc", nil, tree.NewKey(types.NewTextValue("bar3")), true, 370, 400, tree.SortOrder(0).SetDesc(0)},

			// blob
			{"= bar3", tree.NewKey(types.NewBlobValue([]byte("bar3"))), tree.NewKey(types.NewBlobValue([]byte("bar3"))), false, 1330, 1340, 0},
			{">= bar3", tree.NewKey(types.NewBlobValue([]byte("bar3"))), nil, false, 1330, 1400, 0},
			{"> bar3", tree.NewKey(types.NewBlobValue([]byte("bar3"))), nil, true, 1340, 1400, 0},
			{"<= bar3", nil, tree.NewKey(types.NewBlobValue([]byte("bar3"))), false, 1300, 1340, 0},
			{"< bar3", nil, tree.NewKey(types.NewBlobValue([]byte("bar3"))), true, 1300, 1330, 0},

			// blob desc
			{"= bar3 desc", tree.NewKey(types.NewBlobValue([]byte("bar3"))), tree.NewKey(types.NewBlobValue([]byte("bar3"))), false, 260, 270, tree.SortOrder(0).SetDesc(0)},
			{">= bar3 desc", tree.NewKey(types.NewBlobValue([]byte("bar3"))), nil, false, 200, 270, tree.SortOrder(0).SetDesc(0)},
			{"> bar3 desc", tree.NewKey(types.NewBlobValue([]byte("bar3"))), nil, true, 200, 260, tree.SortOrder(0).SetDesc(0)},
			{"<= bar3 desc", nil, tree.NewKey(types.NewBlobValue([]byte("bar3"))), false, 260, 300, tree.SortOrder(0).SetDesc(0)},
			{"< bar3 desc", nil, tree.NewKey(types.NewBlobValue([]byte("bar3"))), true, 270, 300, tree.SortOrder(0).SetDesc(0)},

			// array
			{"= [3]", tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), false, 1430, 1440, 0},
			{">= [3]", tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), nil, false, 1430, 1500, 0},
			{"> [3]", tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), nil, true, 1440, 1500, 0},
			{"<= [3]", nil, tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), false, 1400, 1440, 0},
			{"< [3]", nil, tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), true, 1400, 1430, 0},

			// array desc
			{"= [3] desc", tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), false, 160, 170, tree.SortOrder(0).SetDesc(0)},
			{">= [3] desc", tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), nil, false, 100, 170, tree.SortOrder(0).SetDesc(0)},
			{"> [3] desc", tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), nil, true, 100, 160, tree.SortOrder(0).SetDesc(0)},
			{"<= [3] desc", nil, tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), false, 160, 200, tree.SortOrder(0).SetDesc(0)},
			{"< [3] desc", nil, tree.NewKey(types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(3)))), true, 170, 200, tree.SortOrder(0).SetDesc(0)},

			// object
			{"= {foo: 3}", tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), false, 1530, 1540, 0},
			{">= {foo: 3}", tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), nil, false, 1530, 1600, 0},
			{"> {foo: 3}", tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), nil, true, 1540, 1600, 0},
			{"<= {foo: 3}", nil, tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), false, 1500, 1540, 0},
			{"< {foo: 3}", nil, tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), true, 1500, 1530, 0},

			// object desc
			{"= {foo: 3} desc", tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), false, 60, 70, tree.SortOrder(0).SetDesc(0)},
			{">= {foo: 3} desc", tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), nil, false, 0, 70, tree.SortOrder(0).SetDesc(0)},
			{"> {foo: 3} desc", tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), nil, true, 0, 60, tree.SortOrder(0).SetDesc(0)},
			{"<= {foo: 3} desc", nil, tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), false, 60, 100, tree.SortOrder(0).SetDesc(0)},
			{"< {foo: 3} desc", nil, tree.NewKey(types.NewObjectValue(object.NewFieldBuffer().Add("foo", types.NewIntegerValue(3)))), true, 70, 100, tree.SortOrder(0).SetDesc(0)},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%s/reversed=%v", test.name, reversed), func(t *testing.T) {
				tt := testutil.NewTestTree(t, 10)
				tt.Order = tree.SortOrder(test.order)

				for _, k := range keys {
					k.Encoded = nil
				}

				sort.Slice(keys, func(i, j int) bool {
					ae, _ := keys[i].Encode(10, tt.Order)
					be, _ := keys[j].Encode(10, tt.Order)
					return encoding.Compare(ae, be) < 0
				})

				for i, k := range keys {
					err := tt.Put(k, []byte{byte(i)})
					assert.NoError(t, err)
				}

				rng := tree.Range{
					Min:       test.min,
					Max:       test.max,
					Exclusive: test.exclusive,
				}

				var results []string

				err := tt.IterateOnRange(&rng, reversed, func(k *tree.Key, _ []byte) error {
					results = append(results, k.String())
					return nil
				})
				assert.NoError(t, err)

				var want []string
				if !reversed {
					for _, k := range keys[test.from:test.to] {
						want = append(want, k.String())
					}
				} else {
					subset := keys[test.from:test.to]
					for i := len(subset) - 1; i >= 0; i-- {
						want = append(want, subset[i].String())
					}
				}

				require.Equal(t, want, results)
			})
		}
	}
}

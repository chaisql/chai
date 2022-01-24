package tree_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

var key1 = func() tree.Key {
	k, _ := tree.NewKey(
		types.NewBoolValue(true),
		types.NewIntegerValue(1),
	)
	return k
}()

var key2 = func() tree.Key {
	k, _ := tree.NewKey(
		types.NewBoolValue(true),
		types.NewIntegerValue(2),
	)
	return k
}()

func MustNewKey(t *testing.T, values ...types.Value) tree.Key {
	t.Helper()

	key, err := tree.NewKey(values...)
	require.NoError(t, err)
	return key
}

var val = types.NewBoolValue(true)

func TestTreeGet(t *testing.T) {
	tests := []struct {
		name  string
		key   tree.Key
		v     types.Value
		Fails bool
	}{
		{"existing", key1, val, false},
		{"non-existing", key2, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := tree.Tree{
				Store: testutil.NewTestStore(t, "store"),
			}

			err := tree.Put(key1, val)
			assert.NoError(t, err)

			v, err := tree.Get(test.key)
			if test.Fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, test.v.V(), v.V())
			}
		})
	}
}

func TestTreeDelete(t *testing.T) {
	tests := []struct {
		name  string
		key   tree.Key
		Fails bool
	}{
		{"existing", key1, false},
		{"non-existing", key2, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := tree.Tree{
				Store: testutil.NewTestStore(t, "store"),
			}

			err := tree.Put(key1, val)
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

func TestTreeIterate(t *testing.T) {
	var keys tree.Keys

	// keys: [int, text, double] * 10
	for i := int64(0); i < 10; i++ {
		keys = append(keys, MustNewKey(t,
			types.NewIntegerValue(i),
			types.NewTextValue(fmt.Sprintf("foo%d", i)),
			types.NewDoubleValue(float64(i)),
		))
	}

	// keys: [text, double] * 10
	for i := int64(0); i < 10; i++ {
		keys = append(keys, MustNewKey(t,
			types.NewTextValue(fmt.Sprintf("bar%d", i)),
			types.NewTextValue(fmt.Sprintf("baz%d", i)),
		))
	}

	sort.Sort(keys)

	buildTree := func() *tree.Tree {
		tt := tree.Tree{
			Store: testutil.NewTestStore(t, "store"),
		}

		for i, k := range keys {
			err := tt.Put(k, types.NewIntegerValue(int64(i)))
			assert.NoError(t, err)
		}

		return &tt
	}

	tests := []struct {
		name    string
		pivot   tree.Key
		reverse bool
		keys    []tree.Key
	}{
		{"asc/no-pivot", nil, false, keys},
		{"asc/5", MustNewKey(t, types.NewIntegerValue(5)), false, keys[5:]},
		{"asc/9", MustNewKey(t, types.NewIntegerValue(9)), false, keys[9:]},
		{"asc/15", MustNewKey(t, types.NewIntegerValue(15)), false, keys[10:]},
		{"asc/bar", MustNewKey(t, types.NewTextValue("bar")), false, keys[10:]},
		{"asc/bar0", MustNewKey(t, types.NewTextValue("bar0")), false, keys[10:]},
		{"asc/5,foo3", MustNewKey(t, types.NewIntegerValue(5), types.NewTextValue("foo3")), false, keys[5:]},
		{"desc/no-pivot", nil, false, keys},
		{"desc/5", MustNewKey(t, types.NewIntegerValue(5)), true, keys[:6]},
		{"desc/10", MustNewKey(t, types.NewIntegerValue(9)), true, keys[:10]},
		{"desc/15", MustNewKey(t, types.NewIntegerValue(15)), true, keys[:10]},
		{"desc/bar", MustNewKey(t, types.NewTextValue("bar")), true, keys[:10]},
		{"desc/bar0", MustNewKey(t, types.NewTextValue("bar0")), true, keys[:11]},
		{"desc/5,foo3", MustNewKey(t, types.NewIntegerValue(5), types.NewTextValue("foo3")), true, keys[:5]},
		{"desc/bar44,foo100", MustNewKey(t, types.NewTextValue("bar44"), types.NewTextValue("foo100")), true, keys[:15]},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tt := buildTree()

			var keys []tree.Key

			var rng tree.Range
			if !test.reverse {
				rng.Min = test.pivot
			} else {
				rng.Max = test.pivot
			}
			err := tt.IterateOnRange(&rng, test.reverse, func(k tree.Key, v types.Value) error {
				keys = append(keys, append([]byte{}, k...))
				return nil
			})
			assert.NoError(t, err)

			want := test.keys
			if test.reverse {
				rev := make(tree.Keys, len(test.keys))
				copy(rev, test.keys)
				sort.Sort(sort.Reverse(rev))
				want = rev
			}

			require.Equal(t, want, keys)
		})
	}
}

func TestTreeIterateOnRange(t *testing.T) {
	var keys tree.Keys

	// keys: [int, text, double] * 10
	for i := int64(0); i < 10; i++ {
		keys = append(keys, MustNewKey(t,
			types.NewIntegerValue(i),
			types.NewTextValue(fmt.Sprintf("foo%d", i)),
			types.NewDoubleValue(float64(i)),
		))
	}

	// keys: [text, double] * 10
	for i := int64(0); i < 10; i++ {
		keys = append(keys, MustNewKey(t,
			types.NewTextValue(fmt.Sprintf("bar%d", i)),
			types.NewTextValue(fmt.Sprintf("baz%d", i)),
		))
	}

	sort.Sort(keys)

	buildTree := func() *tree.Tree {
		tt := tree.Tree{
			Store: testutil.NewTestStore(t, "store"),
		}

		for i, k := range keys {
			err := tt.Put(k, types.NewIntegerValue(int64(i)))
			assert.NoError(t, err)
		}

		return &tt
	}

	tests := []struct {
		name    string
		rng     *tree.Range
		reverse bool
		keys    tree.Keys
	}{
		{"asc/nil-range", nil, false, keys},
		{"asc/empty-range", &tree.Range{}, false, keys},
		{"asc/ >= [5]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(5))}, false, keys[5:]},
		{"asc/ > [5]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(5)), Exclusive: true}, false, keys[6:]},
		{"asc/ >= [9]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(9))}, false, keys[9:]},
		{"asc/ >= [15]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(15))}, false, keys[10:]},
		{"asc/ >= [bar]", &tree.Range{Min: MustNewKey(t, types.NewTextValue("bar"))}, false, keys[10:]},
		{"asc/ >= [bar0]", &tree.Range{Min: MustNewKey(t, types.NewTextValue("bar0"))}, false, keys[10:]},
		{"asc/ >= [7] AND <= [bar4]", &tree.Range{
			Min: MustNewKey(t, types.NewIntegerValue(7)),
			Max: MustNewKey(t, types.NewTextValue("bar4")),
		}, false, keys[7:15]},
		{"asc/ > [7] AND < [bar4]", &tree.Range{
			Min:       MustNewKey(t, types.NewIntegerValue(7)),
			Max:       MustNewKey(t, types.NewTextValue("bar4")),
			Exclusive: true,
		}, false, keys[8:14]},
		{"asc/ > [5,10,30] AND < [5,foo30,30]", &tree.Range{
			Min:       MustNewKey(t, types.NewIntegerValue(5), types.NewIntegerValue(10), types.NewIntegerValue(30)),
			Max:       MustNewKey(t, types.NewIntegerValue(5), types.NewTextValue("foo70"), types.NewIntegerValue(30)),
			Exclusive: true,
		}, false, keys[5:6]},
		{"asc/ >= [5] AND <= [5]", &tree.Range{
			Min: MustNewKey(t, types.NewIntegerValue(5)),
			Max: MustNewKey(t, types.NewIntegerValue(5)),
		}, false, keys[5:6]},
		{"desc/empty-range", &tree.Range{}, true, keys},
		{"desc/ >= [5]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(5))}, true, keys[5:]},
		{"desc/ > [5]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(5)), Exclusive: true}, true, keys[6:]},
		{"desc/ >= [9]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(9))}, true, keys[9:]},
		{"desc/ >= [15]", &tree.Range{Min: MustNewKey(t, types.NewIntegerValue(15))}, true, keys[10:]},
		{"desc/ >= [bar]", &tree.Range{Min: MustNewKey(t, types.NewTextValue("bar"))}, true, keys[10:]},
		{"desc/ >= [bar0]", &tree.Range{Min: MustNewKey(t, types.NewTextValue("bar0"))}, true, keys[10:]},
		{"desc/ >= [7] AND <= [bar4]", &tree.Range{
			Min: MustNewKey(t, types.NewIntegerValue(7)),
			Max: MustNewKey(t, types.NewTextValue("bar4")),
		}, true, keys[7:15]},
		{"desc/ > [7] AND < [bar4]", &tree.Range{
			Min:       MustNewKey(t, types.NewIntegerValue(7)),
			Max:       MustNewKey(t, types.NewTextValue("bar4")),
			Exclusive: true,
		}, true, keys[8:14]},
		{"desc/ > [5,10,30] AND < [5,foo30,30]", &tree.Range{
			Min:       MustNewKey(t, types.NewIntegerValue(5), types.NewIntegerValue(10), types.NewIntegerValue(30)),
			Max:       MustNewKey(t, types.NewIntegerValue(5), types.NewTextValue("foo70"), types.NewIntegerValue(30)),
			Exclusive: true,
		}, true, keys[5:6]},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tt := buildTree()

			var keys tree.Keys

			err := tt.IterateOnRange(test.rng, test.reverse, func(k tree.Key, v types.Value) error {
				keys = append(keys, append([]byte{}, k...))
				return nil
			})
			assert.NoError(t, err)

			want := test.keys
			if test.reverse {
				rev := make(tree.Keys, len(test.keys))
				copy(rev, test.keys)
				sort.Sort(sort.Reverse(rev))
				want = rev
			}
			require.Equal(t, want, keys)
		})
	}
}

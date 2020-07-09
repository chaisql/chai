package document

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrayContains(t *testing.T) {
	arr := NewValueBuffer(
		NewIntValue(1),
		NewTextValue("foo"),
		NewBlobValue([]byte{1, 2, 3}),
	)

	ok, err := ArrayContains(arr, NewFloat64Value(1))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ArrayContains(arr, NewBlobValue([]byte("foo")))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ArrayContains(arr, NewBlobValue([]byte("bar")))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSortArray(t *testing.T) {
	tests := []struct {
		name     string
		arr      string
		expected string
	}{
		{"empty array", `[]`, `[]`},
		{"numbers", `[1.4,3,2.1,-5]`, `[-5,1.4,2.1,3]`},
		{"text", `["foo","bar",""]`, `["","bar","foo"]`},
		{"arrays", `[[1, 2],[-1,10],[]]`, `[[],[-1,10],[1,2]]`},
		{"documents", `[{"z":10},{"a":40},{}]`, `[{"z":10},{"a":40},{}]`},
		{"mixed", `["foo",["a"],{},null,true,10]`, `[null,true,10,"foo",["a"],{}]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var arr ValueBuffer
			require.NoError(t, arr.UnmarshalJSON([]byte(test.arr)))
			output, err := SortArray(arr)
			require.NoError(t, err)
			actual, err := json.Marshal(output)
			require.NoError(t, err)
			require.Equal(t, test.expected, string(actual))
		})
	}
}

func TestValueBuffer_GetByIndexWithString(t *testing.T) {
	type args struct {
		f string
	}


	vb := NewValueBuffer(
		NewTextValue("foo"),
	)

	tests := []struct {
		name    string
		vb      ValueBuffer
		args    args
		want    Value
		want1   int
		wantErr bool
	}{
		// TODO: Add test cases.
		{"Value at index with string number", vb, args{f: "0"}, NewTextValue("foo"), 0, false},
		{"Value at index by with string", vb, args{f: "foo"}, Value{}, -1, true},

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := tt.vb.GetByIndexWithString(tt.args.f)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetByIndexWithString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetByIndexWithString() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetByIndexWithString() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}


func TestValueBufferCopy(t *testing.T) {
		tests := []struct {
			name string
			want string
		}{
			{"empty array", `[]`},
			{"flat", `[1.4,-5,"hello",true]`},
			{"nested", `[["foo","bar",1],{"a":1},[1,2]]`},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var from, to ValueBuffer
				require.NoError(t, from.UnmarshalJSON([]byte(test.want)))
				err := to.Copy(from)
				require.NoError(t, err)
				got, err := json.Marshal(to)
				require.NoError(t, err)
				require.Equal(t, test.want, string(got))
			})
		}
}

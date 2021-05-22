package binarysort

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrdering(t *testing.T) {
	tests := []struct {
		name     string
		min, max int
		enc      func([]byte, int) []byte
	}{
		{"uint64", 0, 1000, func(buf []byte, i int) []byte { return AppendUint64(buf, uint64(i)) }},
		{"int64", -1000, 1000, func(buf []byte, i int) []byte { return AppendInt64(buf, int64(i)) }},
		{"float64", -1000, 1000, func(buf []byte, i int) []byte { return AppendFloat64(buf, float64(i)) }},
		{"text", -1000, 1000, func(buf []byte, i int) []byte {
			b, err := AppendBase64(nil, AppendInt64(buf, int64(i)))
			require.NoError(t, err)
			return b
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var prev, cur []byte
			for i := test.min; i < test.max; i++ {
				cur = test.enc(cur[:0], i)
				if prev == nil {
					prev = append(prev[:0], cur...)
					continue
				}

				require.Equal(t, -1, bytes.Compare(prev, cur))
				prev = append(prev[:0], cur...)
			}
		})
	}
}

func TestTwoWays(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
		enc  func([]byte, interface{}) []byte
		dec  func([]byte) (interface{}, error)
	}{
		{"bool", true,
			func(buf []byte, v interface{}) []byte { return AppendBool(buf, v.(bool)) },
			func(buf []byte) (interface{}, error) { return DecodeBool(buf) },
		},
		{"uint64", uint64(10),
			func(buf []byte, v interface{}) []byte { return AppendUint64(buf, v.(uint64)) },
			func(buf []byte) (interface{}, error) { return DecodeUint64(buf) },
		},
		{"int64", int64(10),
			func(buf []byte, v interface{}) []byte { return AppendInt64(buf, v.(int64)) },
			func(buf []byte) (interface{}, error) { return DecodeInt64(buf) },
		},
		{"float64", float64(10),
			func(buf []byte, v interface{}) []byte { return AppendFloat64(buf, v.(float64)) },
			func(buf []byte) (interface{}, error) { return DecodeFloat64(buf) },
		},
		{"base64", []byte("hello"),
			func(buf []byte, v interface{}) []byte { res, _ := AppendBase64(buf, v.([]byte)); return res },
			func(buf []byte) (interface{}, error) { return DecodeBase64(buf) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.dec(test.enc(nil, test.want))
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

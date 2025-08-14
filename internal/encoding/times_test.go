package encoding_test

import (
	"math"
	"testing"
	"time"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/stretchr/testify/require"
)

func TestEncodeTimestamp(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		dec  time.Time
		enc  []byte
	}{
		{
			"epoch",
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			encoding.EncodeInt(nil, 0),
		},
		{
			"nanosecond-precision-loss",
			time.Date(2000, 1, 1, 0, 0, 0, 1, time.UTC),
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			encoding.EncodeInt(nil, 0),
		},
		{
			"microsecond-precision",
			time.Date(2000, 1, 1, 0, 0, 0, 1000, time.UTC),
			time.Date(2000, 1, 1, 0, 0, 0, 1000, time.UTC),
			encoding.EncodeInt(nil, 1),
		},
		{
			"minute",
			time.Date(2000, 1, 1, 0, 1, 0, 0, time.UTC),
			time.Date(2000, 1, 1, 0, 1, 0, 0, time.UTC),
			encoding.EncodeInt(nil, 60_000_000),
		},
		{
			"negative-minute",
			time.Date(1999, 12, 31, 23, 59, 0, 0, time.UTC),
			time.Date(1999, 12, 31, 23, 59, 0, 0, time.UTC),
			encoding.EncodeInt(nil, -60_000_000),
		},
		{
			"max-date",
			time.Date(294_217, 1, 10, 4, 0, 54, 775_807_000, time.UTC),
			time.Date(294_217, 1, 10, 4, 0, 54, 775_807_000, time.UTC),
			encoding.EncodeInt(nil, math.MaxInt64-encoding.Epoch-encoding.Epoch),
		},
		{
			"min-date",
			time.Date(-290_278, 12, 22, 19, 59, 05, 224_192_000, time.UTC),
			time.Date(-290_278, 12, 22, 19, 59, 05, 224_192_000, time.UTC),
			encoding.EncodeInt(nil, math.MinInt64),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc := encoding.EncodeTimestamp(nil, test.t)
			require.Equal(t, test.enc, enc)
			ts, _ := encoding.DecodeTimestamp(enc)
			require.Equal(t, test.dec, ts)
		})
	}
}

package encoding

import (
	"math"
	"time"
)

var (
	Epoch   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	MaxTime = math.MaxInt64 - Epoch
	MinTime = math.MinInt64 + Epoch
)

func EncodeTimestamp(dst []byte, t time.Time) []byte {
	x := t.UnixMicro()
	if x > MaxTime || x < MinTime {
		panic("timestamp out of range")
	}

	diff := x - Epoch

	return EncodeInt(dst, diff)
}

func DecodeTimestamp(b []byte) (time.Time, int) {
	x, n := DecodeInt(b)
	return time.UnixMicro(Epoch + x).UTC(), n
}

func ConvertToTimestamp(x int64) time.Time {
	return time.UnixMicro(Epoch + x).UTC()
}

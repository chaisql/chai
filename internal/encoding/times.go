package encoding

import (
	"math"
	"time"
)

var (
	epoch   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	maxTime = math.MaxInt64 - epoch
	minTime = math.MinInt64 + epoch
)

func EncodeTimestamp(dst []byte, t time.Time) []byte {
	x := t.UnixMicro()
	if x > maxTime || x < minTime {
		panic("timestamp out of range")
	}

	diff := x - epoch

	return EncodeInt(dst, diff)
}

func DecodeTimestamp(b []byte) (time.Time, int) {
	x, n := DecodeInt(b)
	return time.UnixMicro(epoch + x).UTC(), n
}

func ConvertToTimestamp(x int64) time.Time {
	return time.UnixMicro(epoch + x).UTC()
}

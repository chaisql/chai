package types

import (
	"math"
	"strconv"
	"time"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
	"github.com/dromara/carbon/v2"
)

var _ TypeDefinition = TimestampTypeDef{}

type TimestampTypeDef struct{}

func (t TimestampTypeDef) Decode(src []byte) (Value, int) {
	ts, n := encoding.DecodeTimestamp(src)
	return NewTimestampValue(ts), n
}

func (TimestampTypeDef) IsComparableWith(other Type) bool {
	return other == TypeTimestamp || other == TypeText
}

func (TimestampTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeTimestamp
}

var _ Value = NewTimestampValue(time.Time{})

var (
	epoch   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	maxTime = math.MaxInt64 - epoch
	minTime = math.MinInt64 + epoch
)

type TimestampValue time.Time

// NewTimestampValue returns a SQL TIMESTAMP value.
func NewTimestampValue(x time.Time) TimestampValue {
	return TimestampValue(x.UTC())
}

func (v TimestampValue) V() any {
	return time.Time(v)
}

func (v TimestampValue) Type() Type {
	return TypeTimestamp
}

func (v TimestampValue) TypeDef() TypeDefinition {
	return TimestampTypeDef{}
}

func (v TimestampValue) IsZero() (bool, error) {
	return time.Time(v).IsZero(), nil
}

func (v TimestampValue) String() string {
	return strconv.Quote(time.Time(v).Format(time.RFC3339Nano))
}

func (v TimestampValue) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v TimestampValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

func (v TimestampValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeTimestamp(dst, time.Time(v)), nil
}

func (v TimestampValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return v.Encode(dst)
}

func (v TimestampValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeTimestamp:
		return v, nil
	case TypeText:
		return NewTextValue(v.String()), nil
	}

	return nil, errors.Errorf("cannot cast %s as %s", v.Type(), target)
}

func (v TimestampValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		return time.Time(v).Equal(AsTime(other)), nil
	case TypeText:
		ts, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}
		return time.Time(v).Equal(ts), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		return time.Time(v).After(AsTime(other)), nil
	case TypeText:
		ts, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}
		return time.Time(v).After(ts), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		ta := time.Time(v)
		tb := AsTime(other)
		return ta.After(tb) || ta.Equal(tb), nil
	case TypeText:
		ta := time.Time(v)
		tb, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}

		return ta.After(tb) || ta.Equal(tb), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		return time.Time(v).Before(AsTime(other)), nil
	case TypeText:
		ts, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}
		return time.Time(v).Before(ts), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		ta := time.Time(v)
		tb := AsTime(other)
		return ta.Before(tb) || ta.Equal(tb), nil
	case TypeText:
		ta := time.Time(v)
		tb, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}

		return ta.Before(tb) || ta.Equal(tb), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) Between(a, b Value) (bool, error) {
	if !a.Type().IsTimestampCompatible() || !b.Type().IsTimestampCompatible() {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func ParseTimestamp(s string) (time.Time, error) {
	c := carbon.Parse(s, "UTC")
	if c.Error != nil {
		return time.Time{}, errors.New("invalid timestamp")
	}

	ts := c.StdTime()
	m := ts.UnixMicro()
	if m > maxTime || m < minTime {
		return time.Time{}, errors.New("timestamp out of range")
	}

	return ts, nil
}

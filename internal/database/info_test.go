package database_test

import (
	"math"
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/stretchr/testify/require"
)

func TestSequenceInfoString(t *testing.T) {
	// start with default values
	seq := database.SequenceInfo{
		Name:        "seq",
		IncrementBy: 1,
		Min:         1, Max: math.MaxInt64,
		Start: 1,
		Cache: 1,
	}

	require.Equal(t, `CREATE SEQUENCE seq`, seq.String())

	// change the increment
	seq.IncrementBy = 2
	require.Equal(t, `CREATE SEQUENCE seq INCREMENT BY 2`, seq.String())
	seq.IncrementBy = 1

	// change the min
	seq.Min = -10
	require.Equal(t, `CREATE SEQUENCE seq MINVALUE -10 START WITH 1`, seq.String())
	seq.Min = 1

	// change the max
	seq.Max = 10
	require.Equal(t, `CREATE SEQUENCE seq MAXVALUE 10`, seq.String())
	seq.Max = math.MaxInt64

	// change the direction
	seq.IncrementBy = -1
	require.Equal(t, `CREATE SEQUENCE seq INCREMENT BY -1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1`, seq.String())

	// change the min and max and start
	seq.Min = math.MinInt64
	seq.Max = -1
	seq.Start = seq.Max
	require.Equal(t, `CREATE SEQUENCE seq INCREMENT BY -1`, seq.String())

	// change the cache
	seq.Cache = 100
	require.Equal(t, `CREATE SEQUENCE seq INCREMENT BY -1 CACHE 100`, seq.String())

	// change the cycle
	seq.Cycle = true
	require.Equal(t, `CREATE SEQUENCE seq INCREMENT BY -1 CACHE 100 CYCLE`, seq.String())
}

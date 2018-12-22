package query

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	var rb RecordBuffer

	for i := 0; i < 10; i++ {
		rb.Add(record.FieldBuffer{
			field.NewString("name", fmt.Sprintf("john-%d", i)),
			field.NewInt64("age", int64(i*10)),
		})
	}

	q := Query{
		t: rb,
	}

	i := 0
	err := q.ForEach(func(r record.Record) error {
		f, err := r.Field("name")
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("john-%d", i), string(f.Data))
		f, err = r.Field("age")
		require.NoError(t, err)
		require.Equal(t, field.EncodeInt64(int64(i*10)), f.Data)

		i++
		return nil
	})
	require.NoError(t, err)
}

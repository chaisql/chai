package query

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	var rb engine.RecordBuffer

	for i := 0; i < 10; i++ {
		rb.Add(record.FieldBuffer{
			field.NewString("name", fmt.Sprintf("john-%d", i)),
			field.NewInt64("age", int64(i*10)),
		})
	}

	checkState := func(q Query) {
		i := 0
		q = q.ForEach(func(r record.Record) error {
			f, err := r.Field("name")
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("john-%d", i), string(f.Data))
			f, err = r.Field("age")
			require.NoError(t, err)
			require.Equal(t, field.EncodeInt64(int64(i*10)), f.Data)

			i++
			return nil
		})

		tr, err := q.Run(&rb)
		require.NoError(t, err)
		require.NotEmpty(t, tr)
	}

	var q Query

	checkState(q)

	q2 := q.Map(func(r record.Record) (record.Record, error) {
		var fb record.FieldBuffer
		err := fb.AddFrom(r)
		require.NoError(t, err)

		pf, err := fb.Field("name")
		require.NoError(t, err)

		fb.Set(field.NewString("name", string(pf.Data)+"---"))

		return &fb, nil
	})
	_, err := q2.Run(&rb)
	require.NoError(t, err)

	checkState(q)
	// checkState(q2)
}

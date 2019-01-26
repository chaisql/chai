package query

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

func createTable(t require.TestingT, size int) table.Browser {
	var rb table.RecordBuffer

	for i := 0; i < size; i++ {
		rb.Add(record.FieldBuffer{
			field.NewInt64("id", int64(i)),
			field.NewString("name", fmt.Sprintf("john-%d", i)),
			field.NewInt64("age", int64(i*10)),
			field.NewInt64("group", int64(i%3)),
		})
	}

	return table.NewBrowser(&rb)
}

func TestQuery(t *testing.T) {

	t.Run("Select", func(t *testing.T) {
		t.Run("Ok", func(t *testing.T) {
			tb := createTable(t, 10)

			tt, err := Select(Field("id"), Field("name")).Where(GtInt(Field("age"), 20)).Run(tb)
			require.NoError(t, err)

			b := table.NewBrowser(tt)
			count, err := b.Count()
			require.NoError(t, err)
			require.Equal(t, 7, count)

			err = table.NewBrowser(tt).ForEach(func(r record.Record) error {
				_, err := r.Field("id")
				require.NoError(t, err)
				_, err = r.Field("name")
				require.NoError(t, err)
				_, err = r.Field("age")
				require.Error(t, err)
				_, err = r.Field("group")
				require.Error(t, err)

				return nil
			}).Err()
			require.NoError(t, err)
		})
	})
}

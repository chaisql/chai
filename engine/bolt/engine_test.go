package bolt

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestTableInsert(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	table := Table{Bucket: b}
	rowid, err := table.Insert(record.FieldBuffer([]field.Field{
		field.NewInt64("a", 10),
	}))
	require.NoError(t, err)
	require.NotEmpty(t, rowid)
}

func TestTableCursor(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	table := Table{Bucket: b}
	for i := 0; i < 10; i++ {
		_, err := table.Insert(record.FieldBuffer([]field.Field{
			field.NewString("name", fmt.Sprintf("name-%d", i)),
			field.NewInt64("age", int64(i)),
		}))
		require.NoError(t, err)
	}

	c := table.Cursor()
	i := 0
	for c.Next() {
		r := c.Record()

		rc := r.Cursor()
		for rc.Next() {
			require.NoError(t, rc.Err())
			f := rc.Field()

			switch f.Name {
			case "name":
				require.Equal(t, fmt.Sprintf("name-%d", i), string(f.Data))
			case "age":
				age, err := field.DecodeInt64(f.Data)
				require.NoError(t, err)
				require.EqualValues(t, i, age)
			}
		}

		i++
	}
}

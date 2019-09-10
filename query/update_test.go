package query

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

func TestUpdateStatement(t *testing.T) {
	tests := []struct {
		name      string
		withIndex bool
	}{
		{"index", true},
		{"noindex", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx, cleanup := createTable(t, 10, test.withIndex)
			defer cleanup()

			res := Update(Table("test")).Set("age", IntValue(20)).Where(IntField("age").Gt(20)).Exec(tx)
			require.NoError(t, res.Err())

			tb, err := tx.GetTable("test")
			require.NoError(t, err)

			st := table.NewStream(tb)
			count, err := st.Count()
			require.NoError(t, err)
			require.Equal(t, 10, count)

			err = st.Iterate(func(recordID []byte, r record.Record) error {
				f, err := r.GetField("age")
				require.NoError(t, err)
				age, err := field.DecodeInt(f.Data)
				require.NoError(t, err)
				require.True(t, age <= 20)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

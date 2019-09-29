package table_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

var _ table.Reader = (*genji.Table)(nil)

func TestDump(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"OK", `name(String): "John 0", age(Int): 10
name(String): "John 1", age(Int): 11
name(String): "John 2", age(Int): 12
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb, err := tx.CreateTable("test")
				require.NoError(t, err)

				for i := 0; i < 3; i++ {
					recordID, err := tb.Insert(record.FieldBuffer([]record.Field{
						record.NewStringField("name", fmt.Sprintf("John %d", i)),
						record.NewIntField("age", 10+i),
					}))
					require.NoError(t, err)
					require.NotNil(t, recordID)
					// sleep 1ms to ensure ordering
					time.Sleep(time.Millisecond)
				}

				var buf bytes.Buffer
				err = table.Dump(&buf, tb)
				require.NoError(t, err)
				require.Equal(t, test.expected, buf.String())
				return nil
			})
			require.NoError(t, err)

		})
	}
}

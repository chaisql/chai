package table

import (
	"errors"
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func createTable(t require.TestingT, size int) Browser {
	var rb RecordBuffer

	for i := 0; i < size; i++ {
		rb.Insert(record.FieldBuffer{
			field.NewInt64("id", int64(i)),
			field.NewString("name", fmt.Sprintf("john-%d", i)),
			field.NewInt64("age", int64(i*10)),
			field.NewInt64("group", int64(i%3)),
		})
	}

	return NewBrowser(&rb)
}

func TestBrowser(t *testing.T) {
	b := createTable(t, 10)

	t.Run("ForEach", func(t *testing.T) {
		t.Run("Order", func(t *testing.T) {
			i := 0
			b = b.ForEach(func(rowid []byte, r record.Record) error {
				f, err := r.Field("id")
				require.NoError(t, err)
				v, err := field.DecodeInt64(f.Data)
				require.NoError(t, err)
				require.EqualValues(t, i, v)

				i++
				return nil
			})

			require.NoError(t, b.Err())
		})

		t.Run("Error", func(t *testing.T) {
			err := errors.New("some error")
			b2 := b.ForEach(func(rowid []byte, r record.Record) error {
				return err
			})

			// browsers are immutable, b should not be changed
			require.NoError(t, b.Err())
			require.Equal(t, err, b2.Err())
		})
	})

	t.Run("Filter", func(t *testing.T) {
		// filter odd ids
		b2 := b.Filter(func(rowid []byte, r record.Record) (bool, error) {
			f, err := r.Field("id")
			require.NoError(t, err)
			v, err := field.DecodeInt64(f.Data)
			require.NoError(t, err)
			if v%2 == 0 {
				return true, nil
			}

			return false, nil
		})

		require.NoError(t, b2.Err())

		t.Run("Immutable", func(t *testing.T) {
			// browsers are immutable, b should not be changed
			count := 0
			b := b.ForEach(func(rowid []byte, r record.Record) error {
				count++
				return nil
			})
			require.NoError(t, b.Err())
			require.Equal(t, 10, count)
		})

		t.Run("OK", func(t *testing.T) {
			// b2 should only contain even ids
			count := 0
			b := b2.ForEach(func(rowid []byte, r record.Record) error {
				f, err := r.Field("id")
				require.NoError(t, err)
				v, err := field.DecodeInt64(f.Data)
				require.NoError(t, err)
				require.True(t, v%2 == 0)
				count++
				return nil
			})
			require.NoError(t, b.Err())
			require.Equal(t, 5, count)
		})

		t.Run("Error", func(t *testing.T) {
			err := errors.New("some error")
			b2 := b.Filter(func(rowid []byte, r record.Record) (bool, error) {
				return false, err
			})
			require.NoError(t, b.Err())
			require.Equal(t, err, b2.Err())
		})
	})

	t.Run("Map", func(t *testing.T) {
		// double the age
		b2 := b.Map(func(rowid []byte, r record.Record) (record.Record, error) {
			f, err := r.Field("age")
			require.NoError(t, err)
			age, err := field.DecodeInt64(f.Data)
			require.NoError(t, err)

			var fb record.FieldBuffer
			fb.AddFrom(r)

			fb.Set(field.NewInt64("age", age*2))

			return &fb, nil
		})

		require.NoError(t, b2.Err())

		t.Run("Immutable", func(t *testing.T) {
			// browsers are immutable, b should not be changed
			i := 0
			b := b.ForEach(func(rowid []byte, r record.Record) error {
				f, err := r.Field("age")
				require.NoError(t, err)
				age, err := field.DecodeInt64(f.Data)
				require.NoError(t, err)

				require.Equal(t, int64(i*10), age)
				i++
				return nil
			})
			require.NoError(t, b.Err())
		})

		t.Run("OK", func(t *testing.T) {
			// b2 ages should be twice as big
			i := 0
			b := b2.ForEach(func(rowid []byte, r record.Record) error {
				f, err := r.Field("age")
				require.NoError(t, err)
				age, err := field.DecodeInt64(f.Data)
				require.NoError(t, err)

				require.Equal(t, int64(i*20), age)
				i++
				return nil
			})
			require.NoError(t, b.Err())
		})

		t.Run("Error", func(t *testing.T) {
			err := errors.New("some error")
			b2 := b.Map(func(rowid []byte, r record.Record) (record.Record, error) {
				return nil, err
			})
			require.NoError(t, b.Err())
			require.Equal(t, err, b2.Err())
		})
	})

	t.Run("Count", func(t *testing.T) {
		t.Run("Ok", func(t *testing.T) {
			total, err := b.Count()
			require.NoError(t, err)
			require.Equal(t, 10, total)
		})
	})

	t.Run("GroupBy", func(t *testing.T) {
		t.Run("Ok", func(t *testing.T) {

			g := b.GroupBy("group")
			for i, b := range g.Readers {
				b.ForEach(func(rowid []byte, r record.Record) error {
					f, err := r.Field("group")
					require.NoError(t, err)

					j, err := field.DecodeInt64(f.Data)
					require.NoError(t, err)

					require.EqualValues(t, i, j)
					return nil
				})
			}

			require.NoError(t, g.Err())
		})
	})

	t.Run("Count", func(t *testing.T) {
		t.Run("Ok", func(t *testing.T) {
			total, err := b.Count()
			require.NoError(t, err)
			require.Equal(t, 10, total)
		})
	})

	t.Run("Chunk", func(t *testing.T) {
		t.Run("Ok", func(t *testing.T) {
			g := b.Chunk(2)
			require.NoError(t, g.Err())
			for _, r := range g.Readers {
				total, err := r.Count()
				require.NoError(t, err)
				require.Equal(t, 2, total)
			}
		})
	})
}

func TestBrowserGroup(t *testing.T) {
	g := createTable(t, 10).Chunk(2)

	t.Run("Concat", func(t *testing.T) {
		r := g.Concat()
		require.NoError(t, r.Err())
		c, err := r.Count()
		require.NoError(t, err)
		require.Equal(t, 10, c)
	})
}

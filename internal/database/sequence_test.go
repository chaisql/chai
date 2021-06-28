package database_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/catalog"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func getLease(t testing.TB, tx *database.Transaction, name string) (*int64, error) {
	tb, err := tx.Catalog.GetTable(tx, database.SequenceTableName)
	require.NoError(t, err)

	d, err := tb.GetDocument([]byte(name))
	if err != nil {
		return nil, err
	}

	v, err := d.GetByField("seq")
	if err == document.ErrFieldNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lease := v.V.(int64)
	return &lease, nil
}

func TestSequence(t *testing.T) {
	tests := []struct {
		name         string
		info         database.SequenceInfo
		currentValue *int64
		expV         int64
		expErr       bool
	}{
		{
			name: "first call",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: 1,
				Min:         1, Max: 10,
				Start: 1,
			},
			expV: 1,
		},
		{
			name: "first call desc",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: -1,
				Min:         -5, Max: 10,
				Start: 1,
			},
			expV: 1,
		},
		{
			name: "second call",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: 1,
				Min:         1, Max: 10,
				Start: 1,
			},
			currentValue: testutil.Int64Ptr(1),
			expV:         2,
		},
		{
			name: "second call desc",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: -1,
				Min:         -5, Max: 10,
				Start: 1,
			},
			currentValue: testutil.Int64Ptr(1),
			expV:         0,
		},
		{
			name: "equal to min",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: -1,
				Min:         1, Max: 10,
				Start: 1,
			},
			currentValue: testutil.Int64Ptr(2),
			expV:         1,
		},
		{
			name: "too low",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: -1,
				Min:         1, Max: 10,
				Start: 1,
			},
			currentValue: testutil.Int64Ptr(1),
			expErr:       true,
		},
		{
			name: "equal to max",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: 1,
				Min:         1, Max: 10,
				Start: 1,
			},
			currentValue: testutil.Int64Ptr(9),
			expV:         10,
		},
		{
			name: "too high",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: 1,
				Min:         1, Max: 10,
				Start: 1,
			},
			currentValue: testutil.Int64Ptr(10),
			expErr:       true,
		},
		{
			name: "cycle min",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: -1,
				Min:         1, Max: 10,
				Start: 1,
				Cycle: true,
			},
			currentValue: testutil.Int64Ptr(1),
			expV:         10,
		},
		{
			name: "cycle max",
			info: database.SequenceInfo{
				Name:        "a",
				IncrementBy: 1,
				Min:         1, Max: 10,
				Start: 1,
				Cycle: true,
			},
			currentValue: testutil.Int64Ptr(10),
			expV:         1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, cleanup := testutil.NewTestDB(t)
			defer cleanup()

			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			err = tx.Catalog.CreateSequence(tx, &test.info)
			require.NoError(t, err)

			seq := database.Sequence{
				Info: &test.info,
			}
			seq.CurrentValue = test.currentValue
			gotV, gotErr := seq.Next(tx)
			if !test.expErr {
				require.NoError(t, gotErr)
			} else {
				require.Error(t, gotErr)
			}
			require.Equal(t, test.expV, gotV)
		})
	}

	next := func(seq *database.Sequence, tx *database.Transaction, wantV int64, wantLease int64) {
		t.Helper()

		v, err := seq.Next(tx)
		require.NoError(t, err)
		require.Equal(t, int64(wantV), v)

		got, err := getLease(t, tx, "a")
		require.NoError(t, err)
		require.Equal(t, wantLease, *got)
	}

	t.Run("default cache", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Catalog.CreateSequence(tx, &database.SequenceInfo{
			Name:        "a",
			IncrementBy: 1,
			Min:         1, Max: 5,
			Start: 1,
			Cache: 1,
		})
		require.NoError(t, err)

		seq, err := tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		// each call must increase the lease by 1 and store it in the table
		next(seq, tx, 1, 1)
		next(seq, tx, 2, 2)
		next(seq, tx, 3, 3)
		next(seq, tx, 4, 4)
		next(seq, tx, 5, 5)
		// reaching the max should not modify the cache or the lease
		cached := seq.Cached

		_, err = seq.Next(tx)
		require.Error(t, err)
		require.Equal(t, int64(5), *seq.CurrentValue)
		require.Equal(t, cached, seq.Cached)
		got, err := getLease(t, tx, "a")
		require.NoError(t, err)
		require.Equal(t, int64(5), *got)
	})

	t.Run("cache", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Catalog.CreateSequence(tx, &database.SequenceInfo{
			Name:        "a",
			IncrementBy: 1,
			Min:         1, Max: 9,
			Start: 1,
			Cache: 2,
		})
		require.NoError(t, err)

		seq, err := tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		// first call to next must increase the lease to 2 and store it in the table
		next(seq, tx, 1, 2)
		// next call must increase the current value but not touch the lease in the table
		next(seq, tx, 2, 2)
		// next call must increase the current value and the lease
		next(seq, tx, 3, 4)
		// some additional checks
		next(seq, tx, 4, 4)
		next(seq, tx, 5, 6)
		next(seq, tx, 6, 6)
		next(seq, tx, 7, 8)
		next(seq, tx, 8, 8)
		// the lease must not be greater than the max value, but not fail
		next(seq, tx, 9, 9)

		// reaching the max should not modify the cache or the lease
		cached := seq.Cached

		_, err = seq.Next(tx)
		require.Error(t, err)
		require.Equal(t, int64(9), *seq.CurrentValue)
		require.Equal(t, cached, seq.Cached)
		got, err := getLease(t, tx, "a")
		require.NoError(t, err)
		require.Equal(t, int64(9), *got)
	})

	t.Run("cache desc", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Catalog.CreateSequence(tx, &database.SequenceInfo{
			Name:        "a",
			IncrementBy: -1,
			Min:         -5, Max: 9,
			Start: 5,
			Cache: 2,
		})
		require.NoError(t, err)

		seq, err := tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		// first call to next must decrease the lease to 3 and store it in the table
		next(seq, tx, 5, 4)
		// next call must increase the current value but not touch the lease in the table
		next(seq, tx, 4, 4)
		// next call must increase the current value and the lease
		next(seq, tx, 3, 2)
		// some additional checks
		next(seq, tx, 2, 2)
		next(seq, tx, 1, 0)
		next(seq, tx, 0, 0)
		next(seq, tx, -1, -2)
		next(seq, tx, -2, -2)
		next(seq, tx, -3, -4)
		next(seq, tx, -4, -4)
		next(seq, tx, -5, -5)

		// reaching the min should not modify the cache or the lease
		cached := seq.Cached

		_, err = seq.Next(tx)
		require.Error(t, err)
		require.Equal(t, int64(-5), *seq.CurrentValue)
		require.Equal(t, cached, seq.Cached)
		got, err := getLease(t, tx, "a")
		require.NoError(t, err)
		require.Equal(t, int64(-5), *got)
	})

	t.Run("read-only", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		tx, err := db.Begin(true)
		require.NoError(t, err)

		err = tx.Catalog.CreateSequence(tx, &database.SequenceInfo{
			Name:        "a",
			IncrementBy: -1,
			Min:         -4, Max: 9,
			Start: 5,
			Cache: 2,
		})
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// open a read-only tx
		tx, err = db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		seq, err := tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		_, err = seq.Next(tx)
		require.Error(t, err)
	})

	t.Run("release", func(t *testing.T) {
		db, cleanup := testutil.NewTestDB(t)
		defer cleanup()

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Catalog.CreateSequence(tx, &database.SequenceInfo{
			Name:        "a",
			IncrementBy: 1,
			Min:         1, Max: 20,
			Start: 3,
			Cache: 5,
		})
		require.NoError(t, err)

		seq, err := tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		next(seq, tx, 3, 7)
		next(seq, tx, 4, 7)

		got, err := getLease(t, tx, "a")
		require.NoError(t, err)
		require.Equal(t, int64(7), *got)

		err = seq.Release(tx)
		require.NoError(t, err)

		c := catalog.New()
		err = c.Load(tx)
		require.NoError(t, err)

		tx.Catalog = c

		seq, err = tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		got, err = getLease(t, tx, "a")
		require.NoError(t, err)
		require.Equal(t, int64(4), *got)

		next(seq, tx, 5, 9)
	})
}

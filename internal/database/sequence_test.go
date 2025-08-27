package database_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/database/catalogstore"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func getLease(t testing.TB, tx *database.Transaction, catalog *database.Catalog, name string) (*int64, error) {
	tb, err := catalog.GetTable(tx, database.SequenceTableName)
	require.NoError(t, err)

	k := tree.NewKey(types.NewTextValue(name))
	d, err := tb.GetRow(k)
	if err != nil {
		return nil, err
	}

	v, err := d.Get("seq")
	if errors.Is(err, types.ErrColumnNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lease := types.AsInt64(v)
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
			db := testutil.NewTestDB(t)

			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			err = tx.CatalogWriter().CreateSequence(tx, &test.info)
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

	next := func(seq *database.Sequence, tx *database.Transaction, catalog *database.Catalog, wantV int64, wantLease int64) {
		t.Helper()

		v, err := seq.Next(tx)
		require.NoError(t, err)
		require.Equal(t, int64(wantV), v)

		got, err := getLease(t, tx, catalog, "a")
		require.NoError(t, err)
		require.Equal(t, wantLease, *got)
	}

	t.Run("default cache", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CatalogWriter().CreateSequence(tx, &database.SequenceInfo{
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
		next(seq, tx, tx.Catalog, 1, 1)
		next(seq, tx, tx.Catalog, 2, 2)
		next(seq, tx, tx.Catalog, 3, 3)
		next(seq, tx, tx.Catalog, 4, 4)
		next(seq, tx, tx.Catalog, 5, 5)
		// reaching the max should not modify the cache or the lease
		cached := seq.Cached

		_, err = seq.Next(tx)
		require.Error(t, err)
		require.Equal(t, int64(5), *seq.CurrentValue)
		require.Equal(t, cached, seq.Cached)
		got, err := getLease(t, tx, tx.Catalog, "a")
		require.NoError(t, err)
		require.Equal(t, int64(5), *got)
	})

	t.Run("cache", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CatalogWriter().CreateSequence(tx, &database.SequenceInfo{
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
		next(seq, tx, tx.Catalog, 1, 2)
		// next call must increase the current value but not touch the lease in the table
		next(seq, tx, tx.Catalog, 2, 2)
		// next call must increase the current value and the lease
		next(seq, tx, tx.Catalog, 3, 4)
		// some additional checks
		next(seq, tx, tx.Catalog, 4, 4)
		next(seq, tx, tx.Catalog, 5, 6)
		next(seq, tx, tx.Catalog, 6, 6)
		next(seq, tx, tx.Catalog, 7, 8)
		next(seq, tx, tx.Catalog, 8, 8)
		// the lease must not be greater than the max value, but not fail
		next(seq, tx, tx.Catalog, 9, 9)

		// reaching the max should not modify the cache or the lease
		cached := seq.Cached

		_, err = seq.Next(tx)
		require.Error(t, err)
		require.Equal(t, int64(9), *seq.CurrentValue)
		require.Equal(t, cached, seq.Cached)
		got, err := getLease(t, tx, tx.Catalog, "a")
		require.NoError(t, err)
		require.Equal(t, int64(9), *got)
	})

	t.Run("cache desc", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CatalogWriter().CreateSequence(tx, &database.SequenceInfo{
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
		next(seq, tx, tx.Catalog, 5, 4)
		// next call must increase the current value but not touch the lease in the table
		next(seq, tx, tx.Catalog, 4, 4)
		// next call must increase the current value and the lease
		next(seq, tx, tx.Catalog, 3, 2)
		// some additional checks
		next(seq, tx, tx.Catalog, 2, 2)
		next(seq, tx, tx.Catalog, 1, 0)
		next(seq, tx, tx.Catalog, 0, 0)
		next(seq, tx, tx.Catalog, -1, -2)
		next(seq, tx, tx.Catalog, -2, -2)
		next(seq, tx, tx.Catalog, -3, -4)
		next(seq, tx, tx.Catalog, -4, -4)
		next(seq, tx, tx.Catalog, -5, -5)

		// reaching the min should not modify the cache or the lease
		cached := seq.Cached

		_, err = seq.Next(tx)
		require.Error(t, err)
		require.Equal(t, int64(-5), *seq.CurrentValue)
		require.Equal(t, cached, seq.Cached)
		got, err := getLease(t, tx, tx.Catalog, "a")
		require.NoError(t, err)
		require.Equal(t, int64(-5), *got)
	})

	t.Run("read-only", func(t *testing.T) {
		db := testutil.NewTestDB(t)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		err = tx.CatalogWriter().CreateSequence(tx, &database.SequenceInfo{
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
		db := testutil.NewTestDB(t)

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CatalogWriter().CreateSequence(tx, &database.SequenceInfo{
			Name:        "a",
			IncrementBy: 1,
			Min:         1, Max: 20,
			Start: 3,
			Cache: 5,
		})
		require.NoError(t, err)

		seq, err := tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		next(seq, tx, tx.Catalog, 3, 7)
		next(seq, tx, tx.Catalog, 4, 7)

		got, err := getLease(t, tx, tx.Catalog, "a")
		require.NoError(t, err)
		require.Equal(t, int64(7), *got)

		err = seq.Release(tx)
		require.NoError(t, err)

		tx.Catalog = database.NewCatalog()
		err = catalogstore.LoadCatalog(tx)
		require.NoError(t, err)

		seq, err = tx.Catalog.GetSequence("a")
		require.NoError(t, err)

		got, err = getLease(t, tx, tx.Catalog, "a")
		require.NoError(t, err)
		require.Equal(t, int64(4), *got)

		next(seq, tx, tx.Catalog, 5, 9)
	})
}

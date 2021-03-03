package database

import (
	"context"
	"errors"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func newPath(s string) document.Path {
	return document.Path{
		document.PathFragment{
			FieldName: "k",
		},
	}
}

func TestTableInfo(t *testing.T) {
	info := &TableInfo{
		FieldConstraints: []*FieldConstraint{
			{Path: newPath("k"), Type: document.DoubleValue, IsPrimaryKey: true},
		},
	}

	doc := info.ToDocument()

	var res TableInfo
	err := res.ScanDocument(doc)
	require.NoError(t, err)
}

func TestTableInfoStore(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ng := memoryengine.NewEngine()
		defer ng.Close()

		db, err := New(context.Background(), ng, Options{
			Codec: msgpack.NewCodec(),
		})
		require.NoError(t, err)
		defer db.Close()

		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		info := &TableInfo{
			FieldConstraints: []*FieldConstraint{
				{Path: newPath("k"), Type: document.DoubleValue, IsPrimaryKey: true},
			},
		}

		// Inserting one TableInfo should work.
		err = tx.getTableStore().Insert(tx, "foo1", info)
		require.NoError(t, err)

		// Inserting an existing TableInfo should not work.
		err = tx.getTableStore().Insert(tx, "foo1", info)
		require.Equal(t, err, ErrTableAlreadyExists)

		// Getting an the list of TableInfo should work.
		list, err := tx.getTableStore().ListAll()
		require.NoError(t, err)
		require.Len(t, list, 1)

		// Deleting an existing TableInfo should work.
		err = tx.getTableStore().Delete(tx, "foo1")
		require.NoError(t, err)

		// Deleting a non-existing TableInfo should not work.
		err = tx.getTableStore().Delete(tx, "foo1")
		if !errors.Is(err, ErrTableNotFound) {
			require.Equal(t, err, ErrTableNotFound)
		}
	})

	t.Run("on rollback", func(t *testing.T) {
		ng := memoryengine.NewEngine()
		defer ng.Close()

		db, err := New(context.Background(), ng, Options{
			Codec: msgpack.NewCodec(),
		})
		require.NoError(t, err)
		defer db.Close()

		info := &TableInfo{
			FieldConstraints: []*FieldConstraint{
				{Path: newPath("k"), Type: document.DoubleValue, IsPrimaryKey: true},
			},
		}

		insertAndRollback := func() {
			tx, err := db.Begin(true)
			require.NoError(t, err)
			err = tx.getTableStore().Insert(tx, "foo", info)
			require.NoError(t, err)
			err = tx.Rollback()
			require.NoError(t, err)
		}

		insertAndRollback()
		insertAndRollback()
	})

	t.Run("on commit", func(t *testing.T) {
		ng := memoryengine.NewEngine()
		defer ng.Close()

		db, err := New(context.Background(), ng, Options{
			Codec: msgpack.NewCodec(),
		})
		require.NoError(t, err)
		defer db.Close()

		info := &TableInfo{
			FieldConstraints: []*FieldConstraint{
				{Path: newPath("k"), Type: document.DoubleValue, IsPrimaryKey: true},
			},
		}

		insertAndCommit := func() error {
			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			err = tx.getTableStore().Insert(tx, "foo", info)
			if err != nil {
				return err
			}
			return tx.Commit()
		}

		require.NoError(t, insertAndCommit())
		require.Error(t, insertAndCommit())
	})
}

func TestIndexStore(t *testing.T) {
	ng := memoryengine.NewEngine()
	defer ng.Close()

	tx, err := ng.Begin(context.Background(), engine.TxOptions{
		Writable: true,
	})
	require.NoError(t, err)
	defer tx.Rollback()

	err = tx.CreateStore([]byte("test"))
	require.NoError(t, err)
	st, err := tx.GetStore([]byte("test"))
	require.NoError(t, err)

	idxs := indexStore{db: &Database{Codec: msgpack.NewCodec()}, st: st}

	t.Run("Basic operations", func(t *testing.T) {
		cfg := IndexInfo{
			TableName: "test",
			IndexName: "idx_test",
			Unique:    true,
			Type:      document.BoolValue,
		}

		err = idxs.Insert(cfg)
		require.NoError(t, err)

		// Inserting the same index should fail.
		err = idxs.Insert(cfg)
		require.EqualError(t, err, ErrIndexAlreadyExists.Error())

		idxcfg, err := idxs.Get("idx_test")
		require.NoError(t, err)
		require.Equal(t, &cfg, idxcfg)

		// Updating the index should work
		cfg.Unique = false
		err = idxs.Replace(cfg.IndexName, cfg)
		require.NoError(t, err)
		idxcfg, err = idxs.Get("idx_test")
		require.NoError(t, err)
		require.False(t, idxcfg.Unique)

		err = idxs.Delete("idx_test")
		require.NoError(t, err)

		// Getting a non existing index should fail.
		_, err = idxs.Get("idx_test")
		require.EqualError(t, err, ErrIndexNotFound.Error())
	})

	t.Run("List all indexes", func(t *testing.T) {
		idxcfgs := []*IndexInfo{
			{TableName: "test1", IndexName: "idx_test1", Unique: true},
			{TableName: "test2", IndexName: "idx_test2", Unique: true},
			{TableName: "test3", IndexName: "idx_test3", Unique: true},
		}
		for _, v := range idxcfgs {
			err = idxs.Insert(*v)
			require.NoError(t, err)
		}

		list, err := idxs.ListAll()
		require.NoError(t, err)
		require.Len(t, list, len(idxcfgs))
		require.EqualValues(t, idxcfgs, list)

		// Removing one index should remove only one index.
		err = idxs.Delete("idx_test1")
		require.NoError(t, err)

		list, err = idxs.ListAll()
		require.NoError(t, err)
		require.Len(t, list, len(idxcfgs)-1)
	})
}

func TestFieldConstraintsInfer(t *testing.T) {
	tests := []struct {
		name      string
		got, want FieldConstraints
		fails     bool
	}{
		{
			"No change",
			[]*FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			[]*FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			false,
		},
		{
			"Array",
			[]*FieldConstraint{{Path: document.NewPath("a", "0"), Type: document.IntegerValue}},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "0")}},
				{Path: document.NewPath("a", "0"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Document",
			[]*FieldConstraint{{Path: document.NewPath("a", "b"), Type: document.IntegerValue}},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b")}},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Complex path",
			[]*FieldConstraint{{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.IntegerValue}},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b"), Type: document.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3"), Type: document.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3", "1"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Overlaping constraints",
			[]*FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "c"), Type: document.IntegerValue},
			},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b"), document.NewPath("a", "c")}},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "c"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Same path inferred and non inferred: inferred first",
			[]*FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a"), Type: document.DocumentValue},
			},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Same path inferred and non inferred: inferred last",
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Complex case",
			[]*FieldConstraint{
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b", "3", "1", "c", "d"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b", "2"), Type: document.IntegerValue},
			},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d"), document.NewPath("a", "b", "2")}},
				{Path: document.NewPath("a", "b"), Type: document.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d"), document.NewPath("a", "b", "2")}},
				{Path: document.NewPath("a", "b", "3"), Type: document.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d")}},
				{Path: document.NewPath("a", "b", "3", "1"), Type: document.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d")}},
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: document.DocumentValue},
				{Path: document.NewPath("a", "b", "3", "1", "c", "d"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b", "2"), Type: document.IntegerValue},
			},
			false,
		},
		{
			"Same path, different constraint",
			[]*FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b"), Type: document.DoubleValue},
			},
			nil,
			true,
		},
		{
			"Inferred constraint first, conflict with non inferred",
			[]*FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
				{Path: document.NewPath("a"), Type: document.IntegerValue},
			},
			nil,
			true,
		},
		{
			"Non inferred constraint first, conflict with inferred",
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.IntegerValue},
				{Path: document.NewPath("a", "b"), Type: document.IntegerValue},
			},
			nil,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.got.Infer()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}
}

func TestFieldConstraintsAdd(t *testing.T) {
	tests := []struct {
		name  string
		got   FieldConstraints
		add   FieldConstraint
		want  FieldConstraints
		merge bool
		fails bool
	}{
		{
			"Same path",
			[]*FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			FieldConstraint{Path: document.NewPath("a"), Type: document.IntegerValue},
			nil,
			false,
			true,
		},
		{
			"Duplicate primary key",
			[]*FieldConstraint{{Path: document.NewPath("a"), IsPrimaryKey: true, Type: document.IntegerValue}},
			FieldConstraint{Path: document.NewPath("b"), IsPrimaryKey: true, Type: document.IntegerValue},
			nil,
			false,
			true,
		},
		{
			"Different path",
			[]*FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			FieldConstraint{Path: document.NewPath("b"), Type: document.IntegerValue},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.IntegerValue},
				{Path: document.NewPath("b"), Type: document.IntegerValue},
			},
			false,
			false,
		},
		{
			"Conflict, with merge",
			[]*FieldConstraint{{Path: document.NewPath("a"), Type: document.IntegerValue}},
			FieldConstraint{Path: document.NewPath("a"), Type: document.IntegerValue},
			[]*FieldConstraint{
				{Path: document.NewPath("a"), Type: document.IntegerValue},
			},
			true,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.got.Add(&test.add, test.merge)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, test.got)
			}
		})
	}
}

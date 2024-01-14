package catalogstore

import (
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

func LoadCatalog(tx *database.Transaction) error {
	cw := tx.CatalogWriter()

	err := cw.Init(tx)
	if err != nil {
		return err
	}

	tables, indexes, sequences, err := loadCatalogStore(tx, tx.Catalog.CatalogTable)
	if err != nil {
		return errors.Wrap(err, "failed to load catalog store")
	}

	// add the __chai_catalog table to the list of tables
	// so that it can be queried
	ti := tx.Catalog.CatalogTable.Info().Clone()
	// make sure that table is read-only
	ti.ReadOnly = true
	tables = append(tables, *ti)

	// load tables and indexes first
	tx.Catalog.Cache.Load(tables, indexes, nil)

	if len(sequences) > 0 {
		var seqList []database.Sequence
		seqList, err = loadSequences(tx, sequences)
		if err != nil {
			return errors.Wrap(err, "failed to load sequences")
		}

		tx.Catalog.Cache.Load(nil, nil, seqList)
	}

	return nil
}

func loadSequences(tx *database.Transaction, info []database.SequenceInfo) ([]database.Sequence, error) {
	tb, err := tx.Catalog.GetTable(tx, database.SequenceTableName)
	if err != nil {
		return nil, err
	}

	sequences := make([]database.Sequence, len(info))
	for i := range info {
		key := tree.NewKey(types.NewTextValue(info[i].Name))
		r, err := tb.GetRow(key)
		if err != nil {
			return nil, err
		}

		v, err := r.Get("seq")
		if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
			return nil, err
		}

		var currentValue *int64
		if err == nil && v.Type() != types.TypeNull {
			v := types.As[int64](v)
			currentValue = &v
		}

		sequences[i] = database.NewSequence(&info[i], currentValue)
	}

	return sequences, nil
}

func loadCatalogStore(tx *database.Transaction, s *database.CatalogStore) (tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.SequenceInfo, err error) {
	tb := s.Table(tx)

	err = tb.IterateOnRange(nil, false, func(key *tree.Key, r database.Row) error {
		tp, err := r.Get("type")
		if err != nil {
			return err
		}

		switch types.As[string](tp) {
		case database.RelationTableType:
			ti, err := tableInfoFromRow(r)
			if err != nil {
				return errors.Wrap(err, "failed to decode table info")
			}
			tables = append(tables, *ti)
		case database.RelationIndexType:
			i, err := indexInfoFromRow(r)
			if err != nil {
				return errors.Wrap(err, "failed to decode index info")
			}

			indexes = append(indexes, *i)
		case database.RelationSequenceType:
			i, err := sequenceInfoFromRow(r)
			if err != nil {
				return errors.Wrap(err, "failed to decode sequence info")
			}
			sequences = append(sequences, *i)
		}

		return nil
	})
	return
}

func tableInfoFromRow(r database.Row) (*database.TableInfo, error) {
	s, err := r.Get("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(types.As[string](s))).ParseStatement()
	if err != nil {
		return nil, err
	}

	ti := stmt.(*statement.CreateTableStmt).Info

	v, err := r.Get("namespace")
	if err != nil {
		return nil, err
	}
	storeNamespace := types.As[int64](v)
	if storeNamespace <= 0 {
		return nil, errors.Errorf("invalid store namespace: %v", storeNamespace)
	}

	ti.StoreNamespace = tree.Namespace(storeNamespace)

	v, err = r.Get("rowid_sequence_name")
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return nil, err
	}
	if err == nil && v.Type() != types.TypeNull {
		ti.RowidSequenceName = types.As[string](v)
	}

	ti.BuildPrimaryKey()

	return &ti, nil
}

func indexInfoFromRow(r database.Row) (*database.IndexInfo, error) {
	s, err := r.Get("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(types.As[string](s))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateIndexStmt).Info

	v, err := r.Get("namespace")
	if err != nil {
		return nil, err
	}

	storeNamespace := types.As[int64](v)
	if storeNamespace <= 0 {
		return nil, errors.Errorf("invalid store namespace: %v", storeNamespace)
	}

	i.StoreNamespace = tree.Namespace(storeNamespace)

	v, err = r.Get("owner")
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return nil, err
	}
	if err == nil && v.Type() != types.TypeNull {
		owner, err := ownerFromObject(types.As[types.Object](v))
		if err != nil {
			return nil, err
		}
		i.Owner = *owner
	}

	return &i, nil
}

func sequenceInfoFromRow(r database.Row) (*database.SequenceInfo, error) {
	s, err := r.Get("sql")
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sql field")
	}

	stmt, err := parser.NewParser(strings.NewReader(types.As[string](s))).ParseStatement()
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse sql")
	}

	i := stmt.(*statement.CreateSequenceStmt).Info

	v, err := r.Get("owner")
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return nil, errors.Wrap(err, "failed to get owner field")
	}
	if err == nil && v.Type() != types.TypeNull {
		owner, err := ownerFromObject(types.As[types.Object](v))
		if err != nil {
			return nil, errors.Wrap(err, "failed to get owner")
		}
		i.Owner = *owner
	}

	return &i, nil
}

func ownerFromObject(o types.Object) (*database.Owner, error) {
	var owner database.Owner

	v, err := o.GetByField("table_name")
	if err != nil {
		return nil, err
	}

	owner.TableName = types.As[string](v)

	v, err = o.GetByField("paths")
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return nil, err
	}
	if err == nil && v.Type() != types.TypeNull {
		err = types.As[types.Array](v).Iterate(func(i int, value types.Value) error {
			pp, err := parser.ParsePath(types.As[string](value))
			if err != nil {
				return err
			}

			owner.Paths = append(owner.Paths, pp)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return &owner, nil
}

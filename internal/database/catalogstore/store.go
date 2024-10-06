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
		if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
			return nil, err
		}

		var currentValue *int64
		if err == nil && v.Type() != types.TypeNull {
			v := types.AsInt64(v)
			currentValue = &v
		}

		sequences[i] = database.NewSequence(&info[i], currentValue)
	}

	return sequences, nil
}

func loadCatalogStore(tx *database.Transaction, s *database.CatalogStore) (tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.SequenceInfo, err error) {
	tb := s.Table(tx)

	it, err := tb.Iterator(nil)
	if err != nil {
		return nil, nil, nil, err
	}
	defer it.Close()

	// iterate over all the rows in the catalog store
	// and load the tables and indexes
	for it.First(); it.Valid(); it.Next() {
		r, err := it.Value()
		if err != nil {
			return nil, nil, nil, err
		}

		tp, err := r.Get("type")
		if err != nil {
			return nil, nil, nil, err
		}

		switch types.AsString(tp) {
		case database.RelationTableType:
			ti, err := tableInfoFromRow(r)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to decode table info")
			}
			tables = append(tables, *ti)
		case database.RelationIndexType:
			i, err := indexInfoFromRow(r)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to decode index info")
			}

			indexes = append(indexes, *i)
		case database.RelationSequenceType:
			i, err := sequenceInfoFromRow(r)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to decode sequence info")
			}
			sequences = append(sequences, *i)
		}
	}

	if err := it.Error(); err != nil {
		return nil, nil, nil, err
	}

	return
}

func tableInfoFromRow(r database.Row) (*database.TableInfo, error) {
	s, err := r.Get("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(types.AsString(s))).ParseStatement()
	if err != nil {
		return nil, err
	}

	ti := stmt.(*statement.CreateTableStmt).Info

	v, err := r.Get("namespace")
	if err != nil {
		return nil, err
	}
	storeNamespace := types.AsInt64(v)
	if storeNamespace <= 0 {
		return nil, errors.Errorf("invalid store namespace: %v", storeNamespace)
	}

	ti.StoreNamespace = tree.Namespace(storeNamespace)

	v, err = r.Get("rowid_sequence_name")
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return nil, err
	}
	if err == nil && v.Type() != types.TypeNull {
		ti.RowidSequenceName = types.AsString(v)
	}

	ti.BuildPrimaryKey()

	return &ti, nil
}

func indexInfoFromRow(r database.Row) (*database.IndexInfo, error) {
	s, err := r.Get("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(types.AsString(s))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateIndexStmt).Info

	v, err := r.Get("namespace")
	if err != nil {
		return nil, err
	}

	storeNamespace := types.AsInt64(v)
	if storeNamespace <= 0 {
		return nil, errors.Errorf("invalid store namespace: %v", storeNamespace)
	}

	i.StoreNamespace = tree.Namespace(storeNamespace)

	owner, err := ownerFromRow(r)
	if err != nil {
		return nil, err
	}
	if owner != nil {
		i.Owner = *owner
	}

	return &i, nil
}

func sequenceInfoFromRow(r database.Row) (*database.SequenceInfo, error) {
	s, err := r.Get("sql")
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sql field")
	}

	stmt, err := parser.NewParser(strings.NewReader(types.AsString(s))).ParseStatement()
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse sql")
	}

	i := stmt.(*statement.CreateSequenceStmt).Info

	owner, err := ownerFromRow(r)
	if err != nil {
		return nil, err
	}
	if owner != nil {
		i.Owner = *owner
	}

	return &i, nil
}

func ownerFromRow(r database.Row) (*database.Owner, error) {
	var owner database.Owner

	v, err := r.Get("owner_table_name")
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return nil, err
	}
	if err != nil || v.Type() == types.TypeNull {
		return nil, nil
	}

	owner.TableName = types.AsString(v)

	v, err = r.Get("owner_table_columns")
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return nil, err
	}
	if err == nil && v.Type() != types.TypeNull {
		cols := types.AsString(v)
		owner.Columns = strings.Split(cols, ",")
	}

	return &owner, nil
}

package catalogstore

import (
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

func LoadCatalog(tx *database.Transaction, c *database.Catalog) error {
	tables, indexes, sequences, err := loadCatalogStore(tx, c.CatalogTable)
	if err != nil {
		fmt.Println(1, err)
		return err
	}

	for _, tb := range tables {
		// bind default values with catalog
		for _, fc := range tb.FieldConstraints {
			if fc.DefaultValue == nil {
				continue
			}

			fc.DefaultValue.Bind(c)
		}
	}

	// add the __genji_catalog table to the list of tables
	// so that it can be queried
	ti := c.CatalogTable.Info().Clone()
	// make sure that table is read-only
	ti.ReadOnly = true
	tables = append(tables, *ti)

	// load tables and indexes first
	c.Cache.Load(tables, indexes, nil)

	if len(sequences) > 0 {
		var seqList []database.Sequence
		seqList, err = loadSequences(tx, c, sequences)
		if err != nil {
			fmt.Println(2, err)

			return err
		}

		c.Cache.Load(nil, nil, seqList)
	}

	return nil
}

func loadSequences(tx *database.Transaction, c *database.Catalog, info []database.SequenceInfo) ([]database.Sequence, error) {
	tb, err := c.GetTable(tx, database.SequenceTableName)
	if err != nil {
		return nil, err
	}

	sequences := make([]database.Sequence, len(info))
	for i := range info {
		key, err := tree.NewKey(types.NewTextValue(info[i].Name))
		if err != nil {
			return nil, err
		}
		d, err := tb.GetDocument(key)
		if err != nil {
			return nil, err
		}

		v, err := d.GetByField("seq")
		if err != nil && !errors.Is(err, document.ErrFieldNotFound) {
			return nil, err
		}

		var currentValue *int64
		if err == nil {
			v := v.V().(int64)
			currentValue = &v

		}

		sequences[i] = database.NewSequence(&info[i], currentValue)
	}

	return sequences, nil
}

func loadCatalogStore(tx *database.Transaction, s *database.CatalogStore) (tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.SequenceInfo, err error) {
	tb := s.Table(tx)

	err = tb.IterateOnRange(nil, false, func(key tree.Key, d types.Document) error {
		tp, err := d.GetByField("type")
		if err != nil {
			return err
		}

		switch tp.V().(string) {
		case database.RelationTableType:
			ti, err := tableInfoFromDocument(d)
			if err != nil {
				return err
			}
			tables = append(tables, *ti)
		case database.RelationIndexType:
			i, err := indexInfoFromDocument(d)
			if err != nil {
				return err
			}

			indexes = append(indexes, *i)
		case database.RelationSequenceType:
			i, err := sequenceInfoFromDocument(d)
			if err != nil {
				return err
			}
			sequences = append(sequences, *i)
		}

		return nil
	})
	return
}

func tableInfoFromDocument(d types.Document) (*database.TableInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V().(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	ti := stmt.(*statement.CreateTableStmt).Info

	v, err := d.GetByField("store_name")
	if err != nil {
		return nil, err
	}
	ti.StoreName = v.V().([]byte)

	v, err = d.GetByField("docid_sequence_name")
	if err != nil && !errors.Is(err, document.ErrFieldNotFound) {
		return nil, err
	}
	if err == nil {
		ti.DocidSequenceName = v.V().(string)
	}

	return &ti, nil
}

func indexInfoFromDocument(d types.Document) (*database.IndexInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V().(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateIndexStmt).Info

	v, err := d.GetByField("store_name")
	if err != nil {
		return nil, err
	}
	i.StoreName = v.V().([]byte)

	v, err = d.GetByField("owner")
	if err != nil && !errors.Is(err, document.ErrFieldNotFound) {
		return nil, err
	}
	if err == nil {
		owner, err := ownerFromDocument(v.V().(types.Document))
		if err != nil {
			return nil, err
		}
		i.Owner = *owner
	}

	return &i, nil
}

func sequenceInfoFromDocument(d types.Document) (*database.SequenceInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V().(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateSequenceStmt).Info

	v, err := d.GetByField("owner")
	if err != nil && !errors.Is(err, document.ErrFieldNotFound) {
		return nil, err
	}
	if err == nil {
		owner, err := ownerFromDocument(v.V().(types.Document))
		if err != nil {
			return nil, err
		}
		i.Owner = *owner
	}

	return &i, nil
}

func ownerFromDocument(d types.Document) (*database.Owner, error) {
	var owner database.Owner

	v, err := d.GetByField("table_name")
	if err != nil {
		return nil, err
	}

	owner.TableName = v.V().(string)

	v, err = d.GetByField("path")
	if err != nil && !errors.Is(err, document.ErrFieldNotFound) {
		return nil, err
	}
	if err == nil {
		err = v.V().(types.Array).Iterate(func(i int, value types.Value) error {
			pp, err := parser.ParsePath(v.V().(string))
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

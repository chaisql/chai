package catalog

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stringutil"
)

func tableInfoToDocument(ti *database.TableInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(ti.TableName))
	buf.Add("type", document.NewTextValue(CatalogTableTableType))
	buf.Add("store_name", document.NewBlobValue(ti.StoreName))
	buf.Add("sql", document.NewTextValue(ti.String()))
	if ti.DocidSequenceName != "" {
		buf.Add("docid_sequence_name", document.NewTextValue(ti.DocidSequenceName))
	}

	return buf
}

func tableInfoFromDocument(d document.Document) (*database.TableInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V.(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	ti := stmt.(*statement.CreateTableStmt).Info

	v, err := d.GetByField("store_name")
	if err != nil {
		return nil, err
	}
	ti.StoreName = v.V.([]byte)

	v, err = d.GetByField("docid_sequence_name")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		ti.DocidSequenceName = v.V.(string)
	}

	return &ti, nil
}

func indexInfoToDocument(i *database.IndexInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(i.IndexName))
	buf.Add("type", document.NewTextValue(CatalogTableIndexType))
	buf.Add("store_name", document.NewBlobValue(i.StoreName))
	buf.Add("table_name", document.NewTextValue(i.TableName))
	buf.Add("sql", document.NewTextValue(i.String()))
	if i.ConstraintPath != nil {
		buf.Add("constraint_path", document.NewTextValue(i.ConstraintPath.String()))
	}

	return buf
}

func indexInfoFromDocument(d document.Document) (*database.IndexInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V.(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateIndexStmt).Info

	v, err := d.GetByField("store_name")
	if err != nil {
		return nil, err
	}
	i.StoreName = v.V.([]byte)

	v, err = d.GetByField("constraint_path")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		i.ConstraintPath, err = parser.ParsePath(v.V.(string))
		if err != nil {
			return nil, err
		}
	}

	return &i, nil
}

func sequenceInfoToDocument(seq *database.SequenceInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(seq.Name))
	buf.Add("type", document.NewTextValue(CatalogTableSequenceType))
	buf.Add("sql", document.NewTextValue(seq.String()))

	if seq.Owner.TableName != "" {
		owner := document.NewFieldBuffer().Add("table_name", document.NewTextValue(seq.Owner.TableName))
		if seq.Owner.Path != nil {
			owner.Add("path", document.NewTextValue(seq.Owner.Path.String()))
		}

		buf.Add("owner", document.NewDocumentValue(owner))
	}

	return buf
}

func sequenceInfoFromDocument(d document.Document) (*database.SequenceInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V.(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateSequenceStmt).Info

	v, err := d.GetByField("owner")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		d := v.V.(document.Document)
		v, err := d.GetByField("table_name")
		if err != nil {
			return nil, err
		}

		i.Owner.TableName = v.V.(string)

		v, err = d.GetByField("path")
		if err != nil && err != document.ErrFieldNotFound {
			return nil, err
		}
		if err == nil {
			i.Owner.Path, err = parser.ParsePath(v.V.(string))
			if err != nil {
				return nil, err
			}
		}
	}

	return &i, nil
}

type CatalogTable struct {
	Info *database.TableInfo
}

func NewCatalogTable(tx *database.Transaction) *CatalogTable {
	return &CatalogTable{
		Info: &database.TableInfo{
			TableName: CatalogTableName,
			StoreName: []byte(CatalogTableName),
			FieldConstraints: []*database.FieldConstraint{
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "name",
						},
					},
					Type:         document.TextValue,
					IsPrimaryKey: true,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "type",
						},
					},
					Type: document.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "table_name",
						},
					},
					Type: document.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "sql",
						},
					},
					Type: document.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "store_name",
						},
					},
					Type: document.BlobValue,
				},
			},
		},
	}
}

func (s *CatalogTable) Init(tx *database.Transaction) error {
	_, err := tx.Tx.GetStore([]byte(CatalogTableName))
	if err == engine.ErrStoreNotFound {
		err = tx.Tx.CreateStore([]byte(CatalogTableName))
	}

	return err
}

func (s *CatalogTable) Load(tx *database.Transaction) (tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.SequenceInfo, err error) {
	tb := s.GetTable(tx)

	err = tb.AscendGreaterOrEqual(document.Value{}, func(d document.Document) error {
		tp, err := d.GetByField("type")
		if err != nil {
			return err
		}

		switch tp.V.(string) {
		case CatalogTableTableType:
			ti, err := tableInfoFromDocument(d)
			if err != nil {
				return err
			}
			tables = append(tables, *ti)
		case CatalogTableIndexType:
			i, err := indexInfoFromDocument(d)
			if err != nil {
				return err
			}

			indexes = append(indexes, *i)
		case CatalogTableSequenceType:
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

func (s *CatalogTable) GetTable(tx *database.Transaction) *database.Table {
	st, err := tx.Tx.GetStore([]byte(CatalogTableName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", CatalogTableName, err))
	}

	return &database.Table{
		Tx:    tx,
		Store: st,
		Info:  s.Info,
	}
}

// InsertTable inserts a new tableInfo for the given table name.
// If info.StoreName is nil, it generates one and stores it in info.
func (s *CatalogTable) InsertTable(tx *database.Transaction, tableName string, info *database.TableInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Insert(tableInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: tableName}
	}

	return err
}

// Replace replaces tableName table information with the new info.
func (s *CatalogTable) ReplaceTable(tx *database.Transaction, tableName string, info *database.TableInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(tableName), tableInfoToDocument(info))
	return err
}

func (s *CatalogTable) DeleteTable(tx *database.Transaction, tableName string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(tableName))
}

func (s *CatalogTable) InsertIndex(tx *database.Transaction, info *database.IndexInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Insert(indexInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	return err
}

func (s *CatalogTable) ReplaceIndex(tx *database.Transaction, indexName string, info *database.IndexInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(indexName), indexInfoToDocument(info))
	return err
}

func (s *CatalogTable) DeleteIndex(tx *database.Transaction, indexName string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(indexName))
}

func (s *CatalogTable) InsertSequence(tx *database.Transaction, info *database.SequenceInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Insert(sequenceInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: info.Name}
	}

	return err
}

func (s *CatalogTable) ReplaceSequence(tx *database.Transaction, name string, info *database.SequenceInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(name), sequenceInfoToDocument(info))
	return err
}

func (s *CatalogTable) DeleteSequence(tx *database.Transaction, name string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(name))
}

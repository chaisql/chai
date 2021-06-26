package catalog

import (
	"encoding/binary"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/stringutil"
)

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

func (s *CatalogTable) tableInfoToDocument(ti *database.TableInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(ti.TableName))
	buf.Add("type", document.NewTextValue(CatalogTableTableType))
	buf.Add("store_name", document.NewBlobValue(ti.StoreName))
	buf.Add("sql", document.NewTextValue(ti.String()))
	return buf
}

func (s *CatalogTable) indexInfoToDocument(i *database.IndexInfo) document.Document {
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

func (s *CatalogTable) sequenceInfoToDocument(seq *database.SequenceInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(seq.Name))
	buf.Add("type", document.NewTextValue(CatalogTableSequenceType))
	buf.Add("sql", document.NewTextValue(seq.String()))

	return buf
}

// InsertTable inserts a new tableInfo for the given table name.
// If info.StoreName is nil, it generates one and stores it in info.
func (s *CatalogTable) InsertTable(tx *database.Transaction, tableName string, info *database.TableInfo) error {
	tb := s.GetTable(tx)

	if info.StoreName == nil {
		seq, err := tb.Store.NextSequence()
		if err != nil {
			return err
		}
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, seq)
		info.StoreName = buf[:n]
	}

	_, err := tb.Insert(s.tableInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: tableName}
	}

	return err
}

// Replace replaces tableName table information with the new info.
func (s *CatalogTable) ReplaceTable(tx *database.Transaction, tableName string, info *database.TableInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(tableName), s.tableInfoToDocument(info))
	return err
}

func (s *CatalogTable) DeleteTable(tx *database.Transaction, tableName string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(tableName))
}

func (s *CatalogTable) InsertIndex(tx *database.Transaction, info *database.IndexInfo) error {
	tb := s.GetTable(tx)

	if info.StoreName == nil {
		seq, err := tb.Store.NextSequence()
		if err != nil {
			return err
		}

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, seq)
		info.StoreName = buf[:n]
	}

	_, err := tb.Insert(s.indexInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	return err
}

func (s *CatalogTable) ReplaceIndex(tx *database.Transaction, indexName string, info *database.IndexInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(indexName), s.indexInfoToDocument(info))
	return err
}

func (s *CatalogTable) DeleteIndex(tx *database.Transaction, indexName string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(indexName))
}

func (s *CatalogTable) InsertSequence(tx *database.Transaction, info *database.SequenceInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Insert(s.sequenceInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: info.Name}
	}

	return err
}

func (s *CatalogTable) ReplaceSequence(tx *database.Transaction, name string, info *database.SequenceInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(name), s.sequenceInfoToDocument(info))
	return err
}

func (s *CatalogTable) DeleteSequence(tx *database.Transaction, name string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(name))
}

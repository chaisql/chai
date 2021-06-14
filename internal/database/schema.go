package database

import (
	"encoding/binary"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/stringutil"
)

const (
	internalPrefix = "__genji_"
)

const (
	SchemaTableName      = internalPrefix + "schema"
	SchemaTableTableType = "table"
	SchemaTableIndexType = "index"
)

type SchemaTable struct {
	info *TableInfo
}

func NewSchemaTable(tx *Transaction) *SchemaTable {
	return &SchemaTable{
		info: &TableInfo{
			TableName: SchemaTableName,
			StoreName: []byte(SchemaTableName),
			FieldConstraints: []*FieldConstraint{
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

func (s *SchemaTable) Init(tx *Transaction) error {
	_, err := tx.Tx.GetStore([]byte(SchemaTableName))
	if err == engine.ErrStoreNotFound {
		err = tx.Tx.CreateStore([]byte(SchemaTableName))
	}
	return err
}

func (s *SchemaTable) GetSchemaTable(tx *Transaction) *Table {
	st, err := tx.Tx.GetStore([]byte(SchemaTableName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", SchemaTableName, err))
	}

	return &Table{
		Tx:    tx,
		Store: st,
		Info:  s.info,
	}
}

func (s *SchemaTable) tableInfoToDocument(ti *TableInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(ti.TableName))
	buf.Add("type", document.NewTextValue(SchemaTableTableType))
	buf.Add("store_name", document.NewBlobValue(ti.StoreName))
	buf.Add("sql", document.NewTextValue(ti.String()))
	return buf
}

func (s *SchemaTable) indexInfoToDocument(i *IndexInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(i.IndexName))
	buf.Add("type", document.NewTextValue(SchemaTableIndexType))
	buf.Add("store_name", document.NewBlobValue(i.StoreName))
	buf.Add("table_name", document.NewTextValue(i.TableName))
	buf.Add("sql", document.NewTextValue(i.String()))
	if i.ConstraintPath != nil {
		buf.Add("constraint_path", document.NewTextValue(i.ConstraintPath.String()))
	}

	return buf
}

// insertTable inserts a new tableInfo for the given table name.
// If info.StoreName is nil, it generates one and stores it in info.
func (s *SchemaTable) insertTable(tx *Transaction, tableName string, info *TableInfo) error {
	tb := s.GetSchemaTable(tx)

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
func (s *SchemaTable) replaceTable(tx *Transaction, tableName string, info *TableInfo) error {
	tb := s.GetSchemaTable(tx)

	_, err := tb.Replace([]byte(tableName), s.tableInfoToDocument(info))
	return err
}

func (s *SchemaTable) deleteTable(tx *Transaction, tableName string) error {
	tb := s.GetSchemaTable(tx)

	return tb.Delete([]byte(tableName))
}

func (s *SchemaTable) insertIndex(tx *Transaction, info *IndexInfo) error {
	tb := s.GetSchemaTable(tx)

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

func (s *SchemaTable) replaceIndex(tx *Transaction, indexName string, info *IndexInfo) error {
	tb := s.GetSchemaTable(tx)

	_, err := tb.Replace([]byte(indexName), s.indexInfoToDocument(info))
	return err
}

func (s *SchemaTable) deleteIndex(tx *Transaction, indexName string) error {
	tb := s.GetSchemaTable(tx)

	return tb.Delete([]byte(indexName))
}

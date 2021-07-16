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
	"github.com/genjidb/genji/types"
)

func relationToDocument(r Relation) types.Document {
	switch t := r.(type) {
	case *database.TableInfo:
		return tableInfoToDocument(t)
	case *database.IndexInfo:
		return indexInfoToDocument(t)
	case *database.Sequence:
		return sequenceInfoToDocument(t.Info)
	}

	panic(stringutil.Sprintf("objectToDocument: unknown type %q", r.Type()))
}

func tableInfoToDocument(ti *database.TableInfo) types.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", types.NewTextValue(ti.TableName))
	buf.Add("type", types.NewTextValue(RelationTableType))
	buf.Add("store_name", types.NewBlobValue(ti.StoreName))
	buf.Add("sql", types.NewTextValue(ti.String()))
	if ti.DocidSequenceName != "" {
		buf.Add("docid_sequence_name", types.NewTextValue(ti.DocidSequenceName))
	}

	return buf
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
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		ti.DocidSequenceName = v.V().(string)
	}

	return &ti, nil
}

func indexInfoToDocument(i *database.IndexInfo) types.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", types.NewTextValue(i.IndexName))
	buf.Add("type", types.NewTextValue(RelationIndexType))
	buf.Add("store_name", types.NewBlobValue(i.StoreName))
	buf.Add("table_name", types.NewTextValue(i.TableName))
	buf.Add("sql", types.NewTextValue(i.String()))
	if i.Owner.TableName != "" {
		buf.Add("owner", types.NewDocumentValue(ownerToDocument(&i.Owner)))
	}

	return buf
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
	if err != nil && err != document.ErrFieldNotFound {
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

func sequenceInfoToDocument(seq *database.SequenceInfo) types.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", types.NewTextValue(seq.Name))
	buf.Add("type", types.NewTextValue(RelationSequenceType))
	buf.Add("sql", types.NewTextValue(seq.String()))

	if seq.Owner.TableName != "" {
		owner := document.NewFieldBuffer().Add("table_name", types.NewTextValue(seq.Owner.TableName))
		if seq.Owner.Path != nil {
			owner.Add("path", types.NewTextValue(seq.Owner.Path.String()))
		}

		buf.Add("owner", types.NewDocumentValue(owner))
	}

	return buf
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
	if err != nil && err != document.ErrFieldNotFound {
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

func ownerToDocument(owner *database.Owner) types.Document {
	buf := document.NewFieldBuffer().Add("table_name", types.NewTextValue(owner.TableName))
	if owner.Path != nil {
		buf.Add("path", types.NewTextValue(owner.Path.String()))
	}

	return buf
}

func ownerFromDocument(d types.Document) (*database.Owner, error) {
	var owner database.Owner

	v, err := d.GetByField("table_name")
	if err != nil {
		return nil, err
	}

	owner.TableName = v.V().(string)

	v, err = d.GetByField("path")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		owner.Path, err = parser.ParsePath(v.V().(string))
		if err != nil {
			return nil, err
		}
	}

	return &owner, nil
}

type CatalogTable struct {
	Catalog *Catalog
	Info    *database.TableInfo
}

func NewCatalogTable(tx *database.Transaction, catalog *Catalog) *CatalogTable {
	return &CatalogTable{
		Catalog: catalog,
		Info: &database.TableInfo{
			TableName: TableName,
			StoreName: []byte(TableName),
			FieldConstraints: []*database.FieldConstraint{
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "name",
						},
					},
					Type:         types.TextValue,
					IsPrimaryKey: true,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "type",
						},
					},
					Type: types.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "table_name",
						},
					},
					Type: types.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "sql",
						},
					},
					Type: types.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "store_name",
						},
					},
					Type: types.BlobValue,
				},
			},
		},
	}
}

func (s *CatalogTable) Init(tx *database.Transaction) error {
	_, err := tx.Tx.GetStore([]byte(TableName))
	if err == engine.ErrStoreNotFound {
		err = tx.Tx.CreateStore([]byte(TableName))
	}

	return err
}

func (s *CatalogTable) Load(tx *database.Transaction) (tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.SequenceInfo, err error) {
	tb := s.Table(tx)

	err = tb.AscendGreaterOrEqual(nil, func(d types.Document) error {
		tp, err := d.GetByField("type")
		if err != nil {
			return err
		}

		switch tp.V().(string) {
		case RelationTableType:
			ti, err := tableInfoFromDocument(d)
			if err != nil {
				return err
			}
			tables = append(tables, *ti)
		case RelationIndexType:
			i, err := indexInfoFromDocument(d)
			if err != nil {
				return err
			}

			indexes = append(indexes, *i)
		case RelationSequenceType:
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

func (s *CatalogTable) Table(tx *database.Transaction) *database.Table {
	st, err := tx.Tx.GetStore([]byte(TableName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", TableName, err))
	}

	return &database.Table{
		Tx:      tx,
		Store:   st,
		Info:    s.Info,
		Catalog: s.Catalog,
	}
}

// Insert a catalog object to the table.
func (s *CatalogTable) Insert(tx *database.Transaction, r Relation) error {
	tb := s.Table(tx)

	_, err := tb.Insert(relationToDocument(r))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: r.Name()}
	}

	return err
}

// Replace a catalog object with another.
func (s *CatalogTable) Replace(tx *database.Transaction, name string, r Relation) error {
	tb := s.Table(tx)

	_, err := tb.Replace([]byte(name), relationToDocument(r))
	return err
}

func (s *CatalogTable) Delete(tx *database.Transaction, name string) error {
	tb := s.Table(tx)

	return tb.Delete([]byte(name))
}

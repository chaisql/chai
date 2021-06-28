package catalog

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/stringutil"
)

const (
	CatalogTableName         = database.InternalPrefix + "catalog"
	CatalogTableTableType    = "table"
	CatalogTableIndexType    = "index"
	CatalogTableSequenceType = "sequence"
	CatalogStoreSequence     = CatalogTableName + "_seq"
)

// Catalog manages all database objects such as tables, indexes and sequences.
// It stores all these objects in memory for fast access. Any modification
// is persisted into the __genji_catalog table.
type Catalog struct {
	Cache        *catalogCache
	CatalogTable *CatalogTable
}

func New() *Catalog {
	return &Catalog{
		Cache: newCatalogCache(),
	}
}

func (c *Catalog) Load(tx *database.Transaction) error {
	c.CatalogTable = NewCatalogTable(tx)

	// ensure the catalog table exists
	err := c.CatalogTable.Init(tx)
	if err != nil {
		return err
	}

	// load catalog information
	err = c.loadCatalog(tx)
	if err != nil {
		return err
	}

	// ensure the catalog table sequence exists
	err = c.CreateSequence(tx, &database.SequenceInfo{
		Name:        CatalogStoreSequence,
		IncrementBy: 1,
		Min:         1, Max: math.MaxInt64,
		Start: 1,
		Cache: 16,
		Owner: database.SequenceInfoOwner{
			TableName: CatalogTableName,
		},
	})
	if err != nil {
		if _, ok := err.(errs.AlreadyExistsError); !ok {
			return err
		}
	}

	return nil
}

func (c *Catalog) loadCatalog(tx *database.Transaction) error {
	tables, indexes, sequences, err := c.CatalogTable.Load(tx)
	if err != nil {
		return err
	}

	// add the __genji_catalog table to the list of tables
	// so that it can be queried
	ti := c.CatalogTable.Info.Clone()
	// make sure that table is read-only
	ti.ReadOnly = true
	tables = append(tables, *ti)

	// load tables and indexes first
	c.Cache.load(tables, indexes, nil)

	if len(sequences) > 0 {
		var seqList []database.Sequence
		seqList, err = c.loadSequences(tx, sequences)
		if err != nil {
			return err
		}

		c.Cache.load(nil, nil, seqList)
	}

	return nil
}

func (c *Catalog) loadSequences(tx *database.Transaction, info []database.SequenceInfo) ([]database.Sequence, error) {
	tb, err := c.GetTable(tx, database.SequenceTableName)
	if err != nil {
		return nil, err
	}

	sequences := make([]database.Sequence, len(info))
	for i := range info {
		d, err := tb.GetDocument([]byte(info[i].Name))
		if err != nil {
			return nil, err
		}

		sequences[i].Info = &info[i]

		v, err := d.GetByField("seq")
		if err != nil && err != document.ErrFieldNotFound {
			return nil, err
		}

		if err == nil {
			v := v.V.(int64)
			sequences[i].CurrentValue = &v
		}
	}

	return sequences, nil
}

func (c *Catalog) generateStoreName(tx *database.Transaction) ([]byte, error) {
	seq, err := c.GetSequence(CatalogStoreSequence)
	if err != nil {
		return nil, err
	}
	v, err := seq.Next(tx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(v))
	return buf[:n], nil
}

func (c *Catalog) GetTable(tx *database.Transaction, tableName string) (*database.Table, error) {
	ti, err := c.Cache.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	s, err := tx.Tx.GetStore(ti.StoreName)
	if err != nil {
		return nil, err
	}

	idxInfos := c.Cache.GetTableIndexes(tableName)
	indexes := make([]*database.Index, 0, len(idxInfos))

	for i := range idxInfos {
		indexes = append(indexes, database.NewIndex(tx.Tx, idxInfos[i].IndexName, idxInfos[i]))
	}

	return &database.Table{
		Tx:      tx,
		Name:    tableName,
		Store:   s,
		Info:    ti,
		Indexes: indexes,
	}, nil
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (c *Catalog) CreateTable(tx *database.Transaction, tableName string, info *database.TableInfo) error {
	if info == nil {
		info = new(database.TableInfo)
	}
	info.TableName = tableName

	var err error

	// replace user-defined constraints by inferred list of constraints
	info.FieldConstraints, err = info.FieldConstraints.Infer()
	if err != nil {
		return err
	}

	if info.StoreName == nil {
		info.StoreName, err = c.generateStoreName(tx)
		if err != nil {
			return err
		}
	}

	err = c.CatalogTable.InsertTable(tx, tableName, info)
	if err != nil {
		return err
	}

	err = tx.Tx.CreateStore(info.StoreName)
	if err != nil {
		return stringutil.Errorf("failed to create table %q: %w", tableName, err)
	}

	return c.Cache.AddTable(tx, info)
}

// DropTable deletes a table from the
func (c *Catalog) DropTable(tx *database.Transaction, tableName string) error {
	ti, removedIndexes, err := c.Cache.DeleteTable(tx, tableName)
	if err != nil {
		return err
	}

	for _, idx := range removedIndexes {
		err := c.dropIndex(tx, idx.IndexName)
		if err != nil {
			return err
		}
	}

	err = c.CatalogTable.DeleteTable(tx, tableName)
	if err != nil {
		return err
	}

	return tx.Tx.DropStore(ti.StoreName)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns errs.ErrIndexAlreadyExists.
func (c *Catalog) CreateIndex(tx *database.Transaction, info *database.IndexInfo) error {
	err := c.Cache.AddIndex(tx, info)
	if err != nil {
		return err
	}

	if info.StoreName == nil {
		info.StoreName, err = c.generateStoreName(tx)
		if err != nil {
			return err
		}
	}

	err = c.CatalogTable.InsertIndex(tx, info)
	if err != nil {
		return err
	}

	idx, err := c.GetIndex(tx, info.IndexName)
	if err != nil {
		return err
	}

	tb, err := c.GetTable(tx, info.TableName)
	if err != nil {
		return err
	}

	err = c.buildIndex(tx, idx, tb)
	return err
}

// GetIndex returns an index by name.
func (c *Catalog) GetIndex(tx *database.Transaction, indexName string) (*database.Index, error) {
	info, err := c.Cache.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	return database.NewIndex(tx.Tx, info.IndexName, info), nil
}

// ListIndexes returns all indexes for a given table name. If tableName is empty
// if returns a list of all indexes.
// The returned list of indexes is sorted lexicographically.
func (c *Catalog) ListIndexes(tableName string) []string {
	if tableName == "" {
		list := c.Cache.ListIndexes()
		sort.Strings(list)
		return list
	}
	idxs := c.Cache.GetTableIndexes(tableName)
	list := make([]string, 0, len(idxs))
	for _, idx := range idxs {
		list = append(list, idx.IndexName)
	}

	sort.Strings(list)
	return list
}

// DropIndex deletes an index from the database.
func (c *Catalog) DropIndex(tx *database.Transaction, name string) error {
	_, err := c.Cache.DeleteIndex(tx, name)
	if err != nil {
		return err
	}

	return c.dropIndex(tx, name)
}

func (c *Catalog) dropIndex(tx *database.Transaction, name string) error {
	err := c.CatalogTable.DeleteIndex(tx, name)
	if err != nil {
		return err
	}

	return database.NewIndex(tx.Tx, name, nil).Truncate()
}

// AddFieldConstraint adds a field constraint to a table.
func (c *Catalog) AddFieldConstraint(tx *database.Transaction, tableName string, fc database.FieldConstraint) error {
	newTi, _, err := c.Cache.updateTable(tx, tableName, func(clone *database.TableInfo) error {
		return clone.FieldConstraints.Add(&fc)
	})
	if err != nil {
		return err
	}

	return c.CatalogTable.ReplaceTable(tx, tableName, newTi)
}

// RenameTable renames a table.
// If it doesn't exist, it returns errs.ErrTableNotFound.
func (c *Catalog) RenameTable(tx *database.Transaction, oldName, newName string) error {
	newTi, newIdxs, err := c.Cache.updateTable(tx, oldName, func(clone *database.TableInfo) error {
		clone.TableName = newName
		return nil
	})
	if err != nil {
		return err
	}

	// Insert the database.TableInfo keyed by the newName name.
	err = c.CatalogTable.InsertTable(tx, newName, newTi)
	if err != nil {
		return err
	}

	if len(newIdxs) > 0 {
		for _, idx := range newIdxs {
			idx.TableName = newName
			err = c.CatalogTable.ReplaceIndex(tx, idx.IndexName, idx)
			if err != nil {
				return err
			}
		}
	}

	// Delete the old table info.
	return c.CatalogTable.DeleteTable(tx, oldName)
}

// ReIndex truncates and recreates selected index from scratch.
func (c *Catalog) ReIndex(tx *database.Transaction, indexName string) error {
	idx, err := c.GetIndex(tx, indexName)
	if err != nil {
		return err
	}

	tb, err := c.GetTable(tx, idx.Info.TableName)
	if err != nil {
		return err
	}

	err = idx.Truncate()
	if err != nil {
		return err
	}

	return c.buildIndex(tx, idx, tb)
}

func (c *Catalog) buildIndex(tx *database.Transaction, idx *database.Index, table *database.Table) error {
	return table.Iterate(func(d document.Document) error {
		var err error
		values := make([]document.Value, len(idx.Info.Paths))
		for i, path := range idx.Info.Paths {
			values[i], err = path.GetValueFromDocument(d)
			if err == document.ErrFieldNotFound {
				return nil
			}
			if err != nil {
				return err
			}
		}

		err = idx.Set(values, d.(document.Keyer).RawKey())
		if err != nil {
			return stringutil.Errorf("error while building the index: %w", err)
		}

		return nil
	})
}

// ReIndexAll truncates and recreates all indexes of the database from scratch.
func (c *Catalog) ReIndexAll(tx *database.Transaction) error {
	indexes := c.Cache.ListIndexes()

	for _, indexName := range indexes {
		err := c.ReIndex(tx, indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Catalog) GetSequence(name string) (*database.Sequence, error) {
	seq, err := c.Cache.GetSequence(name)
	if err != nil {
		return nil, err
	}

	return seq, nil
}

// CreateSequence creates a sequence with the given name.
func (c *Catalog) CreateSequence(tx *database.Transaction, info *database.SequenceInfo) error {
	if info == nil {
		info = new(database.SequenceInfo)
	}

	seq := database.Sequence{
		Info: info,
	}

	err := c.Cache.AddSequence(tx, &seq)
	if err != nil {
		return err
	}

	err = c.CatalogTable.InsertSequence(tx, info)
	if err != nil {
		return err
	}

	return seq.Init(tx)
}

// DropSequence deletes a sequence from the catalog.
func (c *Catalog) DropSequence(tx *database.Transaction, name string) error {
	_, err := c.Cache.DeleteSequence(tx, name)
	if err != nil {
		return err
	}

	return c.CatalogTable.DeleteSequence(tx, name)
}

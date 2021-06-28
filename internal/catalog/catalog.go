package catalog

import (
	"encoding/binary"
	"errors"
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
	CatalogStoreSequence     = database.InternalPrefix + "store_seq"
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

		v, err := d.GetByField("seq")
		if err != nil && err != document.ErrFieldNotFound {
			return nil, err
		}

		var currentValue *int64
		if err == nil {
			v := v.V.(int64)
			currentValue = &v

		}

		sequences[i] = database.NewSequence(&info[i], currentValue)
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
	o, err := c.Cache.Get(CatalogTableTableType, tableName)
	if err != nil {
		return nil, err
	}

	ti := o.(*database.TableInfo)

	s, err := tx.Tx.GetStore(ti.StoreName)
	if err != nil {
		return nil, err
	}

	idxInfos := c.Cache.GetTableIndexes(tableName)
	indexes := make(database.Indexes, 0, len(idxInfos))

	for i := range idxInfos {
		indexes = append(indexes, database.NewIndex(tx.Tx, idxInfos[i].IndexName, idxInfos[i]))
	}

	sort.Sort(indexes)

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

	if info.TableName == "" {
		return errors.New("table name required")
	}

	_, err := c.GetTable(tx, tableName)
	if err != nil && !errs.IsNotFoundError(err) {
		return err
	}
	if err == nil {
		return errs.AlreadyExistsError{Name: tableName}
	}

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

	err = c.CatalogTable.Insert(tx, info)
	if err != nil {
		return err
	}

	err = tx.Tx.CreateStore(info.StoreName)
	if err != nil {
		return stringutil.Errorf("failed to create table %q: %w", tableName, err)
	}

	return c.Cache.Add(tx, info)
}

// DropTable deletes a table from the catalog
func (c *Catalog) DropTable(tx *database.Transaction, tableName string) error {
	o, err := c.Cache.Get(CatalogTableTableType, tableName)
	if err != nil {
		return err
	}
	ti := o.(*database.TableInfo)

	if ti.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	for _, idx := range c.Cache.GetTableIndexes(tableName) {
		_, err = c.Cache.Delete(tx, CatalogTableIndexType, idx.IndexName)
		if err != nil {
			return err
		}

		err = c.dropIndex(tx, idx.IndexName)
		if err != nil {
			return err
		}
	}

	_, err = c.Cache.Delete(tx, CatalogTableTableType, tableName)
	if err != nil {
		return err
	}

	err = c.CatalogTable.Delete(tx, tableName)
	if err != nil {
		return err
	}

	return tx.Tx.DropStore(ti.StoreName)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns errs.ErrIndexAlreadyExists.
func (c *Catalog) CreateIndex(tx *database.Transaction, info *database.IndexInfo) error {
	// get the associated table
	o, err := c.Cache.Get(CatalogTableTableType, info.TableName)
	if err != nil {
		return err
	}
	ti := o.(*database.TableInfo)

	// if the index is created on a field on which we know the type then create a typed index.
	// if the given info contained existing types, they are overriden.
	info.Types = nil

OUTER:
	for _, path := range info.Paths {
		for _, fc := range ti.FieldConstraints {
			if fc.Path.IsEqual(path) {
				// a constraint may or may not enforce a type
				if fc.Type != 0 {
					info.Types = append(info.Types, document.ValueType(fc.Type))
				}

				continue OUTER
			}
		}

		// no type was inferred for that path, add it to the index as untyped
		info.Types = append(info.Types, document.ValueType(0))
	}

	if info.StoreName == nil {
		info.StoreName, err = c.generateStoreName(tx)
		if err != nil {
			return err
		}
	}

	err = c.Cache.Add(tx, info)
	if err != nil {
		return err
	}

	err = c.CatalogTable.Insert(tx, info)
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
	o, err := c.Cache.Get(CatalogTableIndexType, indexName)
	if err != nil {
		return nil, err
	}
	info := o.(*database.IndexInfo)

	return database.NewIndex(tx.Tx, info.IndexName, info), nil
}

// ListIndexes returns all indexes for a given table name. If tableName is empty
// if returns a list of all indexes.
// The returned list of indexes is sorted lexicographically.
func (c *Catalog) ListIndexes(tableName string) []string {
	if tableName == "" {
		list := c.Cache.ListObjects(CatalogTableIndexType)
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
	// check if the index exists
	o, err := c.Cache.Get(CatalogTableIndexType, name)
	if err != nil {
		return err
	}

	info := o.(*database.IndexInfo)

	// check if the index has been created by a table constraint
	if info.ConstraintPath != nil {
		return stringutil.Errorf("cannot drop index %s because constraint on %s(%s) requires it", info.IndexName, info.TableName, info.ConstraintPath)
	}

	_, err = c.Cache.Delete(tx, CatalogTableIndexType, name)
	if err != nil {
		return err
	}

	return c.dropIndex(tx, name)
}

func (c *Catalog) dropIndex(tx *database.Transaction, name string) error {
	err := c.CatalogTable.Delete(tx, name)
	if err != nil {
		return err
	}

	return database.NewIndex(tx.Tx, name, nil).Truncate()
}

// AddFieldConstraint adds a field constraint to a table.
func (c *Catalog) AddFieldConstraint(tx *database.Transaction, tableName string, fc database.FieldConstraint) error {
	o, err := c.Cache.Get(CatalogTableTableType, tableName)
	if err != nil {
		return err
	}
	ti := o.(*database.TableInfo)

	clone := ti.Clone()
	err = clone.FieldConstraints.Add(&fc)
	if err != nil {
		return err
	}

	err = c.Cache.Replace(tx, clone)
	if err != nil {
		return err
	}

	return c.CatalogTable.Replace(tx, tableName, clone)
}

// RenameTable renames a table.
// If it doesn't exist, it returns errs.ErrTableNotFound.
func (c *Catalog) RenameTable(tx *database.Transaction, oldName, newName string) error {
	// Delete the old table info.
	err := c.CatalogTable.Delete(tx, oldName)
	if err == errs.ErrDocumentNotFound {
		return errs.NotFoundError{Name: oldName}
	}
	if err != nil {
		return err
	}

	o, err := c.Cache.Delete(tx, CatalogTableTableType, oldName)
	if err != nil {
		return err
	}

	ti := o.(*database.TableInfo)

	clone := ti.Clone()
	clone.TableName = newName

	err = c.CatalogTable.Insert(tx, clone)
	if err != nil {
		return err
	}

	err = c.Cache.Add(tx, clone)
	if err != nil {
		return err
	}

	for _, idx := range c.Cache.GetTableIndexes(oldName) {
		o, err := c.Cache.Delete(tx, CatalogTableIndexType, idx.IndexName)
		if err != nil {
			return err
		}
		info := o.(*database.IndexInfo)

		idxClone := info.Clone()
		idxClone.TableName = clone.TableName

		err = c.Cache.Add(tx, idxClone)
		if err != nil {
			return err
		}

		err = c.CatalogTable.Replace(tx, idx.IndexName, idx)
		if err != nil {
			return err
		}
	}

	return nil
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
	indexes := c.Cache.ListObjects(CatalogTableIndexType)

	for _, indexName := range indexes {
		err := c.ReIndex(tx, indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Catalog) GetSequence(name string) (*database.Sequence, error) {
	o, err := c.Cache.Get(CatalogTableSequenceType, name)
	if err != nil {
		return nil, err
	}

	return o.(*database.Sequence), nil
}

// CreateSequence creates a sequence with the given name.
func (c *Catalog) CreateSequence(tx *database.Transaction, info *database.SequenceInfo) error {
	if info == nil {
		info = new(database.SequenceInfo)
	}

	if info.Name == "" && info.Owner.TableName == "" {
		return errors.New("sequence name not provided")
	}

	seq := database.Sequence{
		Info: info,
	}

	err := c.Cache.Add(tx, &seq)
	if err != nil {
		return err
	}

	err = c.CatalogTable.Insert(tx, &seq)
	if err != nil {
		return err
	}

	return seq.Init(tx)
}

// DropSequence deletes a sequence from the catalog.
func (c *Catalog) DropSequence(tx *database.Transaction, name string) error {
	o, err := c.Cache.Delete(tx, CatalogTableSequenceType, name)
	if err != nil {
		return err
	}

	seq := o.(*database.Sequence)
	err = seq.Drop(tx)
	if err != nil {
		return err
	}

	return c.CatalogTable.Delete(tx, name)
}

// ListSequences returns all sequence names sorted lexicographically.
func (c *Catalog) ListSequences() []string {
	return c.Cache.ListObjects(CatalogTableSequenceType)
}

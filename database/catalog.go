package database

import (
	"encoding/binary"
	"errors"
	"strings"
	"sync"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/stringutil"
)

const (
	storePrefix        = 't'
	internalPrefix     = "__genji_"
	tableInfoStoreName = internalPrefix + "tables"
	indexInfoStoreName = internalPrefix + "indexes"
)

// Catalog holds all table and index informations.
type Catalog struct {
	cache *catalogCache
}

func NewCatalog() *Catalog {
	return &Catalog{
		cache: newCatalogCache(),
	}
}

func (c *Catalog) Load(tables []TableInfo, indexes []IndexInfo) {
	tables = append(tables, TableInfo{
		TableName: tableInfoStoreName,
		StoreName: []byte(tableInfoStoreName),
		ReadOnly:  true,
		FieldConstraints: []*FieldConstraint{
			{
				Path: document.Path{
					document.PathFragment{
						FieldName: "table_name",
					},
				},
				Type:         document.TextValue,
				IsPrimaryKey: true,
			},
		},
	})

	tables = append(tables, TableInfo{
		TableName: indexInfoStoreName,
		StoreName: []byte(indexInfoStoreName),
		ReadOnly:  true,
		FieldConstraints: []*FieldConstraint{
			{
				Path: document.Path{
					document.PathFragment{
						FieldName: "index_name",
					},
				},
				Type:         document.TextValue,
				IsPrimaryKey: true,
			},
		},
	})

	c.cache.load(tables, indexes)
}

// Clone the catalog. Mostly used for testing purposes.
func (c *Catalog) Clone() *Catalog {
	var clone Catalog

	clone.cache = c.cache.clone()

	return &clone
}

func (c *Catalog) GetTable(tx *Transaction, tableName string) (*Table, error) {
	ti, err := c.cache.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	s, err := tx.Tx.GetStore(ti.StoreName)
	if err != nil {
		return nil, err
	}

	idxInfos := c.cache.GetTableIndexes(tableName)
	indexes := make([]*Index, 0, len(idxInfos))

	for i := range idxInfos {
		indexes = append(indexes, NewIndex(tx.Tx, idxInfos[i].IndexName, idxInfos[i]))
	}

	return &Table{
		Tx:      tx,
		Name:    tableName,
		Store:   s,
		Info:    ti,
		Indexes: indexes,
	}, nil
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (c *Catalog) CreateTable(tx *Transaction, tableName string, info *TableInfo) error {
	if strings.HasPrefix(tableName, internalPrefix) {
		return stringutil.Errorf("table name must not start with %s", internalPrefix)
	}

	if info == nil {
		info = new(TableInfo)
	}
	info.TableName = tableName

	var err error

	// replace user-defined constraints by inferred list of constraints
	info.FieldConstraints, err = info.FieldConstraints.Infer()
	if err != nil {
		return err
	}

	err = insertTable(tx, tableName, info)
	if err != nil {
		return err
	}

	err = tx.Tx.CreateStore(info.StoreName)
	if err != nil {
		return stringutil.Errorf("failed to create table %q: %w", tableName, err)
	}

	return c.cache.AddTable(tx, info)
}

// DropTable deletes a table from the
func (c *Catalog) DropTable(tx *Transaction, tableName string) error {
	ti, removedIndexes, err := c.cache.DeleteTable(tx, tableName)
	if err != nil {
		return err
	}

	for _, idx := range removedIndexes {
		err := c.dropIndex(tx, idx.IndexName)
		if err != nil {
			return err
		}
	}

	err = deleteTable(tx, tableName)
	if err != nil {
		return err
	}

	return tx.Tx.DropStore(ti.StoreName)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrIndexAlreadyExists.
func (c *Catalog) CreateIndex(tx *Transaction, info *IndexInfo) error {
	if strings.HasPrefix(info.IndexName, internalPrefix) {
		return stringutil.Errorf("table name must not start with %s", internalPrefix)
	}

	err := insertIndex(tx, info)
	if err != nil {
		return err
	}

	err = c.cache.AddIndex(tx, info)
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
func (c *Catalog) GetIndex(tx *Transaction, indexName string) (*Index, error) {
	info, err := c.cache.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	return NewIndex(tx.Tx, info.IndexName, info), nil
}

// ListIndexes returns an index by name.
func (c *Catalog) ListIndexes(tableName string) []string {
	if tableName == "" {
		return c.cache.ListIndexes()
	}
	idxs := c.cache.GetTableIndexes(tableName)
	names := make([]string, 0, len(idxs))
	for _, idx := range idxs {
		names = append(names, idx.IndexName)
	}

	return names
}

// DropIndex deletes an index from the database.
func (c *Catalog) DropIndex(tx *Transaction, name string) error {
	_, err := c.cache.DeleteIndex(tx, name)
	if err != nil {
		return err
	}

	return c.dropIndex(tx, name)
}

func (c *Catalog) dropIndex(tx *Transaction, name string) error {
	err := deleteIndex(tx, name)
	if err != nil {
		return err
	}

	return NewIndex(tx.Tx, name, nil).Truncate()
}

// AddFieldConstraint adds a field constraint to a table.
func (c *Catalog) AddFieldConstraint(tx *Transaction, tableName string, fc FieldConstraint) error {
	newTi, _, err := c.cache.updateTable(tx, tableName, func(clone *TableInfo) error {
		return clone.FieldConstraints.Add(&fc)
	})
	if err != nil {
		return err
	}

	return replaceTable(tx, tableName, newTi)
}

// RenameTable renames a table.
// If it doesn't exist, it returns ErrTableNotFound.
func (c *Catalog) RenameTable(tx *Transaction, oldName, newName string) error {
	newTi, newIdxs, err := c.cache.updateTable(tx, oldName, func(clone *TableInfo) error {
		clone.TableName = newName
		return nil
	})
	if err != nil {
		return err
	}

	// Insert the TableInfo keyed by the newName name.
	err = insertTable(tx, newName, newTi)
	if err != nil {
		return err
	}

	if len(newIdxs) > 0 {
		for _, idx := range newIdxs {
			idx.TableName = newName
			err = replaceIndex(tx, idx.IndexName, *idx)
			if err != nil {
				return err
			}
		}
	}

	// Delete the old table info.
	return deleteTable(tx, oldName)
}

// ReIndex truncates and recreates selected index from scratch.
func (c *Catalog) ReIndex(tx *Transaction, indexName string) error {
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

func (c *Catalog) buildIndex(tx *Transaction, idx *Index, table *Table) error {
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
func (c *Catalog) ReIndexAll(tx *Transaction) error {
	indexes := c.cache.ListIndexes()

	for _, indexName := range indexes {
		err := c.ReIndex(tx, indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

type catalogCache struct {
	tables           map[string]*TableInfo
	indexes          map[string]*IndexInfo
	indexesPerTables map[string][]*IndexInfo

	mu sync.RWMutex
}

func newCatalogCache() *catalogCache {
	return &catalogCache{
		tables:           make(map[string]*TableInfo),
		indexes:          make(map[string]*IndexInfo),
		indexesPerTables: make(map[string][]*IndexInfo),
	}
}

func (c *catalogCache) load(tables []TableInfo, indexes []IndexInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range tables {
		c.tables[tables[i].TableName] = &tables[i]
	}

	for i := range indexes {
		c.indexes[indexes[i].IndexName] = &indexes[i]
		c.indexesPerTables[indexes[i].TableName] = append(c.indexesPerTables[indexes[i].TableName], &indexes[i])
	}
}

func (c *catalogCache) clone() *catalogCache {
	clone := newCatalogCache()

	for k, v := range c.tables {
		clone.tables[k] = v
	}
	for k, v := range c.indexes {
		clone.indexes[k] = v
	}
	for k, v := range c.indexesPerTables {
		clone.indexesPerTables[k] = v
	}

	return clone
}

func (c *catalogCache) AddTable(tx *Transaction, info *TableInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.tables[info.TableName]; ok {
		return ErrTableAlreadyExists
	}

	c.tables[info.TableName] = info

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.tables, info.TableName)
	})

	return nil
}

func (c *catalogCache) DeleteTable(tx *Transaction, tableName string) (*TableInfo, []*IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, nil, ErrTableNotFound
	}

	if ti.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	delete(c.tables, tableName)
	delete(c.indexesPerTables, tableName)
	var removedIndexes []*IndexInfo

	for _, idx := range c.indexes {
		if idx.TableName != tableName {
			continue
		}

		removedIndexes = append(removedIndexes, idx)
	}

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.tables[tableName] = ti

		for _, idx := range removedIndexes {
			c.indexes[idx.IndexName] = idx
		}

		if len(removedIndexes) > 0 {
			c.indexesPerTables[tableName] = removedIndexes
		}
	})

	return ti, removedIndexes, nil
}

func (c *catalogCache) GetTable(tableName string) (*TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, ErrTableNotFound
	}

	return ti, nil
}

func (c *catalogCache) AddIndex(tx *Transaction, info *IndexInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.indexes[info.IndexName]; ok {
		return ErrIndexAlreadyExists
	}

	ti, ok := c.tables[info.TableName]
	if !ok {
		return ErrTableNotFound
	}

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

	c.indexes[info.IndexName] = info
	previousIndexes := c.indexesPerTables[info.TableName]
	c.indexesPerTables[info.TableName] = append(c.indexesPerTables[info.TableName], info)

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.indexes, info.IndexName)

		if len(previousIndexes) == 0 {
			delete(c.indexesPerTables, info.TableName)
		} else {
			c.indexesPerTables[info.TableName] = previousIndexes
		}
	})

	return nil
}

func (c *catalogCache) DeleteIndex(tx *Transaction, indexName string) (*IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if the index exists
	info, ok := c.indexes[indexName]
	if !ok {
		return nil, ErrIndexNotFound
	}

	// remove it from the global map of indexes
	delete(c.indexes, indexName)

	// build a new list of indexes for the related table.
	// the previous list must not be modified.
	newIndexlist := make([]*IndexInfo, 0, len(c.indexesPerTables[info.TableName]))
	for _, idx := range c.indexesPerTables[info.TableName] {
		if idx.IndexName != indexName {
			newIndexlist = append(newIndexlist, idx)
		}
	}

	oldIndexList := c.indexesPerTables[info.TableName]
	c.indexesPerTables[info.TableName] = newIndexlist

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.indexes[indexName] = info
		c.indexesPerTables[info.TableName] = oldIndexList
	})

	return info, nil
}

func (c *catalogCache) GetIndex(indexName string) (*IndexInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.indexes[indexName]
	if !ok {
		return nil, ErrIndexNotFound
	}

	return info, nil
}

func (c *catalogCache) ListIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	indexes := make([]string, 0, len(c.indexes))
	for name := range c.indexes {
		indexes = append(indexes, name)
	}

	return indexes
}

func (c *catalogCache) GetTableIndexes(tableName string) []*IndexInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.indexesPerTables[tableName]
}

func (c *catalogCache) updateTable(tx *Transaction, tableName string, fn func(clone *TableInfo) error) (*TableInfo, []*IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, nil, ErrTableNotFound
	}

	if ti.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	clone := ti.Clone()
	err := fn(clone)
	if err != nil {
		return nil, nil, err
	}

	var oldIndexes, newIndexes []*IndexInfo
	// if the table has been renamed, we need to modify indexes
	// to point to the new table
	if clone.TableName != tableName {
		// delete the old table name from the table list
		delete(c.tables, tableName)

		for _, idx := range c.indexesPerTables[tableName] {
			idxClone := idx.Clone()
			idxClone.TableName = clone.TableName
			newIndexes = append(newIndexes, idxClone)
			oldIndexes = append(oldIndexes, idx)
			c.indexes[idxClone.IndexName] = idxClone
		}
		if len(newIndexes) > 0 {
			c.indexesPerTables[clone.TableName] = newIndexes
			delete(c.indexesPerTables, tableName)
		}
	}

	c.tables[clone.TableName] = clone

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.tables, clone.TableName)
		c.tables[tableName] = ti

		for _, idx := range oldIndexes {
			c.indexes[idx.IndexName] = idx
		}

		if clone.TableName != tableName {
			delete(c.indexesPerTables, clone.TableName)
			c.indexesPerTables[tableName] = oldIndexes
		}
	})

	return clone, newIndexes, nil
}

func initStores(tx *Transaction) error {
	_, err := tx.Tx.GetStore([]byte(tableInfoStoreName))
	if err == engine.ErrStoreNotFound {
		err = tx.Tx.CreateStore([]byte(tableInfoStoreName))
	}
	if err != nil {
		return err
	}

	_, err = tx.Tx.GetStore([]byte(indexInfoStoreName))
	if err == engine.ErrStoreNotFound {
		err = tx.Tx.CreateStore([]byte(indexInfoStoreName))
	}
	return err
}

func GetTableStore(tx *Transaction) *Table {
	st, err := tx.Tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", tableInfoStoreName, err))
	}

	return &Table{
		Tx:    tx,
		Store: st,
		Info: &TableInfo{
			TableName: tableInfoStoreName,
			StoreName: []byte(tableInfoStoreName),
			FieldConstraints: []*FieldConstraint{
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "statements",
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
					Type:         document.TextValue,
					IsPrimaryKey: true,
				},
			},
		},
	}
}

func GetIndexStore(tx *Transaction) *Table {
	st, err := tx.Tx.GetStore([]byte(indexInfoStoreName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", indexInfoStoreName, err))
	}

	return &Table{
		Tx:    tx,
		Store: st,
		Info: &TableInfo{
			TableName: indexInfoStoreName,
			StoreName: []byte(indexInfoStoreName),
			FieldConstraints: []*FieldConstraint{
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "statements",
						},
					},
					Type: document.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "index_name",
						},
					},
					Type:         document.TextValue,
					IsPrimaryKey: true,
				},
			},
		},
	}
}

// insertTable a new tableInfo for the given table name.
// If info.StoreName is nil, it generates one and stores it in info.
func insertTable(tx *Transaction, tableName string, info *TableInfo) error {
	tb := GetTableStore(tx)

	if info.StoreName == nil {
		seq, err := tb.Store.NextSequence()
		if err != nil {
			return err
		}
		buf := make([]byte, binary.MaxVarintLen64+1)
		buf[0] = storePrefix
		n := binary.PutUvarint(buf[1:], seq)
		info.StoreName = buf[:n+1]
	}

	_, err := tb.Insert(info.ToDocument())
	if err == ErrDuplicateDocument {
		return ErrTableAlreadyExists
	}

	return err
}

func deleteTable(tx *Transaction, tableName string) error {
	tb := GetTableStore(tx)

	return tb.Delete([]byte(tableName))
}

// Replace replaces tableName table information with the new info.
func replaceTable(tx *Transaction, tableName string, info *TableInfo) error {
	tb := GetTableStore(tx)

	return tb.Replace([]byte(tableName), info.ToDocument())
}

func insertIndex(tx *Transaction, info *IndexInfo) error {
	tb := GetIndexStore(tx)

	// auto-generate index name
	if info.IndexName == "" {
		seq, err := tb.Store.NextSequence()
		if err != nil {
			return err
		}

		info.IndexName = stringutil.Sprintf("%sautoindex_%s_%d", internalPrefix, info.TableName, seq)
	}

	_, err := tb.Insert(info.ToDocument())
	if err == ErrDuplicateDocument {
		return ErrIndexAlreadyExists
	}

	return err
}

func replaceIndex(tx *Transaction, indexName string, info IndexInfo) error {
	tb := GetIndexStore(tx)

	return tb.Replace([]byte(indexName), info.ToDocument())
}

func deleteIndex(tx *Transaction, indexName string) error {
	tb := GetIndexStore(tx)

	return tb.Delete([]byte(indexName))
}

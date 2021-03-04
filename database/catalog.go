package database

import (
	"errors"
	"strings"
	"sync"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/stringutil"
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

func (c *Catalog) Load(tx *Transaction) error {
	tables, err := tx.getTableStore().ListAll()
	if err != nil {
		return err
	}

	indexes, err := tx.getIndexStore().ListAll()
	if err != nil {
		return err
	}

	tables = append(tables, &TableInfo{
		tableName: tableInfoStoreName,
		storeName: []byte(tableInfoStoreName),
		readOnly:  true,
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

	tables = append(tables, &TableInfo{
		tableName: indexStoreName,
		storeName: []byte(indexStoreName),
		readOnly:  true,
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
	return nil
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

	s, err := tx.tx.GetStore(ti.storeName)
	if err != nil {
		return nil, err
	}

	idxInfos := c.cache.GetTableIndexes(tableName)
	indexes := make([]*Index, 0, len(idxInfos))

	for i := range idxInfos {
		indexes = append(indexes, NewIndex(tx.tx, idxInfos[i].IndexName, idxInfos[i]))
	}

	return &Table{
		tx:      tx,
		Store:   s,
		name:    tableName,
		indexes: indexes,
		info:    ti,
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
	info.tableName = tableName

	var err error

	// replace user-defined constraints by inferred list of constraints
	info.FieldConstraints, err = info.FieldConstraints.Infer()
	if err != nil {
		return err
	}

	err = c.cache.AddTable(tx, info)
	if err != nil {
		return err
	}

	err = tx.getTableStore().Insert(tx, tableName, info)
	if err != nil {
		return err
	}

	err = tx.tx.CreateStore(info.storeName)
	if err != nil {
		return stringutil.Errorf("failed to create table %q: %w", tableName, err)
	}

	return nil
}

// DropTable deletes a table from the database.
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

	err = tx.getTableStore().Delete(tx, tableName)
	if err != nil {
		return err
	}

	return tx.tx.DropStore(ti.storeName)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrIndexAlreadyExists.
func (c *Catalog) CreateIndex(tx *Transaction, opts *IndexInfo) error {
	if strings.HasPrefix(opts.IndexName, internalPrefix) {
		return stringutil.Errorf("table name must not start with %s", internalPrefix)
	}

	// auto-generate index name
	if opts.IndexName == "" {
		seq, err := tx.getIndexStore().st.NextSequence()
		if err != nil {
			return err
		}

		opts.IndexName = stringutil.Sprintf("%sautoindex_%s_%d", internalPrefix, opts.TableName, seq)
	}

	err := c.cache.AddIndex(tx, opts)
	if err != nil {
		return err
	}

	err = tx.getIndexStore().Insert(opts)
	if err != nil {
		return err
	}

	idx, err := c.GetIndex(tx, opts.IndexName)
	if err != nil {
		return err
	}

	tb, err := c.GetTable(tx, opts.TableName)
	if err != nil {
		return err
	}

	return c.buildIndex(tx, idx, tb)
}

// GetIndex returns an index by name.
func (c *Catalog) GetIndex(tx *Transaction, indexName string) (*Index, error) {
	info, err := c.cache.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	return NewIndex(tx.tx, info.IndexName, info), nil
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
	indexStore := tx.getIndexStore()

	opts, err := indexStore.Get(name)
	if err != nil {
		return err
	}
	err = indexStore.Delete(name)
	if err != nil {
		return err
	}

	idx := NewIndex(tx.tx, opts.IndexName, opts)

	return idx.Truncate()
}

// AddFieldConstraint adds a field constraint to a table.
func (c *Catalog) AddFieldConstraint(tx *Transaction, tableName string, fc FieldConstraint) error {
	newTi, _, err := c.cache.updateTable(tx, tableName, func(clone *TableInfo) error {
		return clone.FieldConstraints.Add(&fc)
	})
	if err != nil {
		return err
	}

	return tx.getTableStore().Replace(tx, tableName, newTi)
}

// RenameTable renames a table.
// If it doesn't exist, it returns ErrTableNotFound.
func (c *Catalog) RenameTable(tx *Transaction, oldName, newName string) error {
	newTi, newIdxs, err := c.cache.updateTable(tx, oldName, func(clone *TableInfo) error {
		clone.tableName = newName
		return nil
	})
	if err != nil {
		return err
	}

	tableStore := tx.getTableStore()

	// Insert the TableInfo keyed by the newName name.
	err = tableStore.Insert(tx, newName, newTi)
	if err != nil {
		return err
	}

	if len(newIdxs) > 0 {
		indexStore := tx.getIndexStore()

		for _, idx := range newIdxs {
			idx.TableName = newName
			err = indexStore.Replace(idx.IndexName, *idx)
			if err != nil {
				return err
			}
		}
	}

	// Delete the old reference from the tableInfoStore.
	return tableStore.Delete(tx, oldName)
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
		// TODO
		v, err := idx.Info.Paths[0].GetValueFromDocument(d)
		if err == document.ErrFieldNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		// TODO
		err = idx.Set([]document.Value{v}, d.(document.Keyer).RawKey())
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

func (c *catalogCache) load(tables []*TableInfo, indexes []*IndexInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range tables {
		c.tables[t.tableName] = t
	}

	for _, i := range indexes {
		c.indexes[i.IndexName] = i
		c.indexesPerTables[i.TableName] = append(c.indexesPerTables[i.TableName], i)
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

	if _, ok := c.tables[info.tableName]; ok {
		return ErrTableAlreadyExists
	}

	c.tables[info.tableName] = info

	tx.onRollbackHooks = append(tx.onRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.tables, info.tableName)
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

	if ti.readOnly {
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

	tx.onRollbackHooks = append(tx.onRollbackHooks, func() {
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

	// if the index is created on a field on which we know the type,
	// create a typed index.
	for _, fc := range ti.FieldConstraints {
		for _, path := range info.Paths {
			if fc.Path.IsEqual(path) {
				if fc.Type != 0 {
					// TODO
					info.Types = append(info.Types, document.ValueType(fc.Type))
				}

				break
			}
		}
	}

	c.indexes[info.IndexName] = info
	previousIndexes := c.indexesPerTables[info.TableName]
	c.indexesPerTables[info.TableName] = append(c.indexesPerTables[info.TableName], info)

	tx.onRollbackHooks = append(tx.onRollbackHooks, func() {
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

	tx.onRollbackHooks = append(tx.onRollbackHooks, func() {
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

	if ti.readOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	clone := ti.Clone()
	err := fn(clone)
	if err != nil {
		return nil, nil, err
	}

	var oldIndexes, newIndexes []*IndexInfo
	if clone.tableName != tableName {
		delete(c.tables, tableName)

		for _, idx := range c.indexes {
			if idx.TableName == tableName {
				idxClone := idx.Clone()
				idxClone.TableName = clone.tableName
				newIndexes = append(newIndexes, idxClone)
				oldIndexes = append(oldIndexes, idx)
				c.indexes[idxClone.IndexName] = idxClone
			}
		}
	}

	c.tables[clone.tableName] = clone

	tx.onRollbackHooks = append(tx.onRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.tables, clone.tableName)
		c.tables[tableName] = ti

		for _, idx := range oldIndexes {
			c.indexes[idx.IndexName] = idx
		}
	})

	return clone, newIndexes, nil
}

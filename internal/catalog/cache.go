package catalog

import (
	"errors"
	"strings"
	"sync"

	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/stringutil"
)

type catalogCache struct {
	tables           map[string]*database.TableInfo
	indexes          map[string]*database.IndexInfo
	indexesPerTables map[string][]*database.IndexInfo
	sequences        map[string]*database.Sequence

	mu sync.RWMutex
}

func newCatalogCache() *catalogCache {
	return &catalogCache{
		tables:           make(map[string]*database.TableInfo),
		indexes:          make(map[string]*database.IndexInfo),
		indexesPerTables: make(map[string][]*database.IndexInfo),
		sequences:        make(map[string]*database.Sequence),
	}
}

func (c *catalogCache) load(tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.Sequence) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range tables {
		c.tables[tables[i].TableName] = &tables[i]
	}

	for i := range indexes {
		c.indexes[indexes[i].IndexName] = &indexes[i]
		c.indexesPerTables[indexes[i].TableName] = append(c.indexesPerTables[indexes[i].TableName], &indexes[i])
	}

	for i := range sequences {
		c.sequences[sequences[i].Info.Name] = &sequences[i]
	}
}

func (c *catalogCache) Clone() *catalogCache {
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
	for k, v := range c.sequences {
		clone.sequences[k] = v
	}

	return clone
}

func (c *catalogCache) AddTable(tx *database.Transaction, info *database.TableInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// checking if table exists
	if _, ok := c.tables[info.TableName]; ok {
		return errs.AlreadyExistsError{Name: info.TableName}
	}

	// checking if index exists with the same name
	if _, ok := c.indexes[info.TableName]; ok {
		return errs.AlreadyExistsError{Name: info.TableName}
	}

	// checking if sequence exists with the same name
	if _, ok := c.sequences[info.TableName]; ok {
		return errs.AlreadyExistsError{Name: info.TableName}
	}

	c.tables[info.TableName] = info

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.tables, info.TableName)
	})

	return nil
}

func (c *catalogCache) DeleteTable(tx *database.Transaction, tableName string) (*database.TableInfo, []*database.IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, nil, errs.ErrTableNotFound
	}

	if ti.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	delete(c.tables, tableName)
	delete(c.indexesPerTables, tableName)
	var removedIndexes []*database.IndexInfo

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

func (c *catalogCache) GetTable(tableName string) (*database.TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, errs.ErrTableNotFound
	}

	return ti, nil
}

func pathsToIndexName(paths []document.Path) string {
	var s strings.Builder

	for i, p := range paths {
		if i > 0 {
			s.WriteRune('_')
		}

		s.WriteString(p.String())
	}

	return s.String()
}

func (c *catalogCache) AddIndex(tx *database.Transaction, info *database.IndexInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// auto-generate index name if needed
	if info.IndexName == "" {
		info.IndexName = stringutil.Sprintf("%s_%s_idx", info.TableName, pathsToIndexName(info.Paths))
		if _, ok := c.indexes[info.IndexName]; ok {
			i := 1
			for {
				info.IndexName = stringutil.Sprintf("%s_%s_idx%d", info.TableName, pathsToIndexName(info.Paths), i)
				if _, ok := c.indexes[info.IndexName]; !ok {
					break
				}

				i++
			}
		}
	}

	// checking if index exists with the same name
	if _, ok := c.indexes[info.IndexName]; ok {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	// checking if table exists with the same name
	if _, ok := c.tables[info.IndexName]; ok {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	// checking if sequence exists with the same name
	if _, ok := c.sequences[info.IndexName]; ok {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	// get the associated table
	ti, ok := c.tables[info.TableName]
	if !ok {
		return errs.ErrTableNotFound
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

func (c *catalogCache) DeleteIndex(tx *database.Transaction, indexName string) (*database.IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if the index exists
	info, ok := c.indexes[indexName]
	if !ok {
		return nil, errs.ErrIndexNotFound
	}

	// check if the index has been created by a table constraint
	if info.ConstraintPath != nil {
		return nil, stringutil.Errorf("cannot drop index %s because constraint on %s(%s) requires it", info.IndexName, info.TableName, info.ConstraintPath)
	}

	// remove it from the global map of indexes
	delete(c.indexes, indexName)

	// build a new list of indexes for the related table.
	// the previous list must not be modified.
	newIndexlist := make([]*database.IndexInfo, 0, len(c.indexesPerTables[info.TableName]))
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

func (c *catalogCache) GetIndex(indexName string) (*database.IndexInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.indexes[indexName]
	if !ok {
		return nil, errs.ErrIndexNotFound
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

func (c *catalogCache) GetTableIndexes(tableName string) []*database.IndexInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.indexesPerTables[tableName]
}

func (c *catalogCache) updateTable(tx *database.Transaction, tableName string, fn func(clone *database.TableInfo) error) (*database.TableInfo, []*database.IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, nil, errs.ErrTableNotFound
	}

	if ti.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	clone := ti.Clone()
	err := fn(clone)
	if err != nil {
		return nil, nil, err
	}

	var oldIndexes, newIndexes []*database.IndexInfo
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

func (c *catalogCache) AddSequence(tx *database.Transaction, seq *database.Sequence) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// auto-generate sequence name if needed
	if seq.Info.Name == "" {
		if seq.Info.Owner.TableName == "" {
			return errors.New("sequence name not provided")
		}

		var sb strings.Builder
		sb.WriteString(seq.Info.Owner.TableName)
		if seq.Info.Owner.Path != nil {
			sb.WriteString("_")
			sb.WriteString(seq.Info.Owner.Path.String())
		}
		sb.WriteString("_seq")
		seq.Info.Name = sb.String()

		i := 0
		for {
			if !c.objectExists(seq.Info.Name) {
				break
			}

			i++
			seq.Info.Name = stringutil.Sprintf("%s%d", sb.String(), i)
		}
	}

	if c.objectExists(seq.Info.Name) {
		return errs.AlreadyExistsError{Name: seq.Info.Name}
	}

	c.sequences[seq.Info.Name] = seq

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.sequences, seq.Info.Name)
	})

	return nil
}

func (c *catalogCache) objectExists(name string) bool {
	// checking if sequence exists with the same name
	if _, ok := c.sequences[name]; ok {
		return true
	}

	// checking if table exists with the same name
	if _, ok := c.tables[name]; ok {
		return true
	}

	// checking if index exists with the same name
	if _, ok := c.indexes[name]; ok {
		return true
	}

	return false
}

func (c *catalogCache) GetSequence(name string) (*database.Sequence, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	seq, ok := c.sequences[name]
	if !ok {
		return nil, errs.ErrSequenceNotFound
	}

	return seq, nil
}

func (c *catalogCache) DeleteSequence(tx *database.Transaction, name string) (*database.Sequence, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if the sequence exists
	seq, ok := c.sequences[name]
	if !ok {
		return nil, errs.ErrSequenceNotFound
	}

	// remove it from the global map of sequences
	delete(c.sequences, name)

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.sequences[name] = seq
	})

	return seq, nil
}

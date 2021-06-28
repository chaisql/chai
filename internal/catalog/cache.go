package catalog

import (
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/stringutil"
)

type CatalogObject interface {
	Type() string
	Name() string
	SetName(name string)
	GenerateBaseName() string
}

type catalogCache struct {
	tables    map[string]CatalogObject
	indexes   map[string]CatalogObject
	sequences map[string]CatalogObject
}

func newCatalogCache() *catalogCache {
	return &catalogCache{
		tables:    make(map[string]CatalogObject),
		indexes:   make(map[string]CatalogObject),
		sequences: make(map[string]CatalogObject),
	}
}

func (c *catalogCache) load(tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.Sequence) {
	for i := range tables {
		c.tables[tables[i].TableName] = &tables[i]
	}

	for i := range indexes {
		c.indexes[indexes[i].IndexName] = &indexes[i]
	}

	for i := range sequences {
		c.sequences[sequences[i].Info.Name] = &sequences[i]
	}
}

// TODO put in tests
func (c *catalogCache) Clone() *catalogCache {
	clone := newCatalogCache()

	for k, v := range c.tables {
		clone.tables[k] = v
	}
	for k, v := range c.indexes {
		clone.indexes[k] = v
	}
	for k, v := range c.sequences {
		clone.sequences[k] = v
	}

	return clone
}

func (c *catalogCache) objectExists(name string) bool {
	// checking if table exists with the same name
	if _, ok := c.tables[name]; ok {
		return true
	}

	// checking if sequence exists with the same name
	if _, ok := c.sequences[name]; ok {
		return true
	}

	// checking if index exists with the same name
	if _, ok := c.indexes[name]; ok {
		return true
	}

	return false
}

func (c *catalogCache) generateUnusedName(baseName string) string {
	name := baseName
	i := 0
	for {
		if !c.objectExists(name) {
			break
		}

		i++
		name = stringutil.Sprintf("%s%d", baseName, i)
	}

	return name
}

func (c *catalogCache) getMapByType(tp string) map[string]CatalogObject {
	switch tp {
	case CatalogTableTableType:
		return c.tables
	case CatalogTableIndexType:
		return c.indexes
	case CatalogTableSequenceType:
		return c.sequences
	}

	panic(stringutil.Sprintf("unknown catalog object type %q", tp))
}

func (c *catalogCache) Add(tx *database.Transaction, o CatalogObject) error {
	name := o.Name()

	// if name is provided, ensure it's not duplicated
	if name != "" {
		_, err := c.Get(CatalogTableIndexType, name)
		if err == nil {
			return errs.AlreadyExistsError{Name: name}
		}
	} else {
		name = o.GenerateBaseName()
		name = c.generateUnusedName(name)
		o.SetName(name)
	}

	m := c.getMapByType(o.Type())
	m[name] = o

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		delete(m, name)
	})

	return nil
}

func (c *catalogCache) Replace(tx *database.Transaction, o CatalogObject) error {
	m := c.getMapByType(o.Type())

	old, ok := m[o.Name()]
	if !ok {
		return errs.NotFoundError{Name: o.Name()}
	}

	m[o.Name()] = o

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		m[o.Name()] = old
	})

	return nil
}

func (c *catalogCache) Delete(tx *database.Transaction, tp, name string) (CatalogObject, error) {
	m := c.getMapByType(tp)

	o, ok := m[name]
	if !ok {
		return nil, errs.NotFoundError{Name: name}
	}

	delete(m, name)

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		m[name] = o
	})

	return o, nil
}

func (c *catalogCache) Get(tp, name string) (CatalogObject, error) {
	m := c.getMapByType(tp)

	o, ok := m[name]
	if !ok {
		return nil, errs.NotFoundError{Name: name}
	}

	return o, nil
}

func (c *catalogCache) ListIndexes() []string {
	indexes := make([]string, 0, len(c.indexes))
	for name := range c.indexes {
		indexes = append(indexes, name)
	}

	return indexes
}

func (c *catalogCache) GetTableIndexes(tableName string) []*database.IndexInfo {
	var indexes []*database.IndexInfo
	for _, o := range c.indexes {
		idx := o.(*database.IndexInfo)
		if idx.TableName != tableName {
			continue
		}
		indexes = append(indexes, idx)
	}

	return indexes
}

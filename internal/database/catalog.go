package database

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

const (
	TableName            = InternalPrefix + "catalog"
	RelationTableType    = "table"
	RelationIndexType    = "index"
	RelationSequenceType = "sequence"
	StoreSequence        = InternalPrefix + "store_seq"
)

// Catalog manages all database objects such as tables, indexes and sequences.
// It stores all these objects in memory for fast access. Any modification
// is persisted into the __genji_catalog table.
type Catalog struct {
	Cache        *catalogCache
	CatalogTable *CatalogStore
	Codec        encoding.Codec
}

func NewCatalog() *Catalog {
	return &Catalog{
		Cache:        newCatalogCache(),
		CatalogTable: newCatalogStore(),
	}
}

func (c *Catalog) Init(tx *Transaction, codec encoding.Codec) error {
	c.Codec = codec
	err := c.CatalogTable.Init(tx, c)
	if err != nil {
		return err
	}

	// ensure the store sequence exists
	err = c.CreateSequence(tx, &SequenceInfo{
		Name:        StoreSequence,
		IncrementBy: 1,
		Min:         1, Max: math.MaxInt64,
		Start: 1,
		Cache: 16,
		Owner: Owner{
			TableName: TableName,
		},
	})
	if err != nil {
		if !errs.IsAlreadyExistsError(err) {
			return err
		}
	}

	return nil
}

func (c *Catalog) generateStoreName(tx *Transaction) ([]byte, error) {
	seq, err := c.GetSequence(StoreSequence)
	if err != nil {
		return nil, err
	}
	v, err := seq.Next(tx, c)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(v))
	return buf[:n], nil
}

func (c *Catalog) GetTable(tx *Transaction, tableName string) (*Table, error) {
	o, err := c.Cache.Get(RelationTableType, tableName)
	if err != nil {
		return nil, err
	}

	ti := o.(*TableInfo)

	s, err := tx.Tx.GetStore(ti.StoreName)
	if err != nil {
		return nil, err
	}

	return &Table{
		Tx:      tx,
		Store:   s,
		Info:    ti,
		Catalog: c,
		Codec:   c.Codec,
	}, nil
}

// GetTableInfo returns the table info for the given table name.
func (c *Catalog) GetTableInfo(tableName string) (*TableInfo, error) {
	r, err := c.Cache.Get(RelationTableType, tableName)
	if err != nil {
		return nil, err
	}

	return r.(*TableInfo), nil
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (c *Catalog) CreateTable(tx *Transaction, tableName string, info *TableInfo) error {
	if info == nil {
		info = new(TableInfo)
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
		return errors.Wrap(errs.AlreadyExistsError{Name: tableName})
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

	// bind default values with catalog
	for _, fc := range info.FieldConstraints {
		if fc.DefaultValue == nil {
			continue
		}

		fc.DefaultValue.Bind(c)
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
func (c *Catalog) DropTable(tx *Transaction, tableName string) error {
	o, err := c.Cache.Get(RelationTableType, tableName)
	if err != nil {
		return err
	}
	ti := o.(*TableInfo)

	if ti.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	for _, idx := range c.Cache.GetTableIndexes(tableName) {
		_, err = c.Cache.Delete(tx, RelationIndexType, idx.IndexName)
		if err != nil {
			return err
		}

		err = c.dropIndex(tx, idx.IndexName)
		if err != nil {
			return err
		}
	}

	_, err = c.Cache.Delete(tx, RelationTableType, tableName)
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
func (c *Catalog) CreateIndex(tx *Transaction, info *IndexInfo) error {
	// get the associated table
	o, err := c.Cache.Get(RelationTableType, info.TableName)
	if err != nil {
		return err
	}
	ti := o.(*TableInfo)

	// if the index is created on a field on which we know the type then create a typed index.
	// if the given info contained existing types, they are overriden.
	info.Types = nil

OUTER:
	for _, path := range info.Paths {
		for _, fc := range ti.FieldConstraints {
			if fc.Path.IsEqual(path) {
				// a constraint may or may not enforce a type
				if fc.Type != 0 {
					info.Types = append(info.Types, types.ValueType(fc.Type))
				}

				continue OUTER
			}
		}

		// no type was inferred for that path, add it to the index as untyped
		info.Types = append(info.Types, types.ValueType(0))
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

	err = c.BuildIndex(tx, idx, tb)
	return err
}

// GetIndex returns an index by name.
func (c *Catalog) GetIndex(tx *Transaction, indexName string) (*Index, error) {
	r, err := c.Cache.Get(RelationIndexType, indexName)
	if err != nil {
		return nil, err
	}
	info := r.(*IndexInfo)

	return NewIndex(tx.Tx, info.IndexName, info), nil
}

// GetIndexInfo returns an index info by name.
func (c *Catalog) GetIndexInfo(indexName string) (*IndexInfo, error) {
	r, err := c.Cache.Get(RelationIndexType, indexName)
	if err != nil {
		return nil, err
	}
	return r.(*IndexInfo), nil
}

// ListIndexes returns all indexes for a given table name. If tableName is empty
// if returns a list of all indexes.
// The returned list of indexes is sorted lexicographically.
func (c *Catalog) ListIndexes(tableName string) []string {
	if tableName == "" {
		list := c.Cache.ListObjects(RelationIndexType)
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

// DropIndex deletes an index from the
func (c *Catalog) DropIndex(tx *Transaction, name string) error {
	// check if the index exists
	r, err := c.Cache.Get(RelationIndexType, name)
	if err != nil {
		return err
	}

	info := r.(*IndexInfo)

	// check if the index has been created by a table constraint
	if info.Owner.Path != nil {
		return stringutil.Errorf("cannot drop index %s because constraint on %s(%s) requires it", info.IndexName, info.TableName, info.Owner.Path)
	}

	_, err = c.Cache.Delete(tx, RelationIndexType, name)
	if err != nil {
		return err
	}

	return c.dropIndex(tx, name)
}

func (c *Catalog) dropIndex(tx *Transaction, name string) error {
	err := c.CatalogTable.Delete(tx, name)
	if err != nil {
		return err
	}

	return NewIndex(tx.Tx, name, nil).Truncate()
}

// AddFieldConstraint adds a field constraint to a table.
func (c *Catalog) AddFieldConstraint(tx *Transaction, tableName string, fc FieldConstraint) error {
	r, err := c.Cache.Get(RelationTableType, tableName)
	if err != nil {
		return err
	}
	ti := r.(*TableInfo)

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
func (c *Catalog) RenameTable(tx *Transaction, oldName, newName string) error {
	// Delete the old table info.
	err := c.CatalogTable.Delete(tx, oldName)
	if errors.Is(err, errs.ErrDocumentNotFound) {
		return errors.Wrap(errs.NotFoundError{Name: oldName})
	}
	if err != nil {
		return err
	}

	o, err := c.Cache.Delete(tx, RelationTableType, oldName)
	if err != nil {
		return err
	}

	ti := o.(*TableInfo)

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
		r, err := c.Cache.Delete(tx, RelationIndexType, idx.IndexName)
		if err != nil {
			return err
		}
		info := r.(*IndexInfo)

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

	return c.BuildIndex(tx, idx, tb)
}

func (c *Catalog) BuildIndex(tx *Transaction, idx *Index, table *Table) error {
	return table.Iterate(func(d types.Document) error {
		var err error
		values := make([]types.Value, len(idx.Info.Paths))
		for i, path := range idx.Info.Paths {
			values[i], err = path.GetValueFromDocument(d)
			if errors.Is(err, document.ErrFieldNotFound) {
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
	indexes := c.Cache.ListObjects(RelationIndexType)

	for _, indexName := range indexes {
		err := c.ReIndex(tx, indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Catalog) GetSequence(name string) (*Sequence, error) {
	r, err := c.Cache.Get(RelationSequenceType, name)
	if err != nil {
		return nil, err
	}

	return r.(*Sequence), nil
}

// CreateSequence creates a sequence with the given name.
func (c *Catalog) CreateSequence(tx *Transaction, info *SequenceInfo) error {
	if info == nil {
		info = new(SequenceInfo)
	}

	if info.Name == "" && info.Owner.TableName == "" {
		return errors.New("sequence name not provided")
	}

	seq := Sequence{
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

	return seq.Init(tx, c)
}

// DropSequence deletes a sequence from the catalog.
func (c *Catalog) DropSequence(tx *Transaction, name string) error {
	r, err := c.Cache.Delete(tx, RelationSequenceType, name)
	if err != nil {
		return err
	}

	seq := r.(*Sequence)
	err = seq.Drop(tx, c)
	if err != nil {
		return err
	}

	return c.CatalogTable.Delete(tx, name)
}

// ListSequences returns all sequence names sorted lexicographically.
func (c *Catalog) ListSequences() []string {
	return c.Cache.ListObjects(RelationSequenceType)
}

type Relation interface {
	Type() string
	Name() string
	SetName(name string)
	GenerateBaseName() string
}

type catalogCache struct {
	tables    map[string]Relation
	indexes   map[string]Relation
	sequences map[string]Relation
}

func newCatalogCache() *catalogCache {
	return &catalogCache{
		tables:    make(map[string]Relation),
		indexes:   make(map[string]Relation),
		sequences: make(map[string]Relation),
	}
}

func (c *catalogCache) Load(tables []TableInfo, indexes []IndexInfo, sequences []Sequence) {
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

func (c *catalogCache) getMapByType(tp string) map[string]Relation {
	switch tp {
	case RelationTableType:
		return c.tables
	case RelationIndexType:
		return c.indexes
	case RelationSequenceType:
		return c.sequences
	}

	panic(stringutil.Sprintf("unknown catalog object type %q", tp))
}

func (c *catalogCache) Add(tx *Transaction, o Relation) error {
	name := o.Name()

	// if name is provided, ensure it's not duplicated
	if name != "" {
		if c.objectExists(name) {
			return errors.Wrap(errs.AlreadyExistsError{Name: name})
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

func (c *catalogCache) Replace(tx *Transaction, o Relation) error {
	m := c.getMapByType(o.Type())

	old, ok := m[o.Name()]
	if !ok {
		return errors.Wrap(errs.NotFoundError{Name: o.Name()})
	}

	m[o.Name()] = o

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		m[o.Name()] = old
	})

	return nil
}

func (c *catalogCache) Delete(tx *Transaction, tp, name string) (Relation, error) {
	m := c.getMapByType(tp)

	o, ok := m[name]
	if !ok {
		return nil, errors.Wrap(errs.NotFoundError{Name: name})
	}

	delete(m, name)

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		m[name] = o
	})

	return o, nil
}

func (c *catalogCache) Get(tp, name string) (Relation, error) {
	m := c.getMapByType(tp)

	o, ok := m[name]
	if !ok {
		return nil, errors.Wrap(errs.NotFoundError{Name: name})
	}

	return o, nil
}

func (c *catalogCache) ListObjects(tp string) []string {
	m := c.getMapByType(tp)

	list := make([]string, 0, len(m))
	for name := range m {
		list = append(list, name)
	}

	sort.Strings(list)
	return list
}

func (c *catalogCache) GetTableIndexes(tableName string) []*IndexInfo {
	var indexes []*IndexInfo
	for _, o := range c.indexes {
		idx := o.(*IndexInfo)
		if idx.TableName != tableName {
			continue
		}
		indexes = append(indexes, idx)
	}

	return indexes
}

type CatalogStore struct {
	Catalog *Catalog
	info    *TableInfo
	Codec   encoding.Codec
}

func newCatalogStore() *CatalogStore {
	return &CatalogStore{
		info: &TableInfo{
			TableName: TableName,
			StoreName: []byte(TableName),
			FieldConstraints: []*FieldConstraint{
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

func (s *CatalogStore) Init(tx *Transaction, ctg *Catalog) error {
	s.Catalog = ctg
	_, err := tx.Tx.GetStore([]byte(TableName))
	if errors.Is(err, engine.ErrStoreNotFound) {
		err = tx.Tx.CreateStore([]byte(TableName))
	}

	s.Codec = ctg.Codec
	return err
}

func (s *CatalogStore) Info() *TableInfo {
	return s.info
}

func (s *CatalogStore) Table(tx *Transaction) *Table {
	st, err := tx.Tx.GetStore([]byte(TableName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", TableName, err))
	}

	return &Table{
		Tx:      tx,
		Store:   st,
		Info:    s.info,
		Catalog: s.Catalog,
		Codec:   s.Codec,
	}
}

// Insert a catalog object to the table.
func (s *CatalogStore) Insert(tx *Transaction, r Relation) error {
	tb := s.Table(tx)

	_, err := tb.Insert(relationToDocument(r))
	if errors.Is(err, errs.ErrDuplicateDocument) {
		return errors.Wrap(errs.AlreadyExistsError{Name: r.Name()})
	}

	return err
}

// Replace a catalog object with another.
func (s *CatalogStore) Replace(tx *Transaction, name string, r Relation) error {
	tb := s.Table(tx)

	key, err := tb.EncodeValue(types.NewTextValue(name))
	if err != nil {
		return err
	}

	_, err = tb.Replace(key, relationToDocument(r))
	return err
}

func (s *CatalogStore) Delete(tx *Transaction, name string) error {
	tb := s.Table(tx)

	key, err := tb.EncodeValue(types.NewTextValue(name))
	if err != nil {
		return err
	}

	return tb.Delete(key)
}

func relationToDocument(r Relation) types.Document {
	switch t := r.(type) {
	case *TableInfo:
		return tableInfoToDocument(t)
	case *IndexInfo:
		return indexInfoToDocument(t)
	case *Sequence:
		return sequenceInfoToDocument(t.Info)
	}

	panic(stringutil.Sprintf("objectToDocument: unknown type %q", r.Type()))
}

func tableInfoToDocument(ti *TableInfo) types.Document {
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

func indexInfoToDocument(i *IndexInfo) types.Document {
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

func sequenceInfoToDocument(seq *SequenceInfo) types.Document {
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

func ownerToDocument(owner *Owner) types.Document {
	buf := document.NewFieldBuffer().Add("table_name", types.NewTextValue(owner.TableName))
	if owner.Path != nil {
		buf.Add("path", types.NewTextValue(owner.Path.String()))
	}

	return buf
}

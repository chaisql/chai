package database

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/lock"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/lib/atomic"
	"github.com/genjidb/genji/types"
)

// System tables
const (
	CatalogTableName  = InternalPrefix + "catalog"
	SequenceTableName = InternalPrefix + "sequence"
)

// Relation types
const (
	RelationTableType    = "table"
	RelationIndexType    = "index"
	RelationSequenceType = "sequence"
)

// System sequences
const (
	StoreSequence = InternalPrefix + "store_seq"
)

// System namespaces
const (
	CatalogTableNamespace    tree.Namespace = 1
	SequenceTableNamespace   tree.Namespace = 2
	RollbackSegmentNamespace tree.Namespace = 3
	MinTransientNamespace    tree.Namespace = math.MaxInt64 - 1<<24
	MaxTransientNamespace    tree.Namespace = math.MaxInt64
)

// Catalog manages all database objects such as tables, indexes and sequences.
// It stores all these objects in memory for fast access. Any modification
// is persisted into the __genji_catalog table.
type Catalog struct {
	Cache        *catalogCache
	CatalogTable *CatalogStore
	Locks        *lock.LockManager

	TransientNamespaces *atomic.Counter
}

func NewCatalog() *Catalog {
	return &Catalog{
		Cache:               newCatalogCache(),
		CatalogTable:        newCatalogStore(),
		Locks:               lock.NewLockManager(),
		TransientNamespaces: atomic.NewCounter(int64(MinTransientNamespace), int64(MaxTransientNamespace)),
	}
}

func (c *Catalog) Init(tx *Transaction) error {
	// ensure the catalog schema is store in the catalog table
	err := c.ensureTableExists(tx, c.CatalogTable.info)
	if err != nil {
		return err
	}

	// ensure the store sequence exists
	return c.ensureSequenceExists(tx, &SequenceInfo{
		Name:        StoreSequence,
		IncrementBy: 1,
		Start:       10,
		Min:         1, Max: int64(MinTransientNamespace), // last 24 bits are for transient namespaces
		Owner: Owner{
			TableName: CatalogTableName,
		},
	})
}

func (c *Catalog) ensureTableExists(tx *Transaction, info *TableInfo) error {
	err := c.CreateTable(tx, info.TableName, info)
	if err != nil {
		switch {
		case IsConstraintViolationError(err) && err.(*ConstraintViolationError).Constraint == "PRIMARY KEY":
		case errs.IsAlreadyExistsError(err):
		default:
			return err
		}
	}

	return nil
}

func (c *Catalog) ensureSequenceExists(tx *Transaction, seq *SequenceInfo) error {
	err := c.CreateSequence(tx, seq)
	if err != nil {
		switch {
		case IsConstraintViolationError(err) && err.(*ConstraintViolationError).Constraint == "PRIMARY KEY":
		case errs.IsAlreadyExistsError(err):
		default:
			return err
		}
	}

	return nil
}

func (c *Catalog) generateStoreName(tx *Transaction) (tree.Namespace, error) {
	seq, err := c.GetSequence(StoreSequence)
	if err != nil {
		return 0, err
	}
	v, err := seq.Next(tx, c)
	if err != nil {
		return 0, err
	}

	return tree.Namespace(v), nil
}

func (c *Catalog) LockTable(tx *Transaction, tableName string, mode lock.LockMode) error {
	obj := lock.NewTableObject(tableName)
	if c.Locks.HasLock(tx.ID, obj, mode) {
		return nil
	}

	ok, err := c.Locks.Lock(context.Background(), tx.ID, obj, mode)
	if !ok || err != nil {
		return errors.Wrapf(err, "failed to lock table %s", tableName)
	}

	fn := func() {
		c.Locks.Unlock(tx.ID, obj)
	}
	tx.OnRollbackHooks = append(tx.OnRollbackHooks, fn)
	tx.OnCommitHooks = append(tx.OnCommitHooks, fn)
	return nil
}

func (c *Catalog) GetTable(tx *Transaction, tableName string) (*Table, error) {
	err := c.LockTable(tx, tableName, lock.S)
	if err != nil {
		return nil, err
	}

	o, err := c.Cache.Get(RelationTableType, tableName)
	if err != nil {
		return nil, err
	}

	ti := o.(*TableInfoRelation).Info

	return &Table{
		Tx:      tx,
		Tree:    tree.New(tx.Session, ti.StoreNamespace),
		Info:    ti,
		Catalog: c,
	}, nil
}

// GetTableInfo returns the table info for the given table name.
func (c *Catalog) GetTableInfo(tableName string) (*TableInfo, error) {
	r, err := c.Cache.Get(RelationTableType, tableName)
	if err != nil {
		return nil, err
	}

	return r.(*TableInfoRelation).Info, nil
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (c *Catalog) CreateTable(tx *Transaction, tableName string, info *TableInfo) error {
	err := c.LockTable(tx, tableName, lock.X)
	if err != nil {
		return err
	}

	if info == nil {
		info = new(TableInfo)
	}
	info.TableName = tableName

	if info.TableName == "" {
		return errors.New("table name required")
	}

	_, err = c.GetTable(tx, tableName)
	if err != nil && !errs.IsNotFoundError(err) {
		return err
	}
	if err == nil {
		return errors.WithStack(errs.AlreadyExistsError{Name: tableName})
	}

	if info.StoreNamespace == 0 {
		info.StoreNamespace, err = c.generateStoreName(tx)
		if err != nil {
			return err
		}
	}

	if len(info.FieldConstraints.Ordered) != 0 {
		// bind default values with catalog
		for _, fc := range info.FieldConstraints.Ordered {
			if fc.DefaultValue == nil {
				continue
			}

			fc.DefaultValue.Bind(c)
		}
	}

	rel := TableInfoRelation{Info: info}
	err = c.CatalogTable.Insert(tx, &rel)
	if err != nil {
		return err
	}

	return c.Cache.Add(tx, &rel)
}

// DropTable deletes a table from the catalog
func (c *Catalog) DropTable(tx *Transaction, tableName string) error {
	err := c.LockTable(tx, tableName, lock.X)
	if err != nil {
		return err
	}

	ti, err := c.GetTableInfo(tableName)
	if err != nil {
		return err
	}

	if ti.ReadOnly {
		return errors.New("cannot write to read-only table")
	}

	for _, idx := range c.Cache.GetTableIndexes(tableName) {
		_, err = c.Cache.Delete(tx, RelationIndexType, idx.IndexName)
		if err != nil {
			return err
		}

		err = c.dropIndex(tx, idx)
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

	return tree.New(tx.Session, ti.StoreNamespace).Truncate()
}

// CreateIndex creates an index with the given name.
// If it already exists, returns errs.ErrIndexAlreadyExists.
func (c *Catalog) CreateIndex(tx *Transaction, info *IndexInfo) error {
	err := c.LockTable(tx, info.Owner.TableName, lock.X)
	if err != nil {
		return err
	}

	// check if the associated table exists
	ti, err := c.GetTableInfo(info.Owner.TableName)
	if err != nil {
		return err
	}

	// check if the indexed fields exist
	for _, p := range info.Paths {
		fc := ti.GetFieldConstraintForPath(p)
		if fc == nil {
			return errors.Errorf("field %q does not exist for table %q", p, ti.TableName)
		}
	}

	info.StoreNamespace, err = c.generateStoreName(tx)
	if err != nil {
		return err
	}

	rel := IndexInfoRelation{Info: info}
	err = c.Cache.Add(tx, &rel)
	if err != nil {
		return err
	}

	return c.CatalogTable.Insert(tx, &rel)
}

// GetIndex returns an index by name.
func (c *Catalog) GetIndex(tx *Transaction, indexName string) (*Index, error) {
	info, err := c.GetIndexInfo(indexName)
	if err != nil {
		return nil, err
	}

	return NewIndex(tree.New(tx.Session, info.StoreNamespace), *info), nil
}

// GetIndexInfo returns an index info by name.
func (c *Catalog) GetIndexInfo(indexName string) (*IndexInfo, error) {
	r, err := c.Cache.Get(RelationIndexType, indexName)
	if err != nil {
		return nil, err
	}
	return r.(*IndexInfoRelation).Info, nil
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
	info, err := c.GetIndexInfo(name)
	if err != nil {
		return err
	}

	err = c.LockTable(tx, info.Owner.TableName, lock.X)
	if err != nil {
		return err
	}

	// check if the index has been created by a table constraint
	if len(info.Owner.Paths) > 0 {
		return fmt.Errorf("cannot drop index %s because constraint on %s(%s) requires it", info.IndexName, info.Owner.TableName, info.Owner.Paths)
	}

	_, err = c.Cache.Delete(tx, RelationIndexType, name)
	if err != nil {
		return err
	}

	return c.dropIndex(tx, info)
}

func (c *Catalog) dropIndex(tx *Transaction, info *IndexInfo) error {
	err := tree.New(tx.Session, info.StoreNamespace).Truncate()
	if err != nil {
		return err
	}

	return c.CatalogTable.Delete(tx, info.IndexName)
}

// AddFieldConstraint adds a field constraint to a table.
func (c *Catalog) AddFieldConstraint(tx *Transaction, tableName string, fc *FieldConstraint, tcs TableConstraints) error {
	err := c.LockTable(tx, tableName, lock.X)
	if err != nil {
		return err
	}

	r, err := c.Cache.Get(RelationTableType, tableName)
	if err != nil {
		return err
	}
	ti := r.(*TableInfoRelation).Info

	clone := ti.Clone()
	if fc != nil {
		err = clone.AddFieldConstraint(fc)
		if err != nil {
			return err
		}
	}

	for _, tc := range tcs {
		err = clone.AddTableConstraint(tc)
		if err != nil {
			return err
		}
	}

	cloneRel := &TableInfoRelation{Info: clone}
	err = c.Cache.Replace(tx, cloneRel)
	if err != nil {
		return err
	}

	return c.CatalogTable.Replace(tx, tableName, cloneRel)
}

// RenameTable renames a table.
// If it doesn't exist, it returns errs.ErrTableNotFound.
func (c *Catalog) RenameTable(tx *Transaction, oldName, newName string) error {
	err := c.LockTable(tx, oldName, lock.X)
	if err != nil {
		return err
	}

	// Delete the old table info.
	err = c.CatalogTable.Delete(tx, oldName)
	if errs.IsNotFoundError(err) {
		return errors.Wrapf(err, "table %s does not exist", oldName)
	}
	if err != nil {
		return err
	}

	o, err := c.Cache.Delete(tx, RelationTableType, oldName)
	if err != nil {
		return err
	}

	ti := o.(*TableInfoRelation).Info

	clone := ti.Clone()
	clone.TableName = newName

	cloneRel := &TableInfoRelation{
		Info: clone,
	}
	err = c.CatalogTable.Insert(tx, cloneRel)
	if err != nil {
		return err
	}

	err = c.Cache.Add(tx, cloneRel)
	if err != nil {
		return err
	}

	for _, idx := range c.Cache.GetTableIndexes(oldName) {
		r, err := c.Cache.Delete(tx, RelationIndexType, idx.IndexName)
		if err != nil {
			return err
		}
		info := r.(*IndexInfoRelation).Info

		idxClone := info.Clone()
		idxClone.Owner.TableName = clone.TableName

		cloneRel := &IndexInfoRelation{Info: idxClone}
		err = c.Cache.Add(tx, cloneRel)
		if err != nil {
			return err
		}

		err = c.CatalogTable.Replace(tx, idx.IndexName, cloneRel)
		if err != nil {
			return err
		}
	}

	for _, seqName := range c.ListSequences() {
		seq, err := c.GetSequence(seqName)
		if err != nil {
			return err
		}
		if seq.Info.Owner.TableName != oldName {
			continue
		}

		_, err = c.Cache.Delete(tx, RelationSequenceType, seqName)
		if err != nil {
			return err
		}
		clone := seq.Clone()

		clone.Info.Owner.TableName = newName

		err = c.Cache.Add(tx, clone)
		if err != nil {
			return err
		}

		err = c.CatalogTable.Replace(tx, seqName, clone)
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

// GetFreeTransientNamespace returns the next available transient namespace.
// Transient namespaces start from math.MaxInt64 - (2 << 24) to math.MaxInt64 (around 16 M).
// The transient namespaces counter is not persisted and resets when the database is restarted.
// Once the counter reaches its maximum value, it will wrap around to the minimum value.
// Technically, if a transient namespace is still in use by the time the counter wraps around
// its data may be overwritten. However, transient trees are supposed to verify that the
// namespace is not in use before writing to it.
func (c *Catalog) GetFreeTransientNamespace() tree.Namespace {
	return tree.Namespace(c.TransientNamespaces.Incr())
}

type Relation interface {
	Type() string
	Name() string
	SetName(name string)
	GenerateBaseName() string
}

type TableInfoRelation struct {
	Info *TableInfo
}

func (r *TableInfoRelation) Type() string {
	return "table"
}

func (r *TableInfoRelation) Name() string {
	return r.Info.TableName
}

func (r *TableInfoRelation) SetName(name string) {
	r.Info.TableName = name
}

func (r *TableInfoRelation) GenerateBaseName() string {
	return r.Info.TableName
}

type IndexInfoRelation struct {
	Info *IndexInfo
}

func (r *IndexInfoRelation) Type() string {
	return "index"
}

func (r *IndexInfoRelation) Name() string {
	return r.Info.IndexName
}

func (r *IndexInfoRelation) SetName(name string) {
	r.Info.IndexName = name
}

func (r *IndexInfoRelation) GenerateBaseName() string {
	return fmt.Sprintf("%s_%s_idx", r.Info.Owner.TableName, pathsToIndexName(r.Info.Paths))
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
		c.tables[tables[i].TableName] = &TableInfoRelation{Info: &tables[i]}
	}

	for i := range indexes {
		c.indexes[indexes[i].IndexName] = &IndexInfoRelation{Info: &indexes[i]}
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
		name = fmt.Sprintf("%s%d", baseName, i)
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

	panic(fmt.Sprintf("unknown catalog object type %q", tp))
}

func (c *catalogCache) Add(tx *Transaction, o Relation) error {
	name := o.Name()

	// if name is provided, ensure it's not duplicated
	if name != "" {
		if c.objectExists(name) {
			return errors.WithStack(errs.AlreadyExistsError{Name: name})
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
		return errors.WithStack(errs.NotFoundError{Name: o.Name()})
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
		return nil, errors.WithStack(errs.NotFoundError{Name: name})
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
		return nil, errors.WithStack(&errs.NotFoundError{Name: name})
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
		idx := o.(*IndexInfoRelation).Info
		if idx.Owner.TableName != tableName {
			continue
		}
		indexes = append(indexes, idx)
	}

	return indexes
}

type CatalogStore struct {
	Catalog *Catalog
	info    *TableInfo
}

func newCatalogStore() *CatalogStore {
	return &CatalogStore{
		info: &TableInfo{
			TableName:      CatalogTableName,
			StoreNamespace: CatalogTableNamespace,
			TableConstraints: []*TableConstraint{
				{
					Name:       CatalogTableName + "_pk",
					PrimaryKey: true,
					Paths: []document.Path{
						document.NewPath("name"),
					},
				},
			},
			FieldConstraints: MustNewFieldConstraints(
				&FieldConstraint{
					Position:  0,
					Field:     "name",
					Type:      types.TextValue,
					IsNotNull: true,
				},
				&FieldConstraint{
					Position:  1,
					Field:     "type",
					Type:      types.TextValue,
					IsNotNull: true,
				},
				&FieldConstraint{
					Position: 2,
					Field:    "namespace",
					Type:     types.IntegerValue,
				},
				&FieldConstraint{
					Position: 3,
					Field:    "sql",
					Type:     types.TextValue,
				},
				&FieldConstraint{
					Position: 4,
					Field:    "docid_sequence_name",
					Type:     types.TextValue,
				},
				&FieldConstraint{
					Position: 5,
					Field:    "owner",
					Type:     types.DocumentValue,
					AnonymousType: &AnonymousType{
						FieldConstraints: MustNewFieldConstraints(
							&FieldConstraint{
								Position:  0,
								Field:     "table_name",
								Type:      types.TextValue,
								IsNotNull: true,
							},
							&FieldConstraint{
								Position: 1,
								Field:    "paths",
								Type:     types.ArrayValue,
							},
						),
					},
				},
			),
		},
	}
}

func (s *CatalogStore) Info() *TableInfo {
	return s.info
}

func (s *CatalogStore) Table(tx *Transaction) *Table {
	return &Table{
		Tx:      tx,
		Tree:    tree.New(tx.Session, CatalogTableNamespace),
		Info:    s.info,
		Catalog: s.Catalog,
	}
}

// Insert a catalog object to the table.
func (s *CatalogStore) Insert(tx *Transaction, r Relation) error {
	tb := s.Table(tx)

	_, _, err := tb.Insert(relationToDocument(r))
	if cerr, ok := err.(*ConstraintViolationError); ok && cerr.Constraint == "PRIMARY KEY" {
		return errors.WithStack(errs.AlreadyExistsError{Name: r.Name()})
	}

	return err
}

// Replace a catalog object with another.
func (s *CatalogStore) Replace(tx *Transaction, name string, r Relation) error {
	tb := s.Table(tx)

	key := tree.NewKey(types.NewTextValue(name))
	_, err := tb.Replace(key, relationToDocument(r))
	return err
}

func (s *CatalogStore) Delete(tx *Transaction, name string) error {
	tb := s.Table(tx)

	key := tree.NewKey(types.NewTextValue(name))

	return tb.Delete(key)
}

func relationToDocument(r Relation) types.Document {
	switch t := r.(type) {
	case *TableInfoRelation:
		return tableInfoToDocument(t.Info)
	case *IndexInfoRelation:
		return indexInfoToDocument(t.Info)
	case *Sequence:
		return sequenceInfoToDocument(t.Info)
	}

	panic(fmt.Sprintf("objectToDocument: unknown type %q", r.Type()))
}

func tableInfoToDocument(ti *TableInfo) types.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", types.NewTextValue(ti.TableName))
	buf.Add("type", types.NewTextValue(RelationTableType))
	buf.Add("namespace", types.NewIntegerValue(int64(ti.StoreNamespace)))
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
	buf.Add("namespace", types.NewIntegerValue(int64(i.StoreNamespace)))
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
		buf.Add("owner", types.NewDocumentValue(ownerToDocument(&seq.Owner)))
	}

	return buf
}

func ownerToDocument(owner *Owner) types.Document {
	buf := document.NewFieldBuffer().Add("table_name", types.NewTextValue(owner.TableName))
	if owner.Paths != nil {
		vb := document.NewValueBuffer()
		for _, p := range owner.Paths {
			vb.Append(types.NewTextValue(p.String()))
		}
		buf.Add("paths", types.NewArrayValue(vb))
	}

	return buf
}

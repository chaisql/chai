package database

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
)

const storePrefix = 't'

// FieldConstraint describes constraints on a particular field.
type FieldConstraint struct {
	Path         document.Path
	Type         document.ValueType
	IsPrimaryKey bool
	IsNotNull    bool
	DefaultValue document.Value
	IsInferred   bool
	InferredBy   []document.Path
}

// IsEqual compares f with other member by member.
// Inference is not compared.
func (f *FieldConstraint) IsEqual(other *FieldConstraint) (bool, error) {
	if !f.Path.IsEqual(other.Path) {
		return false, nil
	}

	if f.Type != other.Type {
		return false, nil
	}

	if f.IsPrimaryKey != other.IsPrimaryKey {
		return false, nil
	}

	if f.IsNotNull != other.IsNotNull {
		return false, nil
	}

	if f.HasDefaultValue() != other.HasDefaultValue() {
		return false, nil
	}

	if f.HasDefaultValue() {
		if ok, err := f.DefaultValue.IsEqual(other.DefaultValue); !ok || err != nil {
			return ok, err
		}
	}

	return true, nil
}

func (f *FieldConstraint) String() string {
	var s strings.Builder

	s.WriteString(f.Path.String())
	s.WriteString(" ")
	s.WriteString(f.Type.String())
	if f.IsNotNull {
		s.WriteString(" NOT NULL")
	}
	if f.IsPrimaryKey {
		s.WriteString(" PRIMARY KEY")
	}

	if f.HasDefaultValue() {
		s.WriteString(" DEFAULT ")
		s.WriteString(f.DefaultValue.String())
	}

	return s.String()
}

// MergeInferred adds the other.InferredBy to f.InferredBy and ensures there are no duplicates.
func (f *FieldConstraint) MergeInferred(other *FieldConstraint) {
	for _, by := range other.InferredBy {
		duplicate := false
		for _, fby := range f.InferredBy {
			if fby.IsEqual(by) {
				duplicate = true
				break
			}
		}

		if !duplicate {
			f.InferredBy = append(f.InferredBy, by)
		}
	}
}

// HasDefaultValue returns this field contains a default value constraint.
func (f *FieldConstraint) HasDefaultValue() bool {
	return f.DefaultValue.Type != 0
}

// ToDocument returns a document from f.
func (f *FieldConstraint) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("path", document.NewArrayValue(pathToArray(f.Path)))
	buf.Add("type", document.NewIntegerValue(int64(f.Type)))
	buf.Add("is_primary_key", document.NewBoolValue(f.IsPrimaryKey))
	buf.Add("is_not_null", document.NewBoolValue(f.IsNotNull))
	if f.HasDefaultValue() {
		buf.Add("default_value", f.DefaultValue)
	}
	buf.Add("is_inferred", document.NewBoolValue(f.IsInferred))
	if f.IsInferred {
		vb := document.NewValueBuffer()
		for _, by := range f.InferredBy {
			vb = vb.Append(document.NewArrayValue(pathToArray(by)))
		}
		buf.Add("inferred_by", document.NewArrayValue(vb))
	}
	return buf
}

// ScanDocument implements the document.Scanner interface.
func (f *FieldConstraint) ScanDocument(d document.Document) error {
	v, err := d.GetByField("path")
	if err != nil {
		return err
	}
	f.Path, err = arrayToPath(v.V.(document.Array))
	if err != nil {
		return err
	}

	v, err = d.GetByField("type")
	if err != nil {
		return err
	}
	tp := v.V.(int64)
	f.Type = document.ValueType(tp)

	v, err = d.GetByField("is_primary_key")
	if err != nil {
		return err
	}
	f.IsPrimaryKey = v.V.(bool)

	v, err = d.GetByField("is_not_null")
	if err != nil {
		return err
	}
	f.IsNotNull = v.V.(bool)

	v, err = d.GetByField("default_value")
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if err == nil {
		f.DefaultValue = v
	}

	v, err = d.GetByField("is_inferred")
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if err == nil {
		f.IsInferred = v.V.(bool)
	}

	v, err = d.GetByField("inferred_by")
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if err == nil {
		v.V.(document.Array).Iterate(func(i int, value document.Value) error {
			by, err := arrayToPath(value.V.(document.Array))
			if err != nil {
				return err
			}
			f.InferredBy = append(f.InferredBy, by)
			return nil
		})
	}

	return nil
}

// FieldConstraints is a list of field constraints.
type FieldConstraints []*FieldConstraint

// NewFieldConstraints takes user-defined field constraints, validates them, infers additional
// constraints if needed, and returns a valid FieldConstraints type that can be assigned to a table.
func NewFieldConstraints(userConstraints []*FieldConstraint) (FieldConstraints, error) {
	// ensure no duplicate
	return nil, nil
}

// Infer additional constraints based on user defined ones.
// For example, given the following table:
//   CREATE TABLE foo (a.b[0] TEXT)
// this function will return a TableInfo that behaves as if the table
// had been created like this:
//   CREATE TABLE foo(
//      a DOCUMENT
//      a.b ARRAY
//      a.b[0] TEXT
//   )
func (f FieldConstraints) Infer() (FieldConstraints, error) {
	newConstraints := make(FieldConstraints, 0, len(f))

	for _, fc := range f {
		// loop over all the path fragments and
		// create intermediary inferred constraints.
		if len(fc.Path) > 1 {
			for i := range fc.Path {
				// stop before reaching the last fragment
				// which will be added outside of this loop
				if i+1 == len(fc.Path) {
					break
				}

				newFc := FieldConstraint{
					Path:       fc.Path[:i+1],
					IsInferred: true,
					InferredBy: []document.Path{fc.Path},
				}
				if fc.Path[i+1].FieldName != "" {
					newFc.Type = document.DocumentValue
				} else {
					newFc.Type = document.ArrayValue
				}

				err := newConstraints.Add(&newFc)
				if err != nil {
					return nil, err
				}
			}
		}

		// add the non inferred path to the list
		// and ensure there are no conflicts with
		// existing ones.
		err := newConstraints.Add(fc)
		if err != nil {
			return nil, err
		}
	}

	return newConstraints, nil
}

// Add a field constraint to the list. If another constraint exists for the same path
// and they are equal, newFc will be ignored. Otherwise an error will be returned.
// If newFc has been inferred by another constraint and another constraint exists with the same
// path, their InferredBy member will be merged.
func (f *FieldConstraints) Add(newFc *FieldConstraint) error {
	for i, c := range *f {
		if c.Path.IsEqual(newFc.Path) {
			ok, err := c.IsEqual(newFc)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("conflicting constraints: %q and %q", c.String(), newFc.String())
			}

			// if both inferred, merge the InferredBy member
			if newFc.IsInferred && c.IsInferred {
				c.MergeInferred(newFc)
				return nil
			}

			// if existing one is not inferred, ignore newFc
			if newFc.IsInferred && !c.IsInferred {
				return nil
			}

			// if existing one is inferred, and newFc is not,
			// replace it
			if !newFc.IsInferred && c.IsInferred {
				(*f)[i] = newFc
				return nil
			}
		}
	}

	*f = append(*f, newFc)
	return nil
}

// ValidateDocument calls Convert then ensures the document validates against the field constraints.
func (f FieldConstraints) ValidateDocument(d document.Document) (*document.FieldBuffer, error) {
	fb, err := f.ConvertDocument(d)
	if err != nil {
		return nil, err
	}

	// ensure no field is missing
	for _, fc := range f {
		v, err := fc.Path.GetValueFromDocument(fb)
		if err == nil {
			// if field is found, it has already been converted
			// to the right type above.
			// check if it is required but null.
			if v.Type == document.NullValue && fc.IsNotNull {
				return nil, fmt.Errorf("field %q is required and must be not null", fc.Path)
			}
			continue
		}

		if err != document.ErrFieldNotFound {
			return nil, err
		}

		// if field is not found
		// check if there is a default value
		if fc.DefaultValue.Type != 0 {
			err = fb.Set(fc.Path, fc.DefaultValue)
			if err != nil {
				return nil, err
			}
			// if there is no default value
			// check if field is required
		} else if fc.IsNotNull {
			return nil, fmt.Errorf("field %q is required and must be not null", fc.Path)
		}
	}

	return fb, nil
}

// ConvertDocument the document using the field constraints.
// It converts any path that has a field constraint on it into the specified type.
// If there is no constraint on an integer field or value, it converts it into a double.
// Default values on missing fields are not applied.
func (f FieldConstraints) ConvertDocument(d document.Document) (*document.FieldBuffer, error) {
	fb := document.NewFieldBuffer()
	err := fb.Copy(d)
	if err != nil {
		return nil, err
	}

	// convert the document using field constraints type information.
	// if there is a type constraint on a path, apply it.
	// if a value is an integer and has no constraint, convert it to double.
	err = fb.Apply(func(p document.Path, v document.Value) (document.Value, error) {
		for _, fc := range f {
			if !fc.Path.IsEqual(p) {
				continue
			}

			// check if the constraint enforce a particular type
			// and if so convert the value to the new type.
			if fc.Type != 0 {
				return v.CastAs(fc.Type)
			}
			break
		}

		// no constraint have been found for this path.
		// check if this is an integer and convert it to double.
		if v.Type == document.IntegerValue {
			return v.CastAsDouble()
		}

		return v, nil
	})

	return fb, err
}

// TableInfo contains information about a table.
type TableInfo struct {
	// name of the table.
	tableName string
	// name of the store associated with the table.
	storeName []byte
	readOnly  bool

	FieldConstraints FieldConstraints
}

// GetPrimaryKey returns the field constraint of the primary key.
// Returns nil if there is no primary key.
func (ti *TableInfo) GetPrimaryKey() *FieldConstraint {
	for _, f := range ti.FieldConstraints {
		if f.IsPrimaryKey {
			return f
		}
	}

	return nil
}

// ToDocument turns ti into a document.
func (ti *TableInfo) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("table_name", document.NewTextValue(ti.tableName))
	buf.Add("store_name", document.NewBlobValue(ti.storeName))

	vbuf := document.NewValueBuffer()
	for _, fc := range ti.FieldConstraints {
		vbuf = vbuf.Append(document.NewDocumentValue(fc.ToDocument()))
	}

	buf.Add("field_constraints", document.NewArrayValue(vbuf))

	buf.Add("read_only", document.NewBoolValue(ti.readOnly))
	return buf
}

// ScanDocument decodes d into ti.
func (ti *TableInfo) ScanDocument(d document.Document) error {
	v, err := d.GetByField("table_name")
	if err != nil {
		return err
	}
	ti.tableName = v.V.(string)

	v, err = d.GetByField("store_name")
	if err != nil {
		return err
	}
	ti.storeName = make([]byte, len(v.V.([]byte)))
	copy(ti.storeName, v.V.([]byte))

	v, err = d.GetByField("field_constraints")
	if err != nil {
		return err
	}
	ar := v.V.(document.Array)

	l, err := document.ArrayLength(ar)
	if err != nil {
		return err
	}

	ti.FieldConstraints = make([]*FieldConstraint, l)

	err = ar.Iterate(func(i int, value document.Value) error {
		var fc FieldConstraint
		err := fc.ScanDocument(value.V.(document.Document))
		if err != nil {
			return err
		}

		ti.FieldConstraints[i] = &fc
		return nil
	})
	if err != nil {
		return err
	}

	v, err = d.GetByField("read_only")
	if err != nil {
		return err
	}

	ti.readOnly = v.V.(bool)
	return nil
}

// Clone creates another tableInfo with the same values.
func (ti *TableInfo) Clone() *TableInfo {
	cp := *ti
	cp.FieldConstraints = nil
	for _, fc := range ti.FieldConstraints {
		cp.FieldConstraints = append(cp.FieldConstraints, fc)
	}
	return &cp
}

// tableStore manages table information.
// It loads table information during database startup
// and holds it in memory.
type tableStore struct {
	db *Database
	st engine.Store
}

// List all tables.
func (t *tableStore) ListAll() ([]*TableInfo, error) {
	it := t.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var list []*TableInfo
	var buf []byte
	var err error

	for it.Seek(nil); it.Valid(); it.Next() {
		itm := it.Item()
		buf, err = itm.ValueCopy(buf)
		if err != nil {
			return nil, err
		}

		var ti TableInfo
		err = ti.ScanDocument(t.db.Codec.NewDocument(buf))
		if err != nil {
			return nil, err
		}

		list = append(list, &ti)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return list, nil
}

// Insert a new tableInfo for the given table name.
// If info.storeName is nil, it generates one and stores it in info.
func (t *tableStore) Insert(tx *Transaction, tableName string, info *TableInfo) error {
	tblName := []byte(tableName)

	_, err := t.st.Get(tblName)
	if err == nil {
		return ErrTableAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	if info.storeName == nil {
		seq, err := t.st.NextSequence()
		if err != nil {
			return err
		}
		buf := make([]byte, binary.MaxVarintLen64+1)
		buf[0] = storePrefix
		n := binary.PutUvarint(buf[1:], seq)
		info.storeName = buf[:n+1]
	}

	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(info.ToDocument())
	if err != nil {
		return err
	}

	err = t.st.Put([]byte(tableName), buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (t *tableStore) Delete(tx *Transaction, tableName string) error {
	err := t.st.Delete([]byte(tableName))
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
		}

		return err
	}

	return nil
}

// Replace replaces tableName table information with the new info.
func (t *tableStore) Replace(tx *Transaction, tableName string, info *TableInfo) error {
	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err := enc.EncodeDocument(info.ToDocument())
	if err != nil {
		return err
	}

	tbName := []byte(tableName)
	_, err = t.st.Get(tbName)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
		}

		return err
	}

	return t.st.Put(tbName, buf.Bytes())
}

// IndexInfo holds the configuration of an index.
type IndexInfo struct {
	TableName string
	IndexName string
	Path      document.Path

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	// If set, the index is typed and only accepts that type
	Type document.ValueType
}

// ToDocument creates a document from an IndexConfig.
func (i *IndexInfo) ToDocument() document.Document {
	buf := document.NewFieldBuffer()

	buf.Add("unique", document.NewBoolValue(i.Unique))
	buf.Add("index_name", document.NewTextValue(i.IndexName))
	buf.Add("table_name", document.NewTextValue(i.TableName))
	buf.Add("path", document.NewArrayValue(pathToArray(i.Path)))
	if i.Type != 0 {
		buf.Add("type", document.NewIntegerValue(int64(i.Type)))
	}
	return buf
}

// ScanDocument implements the document.Scanner interface.
func (i *IndexInfo) ScanDocument(d document.Document) error {
	v, err := d.GetByField("unique")
	if err != nil {
		return err
	}
	i.Unique = v.V.(bool)

	v, err = d.GetByField("index_name")
	if err != nil {
		return err
	}
	i.IndexName = string(v.V.(string))

	v, err = d.GetByField("table_name")
	if err != nil {
		return err
	}
	i.TableName = string(v.V.(string))

	v, err = d.GetByField("path")
	if err != nil {
		return err
	}
	i.Path, err = arrayToPath(v.V.(document.Array))
	if err != nil {
		return err
	}

	v, err = d.GetByField("type")
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if err == nil {
		i.Type = document.ValueType(v.V.(int64))
	}

	return nil
}

// Clone returns a copy of the index information.
func (i IndexInfo) Clone() *IndexInfo {
	return &i
}

type indexStore struct {
	db *Database
	st engine.Store
}

func (t *indexStore) Insert(cfg IndexInfo) error {
	key := []byte(cfg.IndexName)
	_, err := t.st.Get(key)
	if err == nil {
		return ErrIndexAlreadyExists
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err = enc.EncodeDocument(cfg.ToDocument())
	if err != nil {
		return err
	}

	return t.st.Put(key, buf.Bytes())
}

func (t *indexStore) Get(indexName string) (*IndexInfo, error) {
	key := []byte(indexName)
	v, err := t.st.Get(key)
	if err == engine.ErrKeyNotFound {
		return nil, ErrIndexNotFound
	}
	if err != nil {
		return nil, err
	}

	var idxopts IndexInfo
	err = idxopts.ScanDocument(t.db.Codec.NewDocument(v))
	if err != nil {
		return nil, err
	}

	return &idxopts, nil
}

func (t *indexStore) Replace(indexName string, cfg IndexInfo) error {
	var buf bytes.Buffer
	enc := t.db.Codec.NewEncoder(&buf)
	defer enc.Close()
	err := enc.EncodeDocument(cfg.ToDocument())
	if err != nil {
		return err
	}

	return t.st.Put([]byte(indexName), buf.Bytes())
}

func (t *indexStore) Delete(indexName string) error {
	key := []byte(indexName)
	err := t.st.Delete(key)
	if err == engine.ErrKeyNotFound {
		return ErrIndexNotFound
	}
	return err
}

func (t *indexStore) ListAll() ([]*IndexInfo, error) {
	it := t.st.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var idxList []*IndexInfo
	var buf []byte
	var err error
	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		buf, err = item.ValueCopy(buf)
		if err != nil {
			return nil, err
		}

		var opts IndexInfo
		err = opts.ScanDocument(t.db.Codec.NewDocument(buf))
		if err != nil {
			return nil, err
		}

		idxList = append(idxList, &opts)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return idxList, nil
}

type Indexes []*Index

func (i Indexes) GetIndex(name string) *Index {
	for _, idx := range i {
		if idx.Info.IndexName == name {
			return idx
		}
	}

	return nil
}

func (i Indexes) GetIndexByPath(p document.Path) *Index {
	for _, idx := range i {
		if idx.Info.Path.IsEqual(p) {
			return idx
		}
	}

	return nil
}

func arrayToPath(a document.Array) (document.Path, error) {
	var path document.Path

	err := a.Iterate(func(_ int, value document.Value) error {
		if value.Type == document.TextValue {
			path = append(path, document.PathFragment{FieldName: value.V.(string)})
		} else {
			path = append(path, document.PathFragment{ArrayIndex: int(value.V.(int64))})
		}
		return nil
	})

	return path, err
}

func pathToArray(path document.Path) document.Array {
	abuf := document.NewValueBuffer()
	for _, p := range path {
		if p.FieldName != "" {
			abuf = abuf.Append(document.NewTextValue(p.FieldName))
		} else {
			abuf = abuf.Append(document.NewIntegerValue(int64(p.ArrayIndex)))
		}
	}

	return abuf
}

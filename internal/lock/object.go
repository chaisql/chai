package lock

import "sync"

// An ObjectType defines all objects that can be locked
// by a transaction.
type ObjectType int

const (
	Database ObjectType = iota
	Table
	Document
)

// IsCompatibleWithLock returns true if the lock mode can be held on
// this object type.
func (o ObjectType) IsCompatibleWithLock(l LockMode) bool {
	switch o {
	case Database:
		return l == X || l == S
	case Table:
		return l == X || l == S || l == IX || l == IS || l == SIX
	case Document:
		return l == X || l == S
	default:
		return false
	}
}

// An Object represents a database resource,
// like a table, a document, or the database itself.
// For a database, Key and Table can remain empty.
// For a table, Table refers to the table name.
// For a document, Key refers to the primary key
// and Table to the table name.
type Object struct {
	Key   string
	Table string
	Type  ObjectType
}

func NewDatabaseObject() *Object {
	return &Object{Type: Database}
}

func NewTableObject(name string) *Object {
	obj := cache.get(name)
	if obj != nil {
		return obj
	}

	return cache.newTableObject(name)
}

func NewDocumentObject(tableName string, pk []byte) *Object {
	return &Object{Key: string(pk), Table: tableName, Type: Document}
}

var cache = newObjectCache()

type objectCache struct {
	mu sync.RWMutex

	objects map[string]*Object
}

func newObjectCache() *objectCache {
	return &objectCache{
		objects: make(map[string]*Object),
	}
}

func (oc *objectCache) get(key string) *Object {
	oc.mu.RLock()
	defer oc.mu.RUnlock()
	return oc.objects[key]
}

func (oc *objectCache) newTableObject(name string) *Object {
	oc.mu.Lock()
	obj, ok := oc.objects[name]
	if ok {
		oc.mu.Unlock()
		return obj
	}

	obj = &Object{Table: name, Type: Table}
	oc.objects[name] = obj
	oc.mu.Unlock()
	return obj
}

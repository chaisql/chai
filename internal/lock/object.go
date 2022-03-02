package lock

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
	return &Object{Table: name, Type: Table}
}

func NewDocumentObject(tableName string, pk []byte) *Object {
	return &Object{Key: string(pk), Table: tableName, Type: Document}
}

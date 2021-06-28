package database

type Catalog interface {
	Load(tx *Transaction) error
	GetTable(tx *Transaction, tableName string) (*Table, error)
	CreateTable(tx *Transaction, tableName string, info *TableInfo) error
	DropTable(tx *Transaction, tableName string) error
	RenameTable(tx *Transaction, oldName, newName string) error
	AddFieldConstraint(tx *Transaction, tableName string, fc FieldConstraint) error
	GetIndex(tx *Transaction, indexName string) (*Index, error)
	ListIndexes(tableName string) []string
	CreateIndex(tx *Transaction, info *IndexInfo) error
	DropIndex(tx *Transaction, name string) error
	ReIndex(tx *Transaction, indexName string) error
	ReIndexAll(tx *Transaction) error
	GetSequence(name string) (*Sequence, error)
	CreateSequence(tx *Transaction, info *SequenceInfo) error
	DropSequence(tx *Transaction, name string) error
	ListSequences() []string
}

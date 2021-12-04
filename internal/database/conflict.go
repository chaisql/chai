package database

// OnConflictAction is a function triggered when trying to insert a document that already exists.
// This function is triggered if the key is duplicated or if there is a unique constraint violation on one
// of the fields of the document.
type OnConflictAction int

const (
	// OnConflictDoNothing ignores the duplicate error and returns nothing.
	OnConflictDoNothing = iota + 1

	// OnConflictDoReplace replaces the conflicting document with a new one.
	OnConflictDoReplace
)

func (o OnConflictAction) String() string {
	switch o {
	case OnConflictDoNothing:
		return "DO NOTHING"
	case OnConflictDoReplace:
		return "DO REPLACE"
	}

	return ""
}

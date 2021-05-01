package testutil

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
)

// BlobValue creates a litteral value of type Blob.
func BlobValue(v []byte) expr.LiteralValue {
	return expr.LiteralValue(document.NewBlobValue(v))
}

// TextValue creates a litteral value of type Text.
func TextValue(v string) expr.LiteralValue {
	return expr.LiteralValue(document.NewTextValue(v))
}

// BoolValue creates a litteral value of type Bool.
func BoolValue(v bool) expr.LiteralValue {
	return expr.LiteralValue(document.NewBoolValue(v))
}

// IntegerValue creates a litteral value of type Integer.
func IntegerValue(v int64) expr.LiteralValue {
	return expr.LiteralValue(document.NewIntegerValue(v))
}

// DoubleValue creates a litteral value of type Double.
func DoubleValue(v float64) expr.LiteralValue {
	return expr.LiteralValue(document.NewDoubleValue(v))
}

// NullValue creates a litteral value of type Null.
func NullValue() expr.LiteralValue {
	return expr.LiteralValue(document.NewNullValue())
}

// DocumentValue creates a litteral value of type Document.
func DocumentValue(d document.Document) expr.LiteralValue {
	return expr.LiteralValue(document.NewDocumentValue(d))
}

// ArrayValue creates a litteral value of type Array.
func ArrayValue(a document.Array) expr.LiteralValue {
	return expr.LiteralValue(document.NewArrayValue(a))
}

package stream

import (
	"strconv"
	"strings"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

// TableValidateOperator validates and converts incoming documents against table and field constraints.
type TableValidateOperator struct {
	baseOperator

	tableName string
}

func TableValidate(tableName string) *TableValidateOperator {
	return &TableValidateOperator{
		tableName: tableName,
	}
}

func (op *TableValidateOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	catalog := in.GetCatalog()
	tx := in.GetTx()

	info, err := catalog.GetTableInfo(op.tableName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		newEnv.SetOuter(out)

		doc, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		fb, err := info.ValidateDocument(tx, doc)
		if err != nil {
			return err
		}

		newEnv.SetDocument(fb)

		return fn(&newEnv)
	})
}

func (op *TableValidateOperator) String() string {
	return stringutil.Sprintf("tableValidate(%q)", op.tableName)
}

// A TableInsertOperator inserts incoming documents to the table.
type TableInsertOperator struct {
	baseOperator
	Name string
}

// TableInsert inserts incoming documents to the table.
func TableInsert(tableName string) *TableInsertOperator {
	return &TableInsertOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *TableInsertOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.Set(environment.TableKey, types.NewTextValue(op.Name))

	var table *database.Table
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		newEnv.SetOuter(out)

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		var err error
		if table == nil {
			table, err = out.GetCatalog().GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		key, d, err := table.Insert(d)
		if err != nil {
			return err
		}

		newEnv.Set(environment.DocPKKey, types.NewBlobValue(key))
		newEnv.SetDocument(d)

		return f(&newEnv)
	})
}

func (op *TableInsertOperator) String() string {
	return stringutil.Sprintf("tableInsert(%q)", op.Name)
}

// A TableReplaceOperator replaces documents in the table
type TableReplaceOperator struct {
	baseOperator
	Name string
}

// TableReplace replaces documents in the table. Incoming documents must implement the document.Keyer interface.
func TableReplace(tableName string) *TableReplaceOperator {
	return &TableReplaceOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *TableReplaceOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		if table == nil {
			var err error
			table, err = out.GetCatalog().GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		key, ok := out.Get(environment.DocPKKey)
		if !ok {
			return errors.New("missing key")
		}

		_, err := table.Replace(key.V().([]byte), d)
		if err != nil {
			return err
		}

		return f(out)
	})
}

func (op *TableReplaceOperator) String() string {
	return stringutil.Sprintf("tableReplace(%q)", op.Name)
}

// A TableDeleteOperator replaces documents in the table
type TableDeleteOperator struct {
	baseOperator
	Name string
}

// TableDelete deletes documents from the table. Incoming documents must implement the document.Keyer interface.
func TableDelete(tableName string) *TableDeleteOperator {
	return &TableDeleteOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *TableDeleteOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if table == nil {
			var err error
			table, err = out.GetCatalog().GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		key, ok := out.Get(environment.DocPKKey)
		if !ok {
			return errors.New("missing key")
		}

		err := table.Delete(key.V().([]byte))
		if err != nil {
			return err
		}

		return f(out)
	})
}

func (op *TableDeleteOperator) String() string {
	return stringutil.Sprintf("tableDelete('%s')", op.Name)
}

// A SeqScanOperator iterates over the documents of a table.
type SeqScanOperator struct {
	baseOperator
	TableName string
	Reverse   bool
}

// SeqScan creates an iterator that iterates over each document of the given table.
func SeqScan(tableName string) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName}
}

// SeqScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func SeqScanReverse(tableName string) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName, Reverse: true}
}

func (it *SeqScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	table, err := in.GetCatalog().GetTable(in.GetTx(), it.TableName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)
	newEnv.Set(environment.TableKey, types.NewTextValue(it.TableName))

	return table.IterateOnRange(nil, it.Reverse, func(key tree.Key, d types.Document) error {
		newEnv.Set(environment.DocPKKey, types.NewBlobValue(key))
		newEnv.SetDocument(d)
		return fn(&newEnv)
	})
}

func (it *SeqScanOperator) String() string {
	if !it.Reverse {
		return stringutil.Sprintf("seqScan(%s)", it.TableName)
	}
	return stringutil.Sprintf("seqScanReverse(%s)", it.TableName)
}

// A PkScanOperator iterates over the documents of a table.
type PkScanOperator struct {
	baseOperator
	TableName string
	Ranges    Ranges
	Reverse   bool
}

// PkScan creates an iterator that iterates over each document of the given table.
func PkScan(tableName string, ranges ...Range) *PkScanOperator {
	return &PkScanOperator{TableName: tableName, Ranges: ranges}
}

// PkScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func PkScanReverse(tableName string, ranges ...Range) *PkScanOperator {
	return &PkScanOperator{TableName: tableName, Ranges: ranges, Reverse: true}
}

func (it *PkScanOperator) String() string {
	var s strings.Builder

	s.WriteString("pkScan")
	if it.Reverse {
		s.WriteString("Reverse")
	}

	s.WriteRune('(')

	s.WriteString(strconv.Quote(it.TableName))
	if len(it.Ranges) > 0 {
		s.WriteString(", ")
		for i, r := range it.Ranges {
			s.WriteString(r.String())
			if i+1 < len(it.Ranges) {
				s.WriteString(", ")
			}
		}
	}

	s.WriteString(")")

	return s.String()
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *PkScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)
	newEnv.Set(environment.TableKey, types.NewTextValue(it.TableName))

	table, err := in.GetCatalog().GetTable(in.GetTx(), it.TableName)
	if err != nil {
		return err
	}

	ranges, err := it.Ranges.Eval(in)
	if err != nil {
		return err
	}

	for _, rng := range ranges {
		err = table.IterateOnRange(rng, it.Reverse, func(key tree.Key, d types.Document) error {
			newEnv.Set(environment.DocPKKey, types.NewBlobValue(key))
			newEnv.SetDocument(d)

			return fn(&newEnv)
		})
		if errors.Is(err, ErrStreamClosed) {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	return nil
}

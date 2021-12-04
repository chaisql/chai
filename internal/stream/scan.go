package stream

import (
	"strings"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// DocumentPointer holds a document key and lazily loads the document on demand when the Iterate or GetByField method is called.
// It implements the types.Document and the document.Keyer interfaces.
type DocumentPointer struct {
	key   []byte
	Table *database.Table
	Doc   types.Document
}

func (d *DocumentPointer) Iterate(fn func(field string, value types.Value) error) error {
	var err error
	if d.Doc == nil {
		d.Doc, err = d.Table.GetDocument(d.key)
		if err != nil {
			return err
		}
	}

	return d.Doc.Iterate(fn)
}

func (d *DocumentPointer) GetByField(field string) (types.Value, error) {
	var err error
	if d.Doc == nil {
		d.Doc, err = d.Table.GetDocument(d.key)
		if err != nil {
			return nil, err
		}
	}

	return d.Doc.GetByField(field)
}

func (d *DocumentPointer) MarshalJSON() ([]byte, error) {
	if d.Doc == nil {
		var err error
		d.Doc, err = d.Table.GetDocument(d.key)
		if err != nil {
			return nil, err
		}
	}

	return d.Doc.(interface{ MarshalJSON() ([]byte, error) }).MarshalJSON()
}

type DocumentsOperator struct {
	baseOperator
	Docs []types.Document
}

// Documents creates a DocumentsOperator that iterates over the given values.
func Documents(documents ...types.Document) *DocumentsOperator {
	return &DocumentsOperator{
		Docs: documents,
	}
}

func (op *DocumentsOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, d := range op.Docs {
		newEnv.SetDocument(d)
		err := fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *DocumentsOperator) String() string {
	var sb strings.Builder

	sb.WriteString("docs(")
	for i, d := range op.Docs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(d.(stringutil.Stringer).String())
	}
	sb.WriteString(")")

	return sb.String()
}

type ExprsOperator struct {
	baseOperator
	Exprs []expr.Expr
}

// Expressions creates an operator that iterates over the given expressions.
// Each expression must evaluate to a document.
func Expressions(exprs ...expr.Expr) *ExprsOperator {
	return &ExprsOperator{Exprs: exprs}
}

func (op *ExprsOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, e := range op.Exprs {
		v, err := e.Eval(in)
		if err != nil {
			return err
		}
		if v.Type() != types.DocumentValue {
			return errors.Wrap(ErrInvalidResult)
		}

		newEnv.SetDocument(v.V().(types.Document))
		err = fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *ExprsOperator) String() string {
	var sb strings.Builder

	sb.WriteString("exprs(")
	for i, e := range op.Exprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.(stringutil.Stringer).String())
	}
	sb.WriteByte(')')

	return sb.String()
}

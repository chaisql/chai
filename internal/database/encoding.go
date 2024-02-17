package database

import (
	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// EncodeRow validates a row against all the constraints of the table
// and encodes it.
func (t *TableInfo) EncodeRow(tx *Transaction, dst []byte, r row.Row) ([]byte, error) {
	if ed, ok := RowIsEncoded(r, &t.ColumnConstraints); ok {
		return ed.encoded, nil
	}

	return encodeRow(tx, dst, &t.ColumnConstraints, r)
}

func encodeRow(tx *Transaction, dst []byte, ccs *ColumnConstraints, r row.Row) ([]byte, error) {
	// loop over all the defined column contraints in order.
	for _, cc := range ccs.Ordered {

		// get the column from the row
		v, err := r.Get(cc.Column)
		if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
			return nil, err
		}

		// if the column is not found OR NULL, and the column has a default value, use the default value,
		// otherwise return an error
		if v == nil {
			if cc.DefaultValue != nil {
				v, err = cc.DefaultValue.Eval(tx, r)
				if err != nil {
					return nil, err
				}
			}
		}

		if v == nil {
			v = types.NewNullValue()
		}

		// if the column is not found OR NULL, and the column is required, return an error
		if cc.IsNotNull && v.Type() == types.TypeNull {
			return nil, &ConstraintViolationError{Constraint: "NOT NULL", Columns: []string{cc.Column}}
		}

		// ensure the value is of the correct type
		v, err = v.CastAs(cc.Type)
		if err != nil {
			return nil, err
		}

		dst, err = v.Encode(dst)
		if err != nil {
			return nil, err
		}
	}

	return dst, nil
}

type EncodedRow struct {
	encoded           []byte
	columnConstraints *ColumnConstraints
}

func NewEncodedRow(ccs *ColumnConstraints, data []byte) *EncodedRow {
	e := EncodedRow{
		columnConstraints: ccs,
		encoded:           data,
	}

	return &e
}

func (e *EncodedRow) ResetWith(ccs *ColumnConstraints, data []byte) {
	e.columnConstraints = ccs
	e.encoded = data
}

func (e *EncodedRow) decodeValue(fc *ColumnConstraint, b []byte) (types.Value, int, error) {
	if b[0] == encoding.NullValue {
		return types.NewNullValue(), 1, nil
	}

	v, n := fc.Type.Def().Decode(b)

	return v, n, nil
}

// Get decodes the selected column from the buffer.
func (e *EncodedRow) Get(column string) (v types.Value, err error) {
	b := e.encoded

	// get the column from the list of column constraints
	cc, ok := e.columnConstraints.ByColumn[column]
	if !ok {
		return nil, errors.Wrapf(types.ErrColumnNotFound, "%s not found", column)
	}

	// skip all columns before the selected column
	for i := 0; i < cc.Position; i++ {
		n := encoding.Skip(b)
		b = b[n:]
	}

	v, _, err = e.decodeValue(cc, b)
	return
}

// Iterate decodes each columns one by one and passes them to fn
// until the end of the row or until fn returns an error.
func (e *EncodedRow) Iterate(fn func(column string, value types.Value) error) error {
	b := e.encoded

	for _, fc := range e.columnConstraints.Ordered {
		v, n, err := e.decodeValue(fc, b)
		if err != nil {
			return err
		}

		b = b[n:]

		err = fn(fc.Column, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EncodedRow) MarshalJSON() ([]byte, error) {
	return row.MarshalJSON(e)
}

func RowIsEncoded(r row.Row, ccs *ColumnConstraints) (*EncodedRow, bool) {
	br, ok := r.(*BasicRow)
	if ok {
		r = br.Row
	}
	ed, ok := r.(*EncodedRow)
	if !ok {
		return nil, false
	}

	// if the pointers are the same, the column constraints are the same
	// otherwise it means we created a copy of the constraints and probably
	// altered them (ie. ALTER TABLE)
	if ed.columnConstraints == ccs {
		return ed, true
	}

	return nil, false
}

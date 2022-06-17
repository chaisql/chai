package database

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/types"
)

// EncodeDocument validates a document against all the constraints of the table
// and encodes it.
func (t *TableInfo) EncodeDocument(tx *Transaction, dst []byte, d types.Document) ([]byte, error) {
	if ed, ok := d.(*encoding.EncodedDocument); ok {
		return ed.Encoded, nil
	}

	return encodeDocument(tx, dst, &t.FieldConstraints, d)
}

func encodeDocument(tx *Transaction, dst []byte, fcs *FieldConstraints, d types.Document) ([]byte, error) {
	var err error

	// loop over all the defined field contraints in order.
	for _, fc := range fcs.Ordered {

		// get the field from the document
		v, err := d.GetByField(fc.Field)
		if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
			return nil, err
		}

		// if the field is not found OR NULL, and the field has a default value, use the default value,
		// otherwise return an error
		if v == nil {
			if fc.DefaultValue != nil {
				v, err = fc.DefaultValue.Eval(tx, d)
				if err != nil {
					return nil, err
				}
			}
		}

		// if the field is not found OR NULL, and the field is required, return an error
		if fc.IsNotNull && (v == nil || v.Type() == types.NullValue) {
			return nil, &ConstraintViolationError{Constraint: "NOT NULL", Paths: []document.Path{document.NewPath(fc.Field)}}
		}

		if v == nil {
			v = types.NewNullValue()
		}

		// ensure the value is of the correct type
		if fc.Type != types.AnyValue {
			v, err = document.CastAs(v, fc.Type)
			if err != nil {
				return nil, err
			}
		}

		// Encode the value only.
		if v.Type() == types.DocumentValue {
			// encode map length
			mlen := len(fc.AnonymousType.FieldConstraints.Ordered)
			if fc.AnonymousType.FieldConstraints.AllowExtraFields {
				mlen += 1
			}
			dst = encoding.EncodeArrayLength(dst, mlen)
			dst, err = encodeDocument(tx, dst, &fc.AnonymousType.FieldConstraints, types.As[types.Document](v))
		} else {
			dst, err = encoding.EncodeValue(dst, v)
		}
		if err != nil {
			return nil, err
		}
	}

	// encode the extra fields, if any.
	if fcs.AllowExtraFields {
		dst, err = encodeExtraFields(dst, fcs, d)
		if err != nil {
			return nil, err
		}
	}

	return dst, nil
}

func encodeExtraFields(dst []byte, fcs *FieldConstraints, d types.Document) ([]byte, error) {
	// count the number of extra fields
	extraFields := 0
	err := d.Iterate(func(field string, value types.Value) error {
		_, ok := fcs.ByField[field]
		if ok {
			return nil
		}
		extraFields++
		return nil
	})
	if err != nil {
		return nil, err
	}

	// encode document length
	dst = encoding.EncodeDocumentLength(dst, extraFields)
	if extraFields == 0 {
		return dst, nil
	}

	fields := make(map[string]struct{}, extraFields)

	err = d.Iterate(func(field string, value types.Value) error {
		_, ok := fcs.ByField[field]
		if ok {
			return nil
		}

		// ensure the field is not repeated
		if _, ok := fields[field]; ok {
			return errors.New("duplicate field " + field)
		}
		fields[field] = struct{}{}

		// encode the field name first
		dst = encoding.EncodeText(dst, field)

		// then encode the value
		dst, err = encoding.EncodeValue(dst, value)
		return err
	})
	if err != nil {
		return nil, err
	}

	return dst, nil
}

type EncodedDocument struct {
	encoded          []byte
	fieldConstraints *FieldConstraints
}

func NewEncodedDocument(fcs *FieldConstraints, data []byte) *EncodedDocument {
	e := EncodedDocument{
		fieldConstraints: fcs,
		encoded:          data,
	}

	return &e
}

func (e *EncodedDocument) skipToExtra(b []byte) int {
	l := len(e.fieldConstraints.Ordered)

	var n int
	for i := 0; i < l; i++ {
		nn := encoding.Skip(b[n:])
		n += nn
	}

	return n
}

func (e *EncodedDocument) decodeValue(fc *FieldConstraint, b []byte) (types.Value, int, error) {
	c := b[0]

	if fc.Type == types.DocumentValue && c == encoding.ArrayValue {
		// skip array
		after := encoding.SkipArray(b[1:])

		// skip type
		b = b[1:]

		// skip length
		_, n := binary.Uvarint(b)
		b = b[n:]

		return types.NewDocumentValue(NewEncodedDocument(&fc.AnonymousType.FieldConstraints, b)), after + 1, nil
	}

	v, n := encoding.DecodeValue(b, fc.Type == types.AnyValue || fc.Type == types.ArrayValue /* intAsDouble */)

	// ensure the returned value is of the correct type
	if fc.Type != types.AnyValue {
		var err error
		v, err = document.CastAs(v, fc.Type)
		if err != nil {
			return nil, 0, err
		}
	}

	return v, n, nil
}

// GetByField decodes the selected field from the buffer.
func (e *EncodedDocument) GetByField(field string) (v types.Value, err error) {
	b := e.encoded

	// get the field from the list of field constraints
	fc, ok := e.fieldConstraints.ByField[field]
	if ok {
		// skip all fields before the selected field
		for i := 0; i < fc.Position; i++ {
			n := encoding.Skip(b)
			b = b[n:]
		}

		v, _, err = e.decodeValue(fc, b)
		return
	}

	// if extra fields are not allowed, return an error
	if !e.fieldConstraints.AllowExtraFields {
		return nil, errors.Wrapf(types.ErrFieldNotFound, "field %q not found", field)
	}

	// otherwise, decode the field from the extra fields
	n := e.skipToExtra(b)
	b = b[n:]

	return encoding.DecodeDocument(b, true /* intAsDouble */).GetByField(field)
}

// Iterate decodes each fields one by one and passes them to fn
// until the end of the document or until fn returns an error.
func (e *EncodedDocument) Iterate(fn func(field string, value types.Value) error) error {
	b := e.encoded

	for _, fc := range e.fieldConstraints.Ordered {
		v, n, err := e.decodeValue(fc, b)
		if err != nil {
			return err
		}

		b = b[n:]

		if v.Type() == types.NullValue {
			continue
		}

		err = fn(fc.Field, v)
		if err != nil {
			return err
		}
	}

	if !e.fieldConstraints.AllowExtraFields {
		return nil
	}

	return encoding.DecodeDocument(b, true /* intAsDouble */).Iterate(func(field string, value types.Value) error {
		return fn(field, value)
	})
}

func (e *EncodedDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(e)
}

package query

import (
	"bytes"

	"github.com/asdine/genji/field"
)

type eqOp struct {
	a, b Expr
}

func Eqq(a, b Expr) Expr {
	return eqOp{a, b}
}

func (e eqOp) Eval(ctx EvalContext) (Scalar, error) {
	sa, err := e.a.Eval(ctx)
	if err != nil {
		return falseScalar, err
	}

	sb, err := e.b.Eval(ctx)
	if err != nil {
		return falseScalar, err
	}

	// if same type, no conversion needed
	if sa.Type == sb.Type {
		if bytes.Equal(sa.Data, sb.Data) {
			return trueScalar, nil
		}

		return falseScalar, nil
	}

	if len(sa.Data) > 0 && sa.Value == nil {
		sa.Value, err = field.DecodeInt64(sa.Data)
		// sa.Value, err = field.Decode(field.Field{Type: sa.Type, Data: sa.Data})
		if err != nil {
			return falseScalar, err
		}
	}

	if len(sb.Data) > 0 && sb.Value == nil {
		sb.Value, err = field.DecodeInt64(sb.Data)
		// sb.Value, err = field.Decode(field.Field{Type: sb.Type, Data: sb.Data})
		if err != nil {
			return falseScalar, err
		}
	}

	// string == []byte
	if sa.Type == field.String && sb.Type == field.Bytes {
		if sa.Value.(string) == string(sb.Value.([]byte)) {
			return trueScalar, nil
		}

		return falseScalar, nil
	}

	// []byte == string
	if sb.Type == field.String && sa.Type == field.Bytes {
		if sb.Value.(string) == string(sa.Value.([]byte)) {
			return trueScalar, nil
		}

		return falseScalar, nil
	}

	// int or float == int or float
	if field.IsNumber(sa.Type) && field.IsNumber(sb.Type) {
		af, bf := numberToFloat(sa.Value), numberToFloat(sb.Value)
		if af == bf {
			return trueScalar, nil
		}

		return falseScalar, nil
	}

	return falseScalar, nil
}

func numberToFloat(v interface{}) float64 {
	var f float64

	switch t := v.(type) {
	case uint:
		f = float64(t)
	case uint8:
		f = float64(t)
	case uint16:
		f = float64(t)
	case uint32:
		f = float64(t)
	case uint64:
		f = float64(t)
	case int:
		f = float64(t)
	case int8:
		f = float64(t)
	case int16:
		f = float64(t)
	case int32:
		f = float64(t)
	case int64:
		f = float64(t)
	}

	return f
}

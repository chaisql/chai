package types

import (
	"sort"

	"github.com/cockroachdb/errors"
)

var _ Value = NewObjectValue(nil)

type ObjectValue struct {
	o Object
}

// NewObjectValue returns a SQL INTEGER value.
func NewObjectValue(x Object) *ObjectValue {
	return &ObjectValue{
		o: x,
	}
}

func (o *ObjectValue) V() any {
	return o.o
}

func (o *ObjectValue) Type() ValueType {
	return TypeObject
}

func (v *ObjectValue) IsZero() (bool, error) {
	err := v.o.Iterate(func(_ string, _ Value) error {
		// We return an error in the first iteration to stop it.
		return errors.WithStack(errStop)
	})
	if err == nil {
		// If err is nil, it means that we didn't iterate,
		// thus the object is empty.
		return true, nil
	}
	if errors.Is(err, errStop) {
		// If err is errStop, it means that we iterate
		// at least once, thus the object is not empty.
		return false, nil
	}
	// An unexpecting error occurs, let's return it!
	return false, err
}

func (o *ObjectValue) String() string {
	data, _ := o.MarshalText()
	return string(data)
}

func (o *ObjectValue) MarshalText() ([]byte, error) {
	return MarshalTextIndent(o, "", "")
}

func (o *ObjectValue) MarshalJSON() ([]byte, error) {
	return jsonObject{Object: o.o}.MarshalJSON()
}

func (v *ObjectValue) EQ(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorEq, v.o, AsObject(other))
}

func (v *ObjectValue) GT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorGt, v.o, AsObject(other))
}

func (v *ObjectValue) GTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorGte, v.o, AsObject(other))
}

func (v *ObjectValue) LT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorLt, v.o, AsObject(other))
}

func (v *ObjectValue) LTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorLte, v.o, AsObject(other))
}

func (v *ObjectValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeObject || b.Type() != TypeObject {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

// Fields returns a list of all the fields at the root of the object
// sorted lexicographically.
func Fields(o Object) ([]string, error) {
	var fields []string
	err := o.Iterate(func(f string, _ Value) error {
		fields = append(fields, f)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(fields)
	return fields, nil
}

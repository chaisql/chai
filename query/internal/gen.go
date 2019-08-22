package main

import (
	"bytes"
	"os"
	"strings"
	"text/template"

	"golang.org/x/tools/imports"
)

const tmpl = `package query

import (
	"github.com/asdine/genji/field"
)

{{ range .Types -}}
// {{ .Name }}FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type {{ .Name }}FieldSelector struct {
	FieldSelector
}

// {{ .Name }}Field creates a typed FieldSelector for fields of type {{ .T }}.
func {{ .Name }}Field(name string) {{ .Name }}FieldSelector {
	return {{ .Name }}FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f {{ .Name }}FieldSelector) Eq(x {{ .T }}) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.Encode{{ .Name }}(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f {{ .Name }}FieldSelector) Gt(x {{ .T }}) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.Encode{{ .Name }}(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f {{ .Name }}FieldSelector) Gte(x {{ .T }}) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.Encode{{ .Name }}(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f {{ .Name }}FieldSelector) Lt(x {{ .T }}) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.Encode{{ .Name }}(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f {{ .Name }}FieldSelector) Lte(x {{ .T }}) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.Encode{{ .Name }}(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f {{ .Name }}FieldSelector) Value(x {{ .T }}) *Scalar {
	return &Scalar{
		Type: field.{{ .Name }},
		Data: field.Encode{{ .Name }}(x),
	}
}

// {{ .Name }}Value is an expression that evaluates to itself.
type {{ .Name }}Value {{ .T }}

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v {{ .Name }}Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.{{ .Name }},
		Data: field.Encode{{ .Name }}({{ .T }}(v)),
	}, nil
}
{{end}}
`

type Types struct {
	Types []Type
}

type Type struct {
	Name string
	T    string
}

func (t *Type) NameLower() string {
	return strings.ToLower(t.Name)
}

func main() {
	t := template.Must(template.New("main").Parse(tmpl))

	var buf bytes.Buffer

	err := t.Execute(&buf, &Types{
		Types: []Type{
			{"Bytes", "[]byte"},
			{"String", "string"},
			{"Bool", "bool"},
			{"Uint", "uint"},
			{"Uint8", "uint8"},
			{"Uint16", "uint16"},
			{"Uint32", "uint32"},
			{"Uint64", "uint64"},
			{"Int", "int"},
			{"Int8", "int8"},
			{"Int16", "int16"},
			{"Int32", "int32"},
			{"Int64", "int64"},
			{"Float32", "float32"},
			{"Float64", "float64"},
		},
	})
	if err != nil {
		panic(err)
	}

	// format using goimports
	output, err := imports.Process("", buf.Bytes(), &imports.Options{
		TabWidth:   8,
		TabIndent:  true,
		Comments:   true,
		FormatOnly: true,
	})
	if err != nil {
		panic(err)
	}

	f, err := os.Create("types.gen.go")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	_, err = f.Write(output)
	if err != nil {
		panic(err)
	}
}

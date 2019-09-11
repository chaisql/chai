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
	"github.com/asdine/genji/value"
)

{{ range .Types -}}
// {{ .Name }}FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type {{ .Name }}FieldSelector struct {
	Field
}

// {{ .Name }}Field creates a typed FieldSelector for fields of type {{ .T }}.
func {{ .Name }}Field(name string) {{ .Name }}FieldSelector {
	return {{ .Name }}FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f {{ .Name }}FieldSelector) Eq(x {{ .T }}) Expr {
	return Eq(f.Field, {{ .Name }}Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f {{ .Name }}FieldSelector) Gt(x {{ .T }}) Expr {
	return Gt(f.Field, {{ .Name }}Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f {{ .Name }}FieldSelector) Gte(x {{ .T }}) Expr {
	return Gte(f.Field, {{ .Name }}Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f {{ .Name }}FieldSelector) Lt(x {{ .T }}) Expr {
	return Lt(f.Field, {{ .Name }}Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f {{ .Name }}FieldSelector) Lte(x {{ .T }}) Expr {
	return Lte(f.Field, {{ .Name }}Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f {{ .Name }}FieldSelector) Value(x {{ .T }}) *value.Value {
	return &value.Value{
		Type: value.{{ .Name }},
		Data: value.Encode{{ .Name }}(x),
	}
}

// {{ .Name }}Value creates a litteral value of type {{ .Name }}.
func {{ .Name }}Value(v {{ .T }}) LitteralValue {
	return LitteralValue{value.New{{ .Name }}(v)}
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

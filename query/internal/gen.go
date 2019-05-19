package main

import (
	"os"
	"strings"
	"text/template"
)

const tmpl = `
package query

import (
	"github.com/asdine/genji/field"
)

{{ range .Types -}}
// Eq{{ .Name }} matches if x is equal to the field selected by f.
func Eq{{ .Name }}(f FieldSelector, x {{ .T }}) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.Encode{{ .Name }}(x),
	}
}

// Gt{{ .Name }} matches if x is greater than the field selected by f.
func Gt{{ .Name }}(f FieldSelector, x {{ .T }}) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.Encode{{ .Name }}(x),
	}
}

// Gte{{ .Name }} matches if x is greater than or equal to the field selected by f.
func Gte{{ .Name }}(f FieldSelector, x {{ .T }}) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.Encode{{ .Name }}(x),
	}
}

// Lt{{ .Name }} matches if x is less than the field selected by f.
func Lt{{ .Name }}(f FieldSelector, x {{ .T }}) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.Encode{{ .Name }}(x),
	}
}

// Lte{{ .Name }} matches if x is less than or equal to the field selected by f.
func Lte{{ .Name }}(f FieldSelector, x {{ .T }}) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.Encode{{ .Name }}(x),
	}
}

// {{ .Name }}Field is a type safe selector that allows to compare values with fields
// based on their types.
type {{ .Name }}Field struct {
	FieldSelector
}

// New{{ .Name }}Field creates a typed FieldSelector for fields of type {{ .T }}.
func New{{ .Name }}Field(name string) {{ .Name }}Field {
	return {{ .Name }}Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f {{ .Name }}Field) Eq(x {{ .T }}) Matcher {
	return Eq{{ .Name }}(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f {{ .Name }}Field) Gt(x {{ .T }}) Matcher {
	return Gt{{ .Name }}(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f {{ .Name }}Field) Gte(x {{ .T }}) Matcher {
	return Gte{{ .Name }}(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f {{ .Name }}Field) Lt(x {{ .T }}) Matcher {
	return Lt{{ .Name }}(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f {{ .Name }}Field) Lte(x {{ .T }}) Matcher {
	return Lte{{ .Name }}(f.FieldSelector, x)
}

{{ end}}
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
	f, err := os.Create("types.gen.go")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = t.Execute(f, &Types{
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
}

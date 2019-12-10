package generator

import (
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/asdine/genji/document"
)

const documentsTmpl = `
{{- define "documents" }}
  {{- range .Documents }}
    {{- template "document" . }}
  {{- end }}
{{- end }}
`

const documentTmpl = `
{{- define "document" }}
{{- template "document-GetByField" . }}
{{- template "document-Iterate" . }}
{{- template "document-ScanDocument" . }}
{{- template "document-Scan" . }}
{{- end }}
`

const documentGetByFieldTmpl = `
{{ define "document-GetByField" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// GetByField implements the field method of the document.Document interface.
func ({{$fl}} *{{$structName}}) GetByField(field string) (document.Value, error) {
	switch field {
	{{- range .Fields }}
	case "{{.FieldName}}":
		return document.New{{.Type}}Value({{$fl}}.{{.Name}}), nil
	{{- end}}
	}

	return document.Value{}, errors.New("unknown field")
}
{{ end }}
`

const documentIterateTmpl = `
{{ define "document-Iterate" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func ({{$fl}} *{{$structName}}) Iterate(fn func(string, document.Value) error) error {
	var err error

	{{range .Fields}}
	err = fn("{{.FieldName}}", document.New{{.Type}}Value({{$fl}}.{{.Name}}))
	if err != nil {
		return err
	}
	{{end}}

	return nil
}
{{ end }}
`

const documentScanDocumentTmpl = `
{{ define "document-ScanDocument" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// ScanDocument extracts fields from document and assigns them to the struct fields.
// It implements the document.Scanner interface.
func ({{$fl}} *{{$structName}}) ScanDocument(doc document.Document) error {
	return doc.Iterate(func(f string, v document.Value) error {
		var err error

		switch f {
		{{- range .Fields}}
		case "{{.FieldName}}":
		{{$fl}}.{{.Name}}, err = v.ConvertTo{{.Type}}()
		{{- end}}
		}
		return err
	})
}
{{ end }}
`

const documentScanTmpl = `
{{ define "document-Scan" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// Scan extracts fields from src and assigns them to the struct fields.
// It implements the driver.Scanner interface.
func ({{$fl}} *{{$structName}}) Scan(src interface{}) error {
	doc, ok := src.(document.Document)
	if !ok {
		return errors.New("unable to scan document from src")
	}

	return {{$fl}}.ScanDocument(doc)
}
{{ end }}
`

type documentContext struct {
	Name   string
	Fields []documentField
}

type documentField struct {
	// Name of the struct field, as found in the structure
	Name string
	// Genji type
	Type string
	// Go type
	GoType string
	// Name of the field in the encoded document
	FieldName string
}

func (rctx *documentContext) lookupDocument(f *ast.File, target string) (bool, error) {
	for _, n := range f.Decls {
		gn, ok := ast.Node(n).(*ast.GenDecl)
		if !ok || gn.Tok != token.TYPE || len(gn.Specs) == 0 {
			continue
		}

		ts, ok := gn.Specs[0].(*ast.TypeSpec)
		if !ok {
			continue
		}

		if ts.Name.Name != target {
			continue
		}

		s, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false, errors.New("invalid object")
		}

		rctx.Name = target

		for _, fd := range s.Fields.List {
			var typeName string

			typ, ok := fd.Type.(*ast.Ident)
			if !ok {
				atyp, ok := fd.Type.(*ast.ArrayType)
				if !ok {
					return false, errors.New("struct must only contain supported fields")
				}

				typ, ok = atyp.Elt.(*ast.Ident)
				if !ok || typ.Name != "byte" {
					return false, errors.New("struct must only contain supported fields")
				}

				typeName = "[]byte"
			} else {
				typeName = typ.Name
			}

			if len(fd.Names) == 0 {
				return false, errors.New("embedded fields are not supported")
			}

			if document.NewValueTypeFromGoType(typeName) == 0 {
				return false, fmt.Errorf("unsupported type %s", typeName)
			}

			for _, name := range fd.Names {
				rctx.Fields = append(rctx.Fields, documentField{
					Name:      name.String(),
					Type:      document.NewValueTypeFromGoType(typeName).String(),
					GoType:    typeName,
					FieldName: strings.ToLower(name.String()),
				})
			}

			if fd.Tag != nil {
				err := handleGenjiTag(rctx, fd)
				if err != nil {
					return false, err
				}
			}
		}

		return true, nil
	}

	return false, nil
}

func (rctx *documentContext) IsExported() bool {
	return unicode.IsUpper(rune(rctx.Name[0]))
}

func (rctx *documentContext) FirstLetter() string {
	return strings.ToLower(rctx.Name[0:1])
}

func (rctx *documentContext) UnexportedName() string {
	if !rctx.IsExported() {
		return rctx.Name
	}

	return rctx.Unexport(rctx.Name)
}

func (rctx *documentContext) ExportedName() string {
	if rctx.IsExported() {
		return rctx.Name
	}

	return rctx.Export(rctx.Name)
}

func (rctx *documentContext) NameWithPrefix(prefix string) string {
	n := prefix + rctx.ExportedName()
	if rctx.IsExported() {
		return rctx.Export(n)
	}

	return rctx.Unexport(n)
}

func (rctx *documentContext) Export(n string) string {
	name := []byte(n)
	name[0] = byte(unicode.ToUpper(rune(n[0])))
	return string(name)
}

func (rctx *documentContext) Unexport(n string) string {
	name := []byte(n)
	name[0] = byte(unicode.ToLower(rune(n[0])))
	return string(name)
}

func handleGenjiTag(ctx *documentContext, fd *ast.Field) error {
	if len(fd.Names) > 1 {
		return errors.New("single genji tag for multiple fields not supported")
	}

	unquoted, err := strconv.Unquote(fd.Tag.Value)
	if err != nil {
		return err
	}

	v, ok := reflect.StructTag(unquoted).Lookup("genji")
	if !ok {
		return nil
	}

	if v != "" {
		ctx.Fields[len(ctx.Fields)-1].FieldName = v
	}

	return nil
}

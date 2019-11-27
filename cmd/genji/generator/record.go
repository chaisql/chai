package generator

import (
	"fmt"
	"go/ast"
	"go/types"
	"reflect"
	"strings"
	"unicode"

	"github.com/asdine/genji/value"
)

const recordsTmpl = `
{{- define "records" }}
  {{- range .Records }}
    {{- template "record" . }}
  {{- end }}
{{- end }}
`

const recordTmpl = `
{{- define "record" }}
{{- template "record-GetField" . }}
{{- template "record-Iterate" . }}
{{- template "record-ScanRecord" . }}
{{- template "record-Scan" . }}
{{- end }}
`

const recordGetFieldTmpl = `
{{ define "record-GetField" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// GetField implements the field method of the record.Record interface.
func ({{$fl}} *{{$structName}}) GetField(name string) (record.Field, error) {
	switch name {
	{{- range .Fields }}
	case "{{.FieldName}}":
		{{ if eq .GoType .GoNamedType -}}
			return record.New{{.Type}}Field("{{.FieldName}}", {{$fl}}.{{.Name}}), nil
		{{- else -}}
			return record.New{{.Type}}Field("{{.FieldName}}", {{.GoType}}({{$fl}}.{{.Name}})), nil
		{{- end -}}
	{{- end}}
	}

	return record.Field{}, errors.New("unknown field")
}
{{ end }}
`

const recordIterateTmpl = `
{{ define "record-Iterate" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func ({{$fl}} *{{$structName}}) Iterate(fn func(record.Field) error) error {
	var err error

	{{range .Fields}}
	{{ if eq .GoType .GoNamedType -}}
		err = fn(record.New{{.Type}}Field("{{.FieldName}}", {{$fl}}.{{.Name}}))
	{{- else -}}
		err = fn(record.New{{.Type}}Field("{{.FieldName}}", {{.GoType}}({{$fl}}.{{.Name}})))
	{{- end }}
	if err != nil {
		return err
	}
	{{end}}

	return nil
}
{{ end }}
`

const recordScanRecordTmpl = `
{{ define "record-ScanRecord" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func ({{$fl}} *{{$structName}}) ScanRecord(rec record.Record) error {
	return rec.Iterate(func(f record.Field) error {
		var err error

		switch f.Name {
		{{- range .Fields}}
		case "{{.FieldName}}":
		{{ if eq .GoType .GoNamedType -}}
			{{$fl}}.{{.Name}}, err = f.DecodeTo{{.Type}}() 
		{{- else -}}
			var tmp {{.GoType}}
			tmp, err = f.DecodeTo{{.Type}}() 
			{{$fl}}.{{.Name}} = {{.GoNamedType}}(tmp)
		{{- end }}
		{{- end}}
		}
		return err
	})
}
{{ end }}
`

const recordScanTmpl = `
{{ define "record-Scan" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// Scan extracts fields from src and assigns them to the struct fields.
// It implements the driver.Scanner interface.
func ({{$fl}} *{{$structName}}) Scan(src interface{}) error {
	rr, ok := src.(record.Record)
	if !ok {
		return errors.New("unable to scan record from src")
	}

	return {{$fl}}.ScanRecord(rr)
}
{{ end }}
`

type recordContext struct {
	Name   string
	Fields []recordField
}

type recordField struct {
	// Name of the struct field, as found in the structure
	Name string
	// Genji type
	Type string
	// Go type
	GoType string
	// Name of the field in the encoded record
	FieldName string
	// The type
	GoNamedType string
}

type recordTags []string

func (r recordTags) FieldName() string {
	switch {
	case len(r) == 0:
		return ""
	case len(r) == 1 && r[0] == "-":
		return ""
	default:
		return r[0]
	}
}

func (r recordTags) Ignore() bool {
	return len(r) == 1 && r[0] == "-"
}

func (r recordTags) Contains(option string) bool {
	for i, opt := range r {
		if i == 0 { // First tag is the name, not an option
			continue
		}
		if opt == option {
			return true
		}
	}
	return false
}

func (rctx *recordContext) lookupRecord(f *ast.File, info *types.Info, target string) (bool, error) {

	for _, def := range info.Defs {
		if def == nil {
			continue
		}
		if def.Name() != target {
			continue
		}
		tn, ok := def.(*types.TypeName)
		if !ok {
			return false, nil
		}
		str, ok := tn.Type().Underlying().(*types.Struct)
		if !ok {
			return false, nil
		}
		rctx.Name = target
		for i := 0; i < str.NumFields(); i++ {
			fld := str.Field(i)
			tag := str.Tag(i)
			tags := extractGenjiTag(tag)
			if tags.Ignore() {
				continue
			}
			typ := fld.Type()

			_, ok = typ.(*types.Basic)
			if !ok {
				_, ok := typ.Underlying().(*types.Basic)
				if ok {
					typ = typ.Underlying()
				} else {
					sl, ok := fld.Type().Underlying().(*types.Slice)
					if ok {
						slType, ok := sl.Elem().Underlying().(*types.Basic)
						if !ok || slType.Kind() != types.Byte {
							return false, fmt.Errorf("struct must only contain supported fields: (%s %s) is not supported", fld.Name(), typ.String())
						}
					} else {
						return false, fmt.Errorf("unsupported type %s", fld.Name())
					}
				}
			}

			if value.TypeFromGoType(typ.String()) == 0 {
				return false, fmt.Errorf("unsupported type %s", typ.String())
			}
			namedType := fld.Type().String()
			namedParts := strings.Split(namedType, ".")
			namedType = namedParts[len(namedParts)-1]

			fd := recordField{
				fld.Name(), value.TypeFromGoType(typ.String()).String(), typ.String(), strings.ToLower(fld.Name()), namedType,
			}

			rctx.Fields = append(rctx.Fields, fd)
			err := handleGenjiTag(rctx, fd, tags)
			if err != nil {
				return false, err
			}

		}

		return true, nil
	}

	return false, nil
}

func (rctx *recordContext) IsExported() bool {
	return unicode.IsUpper(rune(rctx.Name[0]))
}

func (rctx *recordContext) FirstLetter() string {
	return strings.ToLower(rctx.Name[0:1])
}

func (rctx *recordContext) UnexportedName() string {
	if !rctx.IsExported() {
		return rctx.Name
	}

	return rctx.Unexport(rctx.Name)
}

func (rctx *recordContext) ExportedName() string {
	if rctx.IsExported() {
		return rctx.Name
	}

	return rctx.Export(rctx.Name)
}

func (rctx *recordContext) NameWithPrefix(prefix string) string {
	n := prefix + rctx.ExportedName()
	if rctx.IsExported() {
		return rctx.Export(n)
	}

	return rctx.Unexport(n)
}

func (rctx *recordContext) Export(n string) string {
	name := []byte(n)
	name[0] = byte(unicode.ToUpper(rune(n[0])))
	return string(name)
}

func (rctx *recordContext) Unexport(n string) string {
	name := []byte(n)
	name[0] = byte(unicode.ToLower(rune(n[0])))
	return string(name)
}

func extractGenjiTag(tag string) recordTags {
	tags := recordTags{}
	if len(tag) == 0 {
		return nil
	}
	v, ok := reflect.StructTag(tag).Lookup("genji")
	if !ok {
		return nil
	}
	tags = strings.Split(v, ",")

	return tags

}

func handleGenjiTag(ctx *recordContext, fd recordField, tags recordTags) error {

	if tags.FieldName() != "" {
		ctx.Fields[len(ctx.Fields)-1].FieldName = tags.FieldName()
	}

	return nil
}

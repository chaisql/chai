package generator

import (
	"errors"
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
{{- template "record-Pk" . }}
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
	case "{{.Name}}":
		{{ if eq .GoType .GoNamedType -}}
			return record.New{{.Type}}Field("{{.Name}}", {{$fl}}.{{.Name}}), nil
		{{- else -}}
			return record.New{{.Type}}Field("{{.Name}}", {{.GoType}}({{$fl}}.{{.Name}})), nil
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
		err = fn(record.New{{.Type}}Field("{{.Name}}", {{$fl}}.{{.Name}}))
	{{- else -}}
		err = fn(record.New{{.Type}}Field("{{.Name}}", {{.GoType}}({{$fl}}.{{.Name}})))
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
		case "{{.Name}}":
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
	r, ok := src.(record.Record)
	if !ok {
		return errors.New("unable to scan record from src")
	}

	return {{$fl}}.ScanRecord(r)
}
{{ end }}
`

const recordPkTmpl = `
{{ define "record-Pk" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

{{- if ne .Pk.Name ""}}
// PrimaryKey returns the primary key. It implements the table.PrimaryKeyer interface.
func ({{$fl}} *{{$structName}}) PrimaryKey() ([]byte, error) {
	{{ if eq .Pk.GoType .Pk.GoNamedType -}}
		return value.Encode{{.Pk.Type}}({{$fl}}.{{.Pk.Name}}), nil
	{{- else -}}
		return value.Encode{{.Pk.Type}}({{.Pk.GoType}}({{$fl}}.{{.Pk.Name}})), nil
	{{- end }}	
}
{{- end}}
{{ end }}
`

type field struct {
	Name, Type, GoType, GoNamedType string
}

type recordContext struct {
	Name   string
	Fields []field
	Pk     field
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
			tags := extractGenjiTags(tag)
			_, ok := tags["ignore"] // This field has been tagged to be ignored or skipped
			if ok {
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

			fd := field{
				fld.Name(), value.TypeFromGoType(typ.String()).String(), typ.String(), namedType,
			}
			err := handleGenjiTag(rctx, fd, tags)
			if err != nil {
				return false, err
			}
			rctx.Fields = append(rctx.Fields, fd)

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

func extractGenjiTags(tag string) map[string]interface{} {
	tags := map[string]interface{}{}
	if len(tag) == 0 {
		return nil
	}
	v, ok := reflect.StructTag(tag).Lookup("genji")
	if !ok {
		return nil
	}
	gtags := strings.Split(v, ",")
	for _, gtag := range gtags {
		tags[gtag] = nil
	}
	return tags

}

func handleGenjiTag(ctx *recordContext, fd field, tags map[string]interface{}) error {

	for tag := range tags {
		switch tag {
		case "pk":
			if ctx.Pk.Name != "" {
				return errors.New("only one pk field is allowed")
			}

			ctx.Pk = fd
		default:
			return fmt.Errorf("unsupported genji tag '%s'", tag)
		}
	}

	return nil
}

package generator

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"io"
	"strconv"
	"strings"
	"text/template"
	"unicode"

	"golang.org/x/tools/imports"
)

var t *template.Template

func init() {
	t = template.Must(template.New("").Parse(recordTmpl))
}

const recordTmpl = `
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// Field implements the field method of the record.Record interface.
func ({{$fl}} *{{$structName}}) Field(name string) (field.Field, error) {
	switch name {
	{{- range .Fields }}
	case "{{.Name}}":
		{{- if eq .Type "string"}}
		return field.Field{
			Name: "{{.Name}}",
			Type: field.String,
			Data: []byte({{$fl}}.{{.Name}}),
		}, nil
		{{- else if eq .Type "int64"}}
		return field.Field{
			Name: "{{.Name}}",
			Type: field.Int64,
			Data: field.EncodeInt64({{$fl}}.{{.Name}}),
		}, nil
		{{- end}}
	{{- end}}
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func ({{$fl}} *{{$structName}}) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	{{range .Fields }}
	f, _ = {{$fl}}.Field("{{.Name}}")
	err = fn(f)
	if err != nil {
		return err
	}
	{{end}}

	return nil
}

{{- if ne .Pk.Name ""}}
// Pk returns the primary key. It implements the table.Pker interface.
func ({{$fl}} *{{$structName}}) Pk() ([]byte, error) {
	{{- if eq .Pk.Type "string"}}
		return []byte({{$fl}}.{{.Pk.Name}}), nil
	{{- else if eq .Pk.Type "int64"}}
		return field.EncodeInt64({{$fl}}.{{.Pk.Name}}), nil
	{{- end}}
}
{{- end}}

// {{$structName}}Selector provides helpers for selecting fields from the {{$structName}} structure.
type {{$structName}}Selector struct{}

{{- if .IsExported }}
// New{{$structName}}Selector creates a {{$structName}}Selector.
func New{{$structName}}Selector() {{$structName}}Selector {
{{- else}}
// new{{$structName}}Selector creates a {{$structName}}Selector.
func new{{.ExportedName}}Selector() {{$structName}}Selector {
{{- end}}
	return {{$structName}}Selector{}
}

{{- range $i, $a := .Fields }}
	{{- if eq .Type "string"}}
		// {{$a.Name}} returns a string selector.
		func ({{$structName}}Selector) {{$a.Name}}() query.StrField {
			return query.NewStrField("{{$a.Name}}")
		}
	{{- else if eq .Type "int64"}}
		// {{$a.Name}} returns an int64 selector.
		func ({{$structName}}Selector) {{$a.Name}}() query.Int64Field {
			return query.NewInt64Field("{{$a.Name}}")
		}
	{{- end}}
{{- end}}
`

type fileContext struct {
	Package string
	Records []recordContext
}

type recordContext struct {
	Name   string
	Fields []struct {
		Name, Type string
	}
	Pk struct {
		Name, Type string
	}
}

func (s *recordContext) IsExported() bool {
	return unicode.IsUpper(rune(s.Name[0]))
}

func (s *recordContext) FirstLetter() string {
	return strings.ToLower(s.Name[0:1])
}

func (s *recordContext) UnexportedName() string {
	name := []byte(s.Name)
	name[0] = byte(unicode.ToLower(rune(s.Name[0])))
	return string(name)
}

func (s *recordContext) ExportedName() string {
	name := []byte(s.Name)
	name[0] = byte(unicode.ToUpper(rune(s.Name[0])))
	return string(name)
}

// GenerateRecords parses the given ast, looks for the targets structs
// and generates complementary code to the given writer.
func GenerateRecords(w io.Writer, f *ast.File, targets ...string) error {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "package %s\n", f.Name.Name)

	for _, target := range targets {
		ctx, err := lookupTarget(f, target)
		if err != nil {
			return err
		}

		err = t.Execute(&buf, &ctx)
		if err != nil {
			return err
		}
	}

	// format using goimports
	output, err := imports.Process("", buf.Bytes(), nil)
	if err != nil {
		return err
	}

	_, err = w.Write(output)
	return err
}

func lookupTarget(f *ast.File, target string) (*recordContext, error) {
	var ctx recordContext

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
			return nil, errors.New("invalid object")
		}

		ctx.Name = target

		for _, fd := range s.Fields.List {
			typ, ok := fd.Type.(*ast.Ident)
			if !ok {
				return nil, errors.New("struct must only contain supported fields")
			}

			if len(fd.Names) == 0 {
				return nil, errors.New("embedded fields are not supported")
			}

			if typ.Name != "int64" && typ.Name != "string" {
				return nil, fmt.Errorf("unsupported type %s", typ.Name)
			}

			for _, name := range fd.Names {
				ctx.Fields = append(ctx.Fields, struct {
					Name, Type string
				}{
					name.String(), string(typ.Name),
				})
			}

			if fd.Tag != nil {
				err := handleGenjiTag(&ctx, fd)
				if err != nil {
					return nil, err
				}
			}
		}

		return &ctx, nil
	}

	return nil, fmt.Errorf("struct %s not found", target)
}

func handleGenjiTag(ctx *recordContext, fd *ast.Field) error {
	unquoted, err := strconv.Unquote(fd.Tag.Value)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(unquoted, "genji:") {
		return nil
	}

	rawOpts, err := strconv.Unquote(strings.TrimPrefix(unquoted, "genji:"))
	if err != nil {
		return err
	}

	gtags := strings.Split(rawOpts, ",")

	for _, gtag := range gtags {
		switch gtag {
		case "pk":
			if ctx.Pk.Name != "" {
				return errors.New("only one pk field is allowed")
			}

			ctx.Pk.Name = fd.Names[0].Name
			ctx.Pk.Type = fd.Type.(*ast.Ident).Name
		default:
			return fmt.Errorf("unsupported genji tag '%s'", gtag)
		}
	}

	return nil
}

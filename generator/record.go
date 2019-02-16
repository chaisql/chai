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

	"golang.org/x/tools/imports"
)

var t *template.Template

func init() {
	t = template.Must(template.New("").Parse(recordTmpl))
}

const recordTmpl = `
{{- $fl := .Struct.FirstLetter -}}
{{- $structName := .Struct.Name -}}
package {{.Package}}

// Field implements the field method of the record.Record interface.
func ({{$fl}} *{{$structName}}) Field(name string) (field.Field, error) {
	switch name {
	{{- range .Struct.Fields }}
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

{{- if ne .Struct.Pk.Name ""}}
// Pk returns the primary key. It implements the table.Pker interface.
func ({{$fl}} *{{$structName}}) Pk() ([]byte, error) {
	{{- if eq .Struct.Pk.Type "string"}}
		return []byte({{$fl}}.{{.Struct.Pk.Name}}), nil
	{{- else if eq .Struct.Pk.Type "int64"}}
		return field.EncodeInt64({{$fl}}.{{.Struct.Pk.Name}}), nil
	{{- end}}
}
{{- end}}

{{- $cursor := printf "%sCursor" .Struct.Unexported }}
// Cursor creates a cursor for scanning records.
func ({{$fl}} *{{$structName}}) Cursor() record.Cursor {
	return &{{$cursor}}{
		{{$structName}}: {{$fl}},
		i: -1,
	}
}

type {{$cursor}} struct {
	{{$structName}} *{{$structName}}
	i int
	err error
}

func (c *{{$cursor}}) Next() bool {
	if c.i+2 > {{len .Struct.Fields}} {
		return false
	}

	c.i++
	return true
}

func (c *{{$cursor}}) Field() field.Field {
	switch c.i {
	{{- range $i, $a := .Struct.Fields }}
	case {{$i}}:
		f, _ := c.{{$structName}}.Field("{{$a.Name}}")
		return f
	{{- end}}
	}

	c.err = errors.New("no more fields")
	return field.Field{}
}

func (c *{{$cursor}}) Err() error {
	return c.err
}

// {{$structName}}Selector provides helpers for selecting fields from the {{$structName}} structure.
type {{$structName}}Selector struct{}

// New{{$structName}}Selector creates a {{$structName}}Selector.
func New{{$structName}}Selector() {{$structName}}Selector {
	return {{$structName}}Selector{}
}

{{- range $i, $a := .Struct.Fields }}
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

type recordContext struct {
	Package string
	Struct  struct {
		Name        string
		Unexported  string
		FirstLetter string
		Fields      []struct {
			Name, Type string
		}
		Pk struct {
			Name, Type string
		}
	}
}

// GenerateRecord parses the given ast, looks for the target struct
// and generates complementary code to the given writer.
func GenerateRecord(f *ast.File, target string, w io.Writer) error {
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
			return errors.New("invalid object")
		}

		var ctx recordContext
		ctx.Package = f.Name.Name
		ctx.Struct.Name = target
		ctx.Struct.FirstLetter = strings.ToLower(target[0:1])
		ctx.Struct.Unexported = ctx.Struct.FirstLetter + target[1:]

		for _, fd := range s.Fields.List {
			typ, ok := fd.Type.(*ast.Ident)
			if !ok {
				return errors.New("struct must only contain supported fields")
			}

			if len(fd.Names) == 0 {
				return errors.New("embedded fields are not supported")
			}

			if typ.Name != "int64" && typ.Name != "string" {
				return fmt.Errorf("unsupported type %s", typ.Name)
			}

			for _, name := range fd.Names {
				ctx.Struct.Fields = append(ctx.Struct.Fields, struct {
					Name, Type string
				}{
					name.String(), string(typ.Name),
				})
			}

			if fd.Tag != nil {
				err := handleGenjiTag(&ctx, fd)
				if err != nil {
					return err
				}
			}
		}

		var buf bytes.Buffer

		err := t.Execute(&buf, &ctx)
		if err != nil {
			return err
		}

		// format using goimports
		output, err := imports.Process("", buf.Bytes(), nil)
		if err != nil {
			return err
		}

		_, err = w.Write(output)
		return err
	}

	return fmt.Errorf("struct %s not found", target)
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
			if ctx.Struct.Pk.Name != "" {
				return errors.New("only one pk field is allowed")
			}

			ctx.Struct.Pk.Name = fd.Names[0].Name
			ctx.Struct.Pk.Type = fd.Type.(*ast.Ident).Name
		default:
			return fmt.Errorf("unsupported genji tag '%s'", gtag)
		}
	}

	return nil
}

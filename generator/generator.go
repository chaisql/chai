package generator

import (
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"io"
	"strings"
	"text/template"
)

var t *template.Template

func init() {
	t = template.Must(template.New("").Parse(tmpl))
}

const tmpl = `
{{- $fl := .Struct.FirstLetter -}}
{{- $structName := .Struct.Name -}}
package {{.Package}}

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// Field implements the field method of the record.Record interface.
func ({{$fl}} *{{$structName}}) Field(name string) (*field.Field, error) {
	switch name {
	{{- range .Struct.Fields }}
	case "{{.Name}}":
		{{- if eq .Type "string"}}
		return &field.Field{
			Name: "{{.Name}}",
			Type: field.String,
			Data: []byte({{$fl}}.{{.Name}}),
		}, nil
		{{- else if eq .Type "int64"}}
		return &field.Field{
			Name: "{{.Name}}",
			Type: field.Int64,
			Data: field.EncodeInt64({{$fl}}.{{.Name}}),
		}, nil
		{{- end}}
	{{- end}}
	}

	return nil, errors.New("unknown field")
}

{{- $cursor := printf "%sCursor" .Struct.Unexported }}

func ({{$fl}} *{{$structName}}) Cursor() record.Cursor {
	return &{{$cursor}} {
		{{$structName}}: {{$fl}},
		i : -1,
	}
}

type {{$cursor}} struct {
	{{$structName}} *{{$structName}}
	i int
}

func (c *{{$cursor}}) Next() bool {
	if c.i + 2 > {{len .Struct.Fields}} {
		return false
	}

	c.i++
	return true
}

func (c *{{$cursor}}) Field() (*field.Field, error) {
	switch c.i {
	{{- range $i, $a := .Struct.Fields }}
	case {{$i}}:
		return c.{{$structName}}.Field("{{$a.Name}}")
	{{- end}}
	}

	return nil, errors.New("cursor has no more fields")
}

func (c *{{$cursor}}) Err() error {
	return nil
}
`

type context struct {
	Package string
	Struct  struct {
		Name        string
		Unexported  string
		FirstLetter string
		Fields      []struct {
			Name, Type string
		}
	}
}

// Generate parses the given ast, looks for the target struct
// and generates complementary code to the given writer.
func Generate(f *ast.File, target string, w io.Writer) error {
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

		var ctx context
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
		}

		return t.Execute(w, &ctx)
	}

	return fmt.Errorf("struct %s not found", target)
}

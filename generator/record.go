package generator

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"io"
	"strconv"
	"strings"
	"text/template"
	"unicode"
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

	{{range .Fields}}
	f, _ = {{$fl}}.Field("{{.Name}}")
	err = fn(f)
	if err != nil {
		return err
	}
	{{end}}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the record.Scanner interface.
func ({{$fl}} *{{$structName}}) ScanRecord(rec record.Record) error {
	var f field.Field
	var err error

	{{range .Fields}}
		f, err = rec.Field("{{.Name}}")
		if err != nil {
			return err
		}
		{{- if eq .Type "string"}}
		{{$fl}}.{{.Name}} = string(f.Data)
		{{- else if eq .Type "int64"}}
		{{$fl}}.{{.Name}}, err = field.DecodeInt64(f.Data)
		if err != nil {
			return err
		}
		{{- end}}
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

// {{$structName}}Store manages the table. It provides several typed helpers
// that simplify common operations.
type {{$structName}}Store struct {
	*genji.Store
}

// {{.NameWithPrefix "New"}}Store creates a {{$structName}}Store.
func {{.NameWithPrefix "New"}}Store(db *genji.DB) *{{$structName}}Store {
	schema := record.Schema{
		Fields: []field.Field{
		{{- range .Fields}}
			{{- if eq .Type "string"}}
			{Name: "{{.Name}}", Type: field.String},
			{{- else if eq .Type "int64"}}
			{Name: "{{.Name}}", Type: field.Int64},
			{{- end}}
		{{- end}}
		},
	}

	return &{{$structName}}Store{Store: genji.NewStore(db, "{{$structName}}", &schema)}
}

// {{.NameWithPrefix "New"}}StoreWithTx creates a {{$structName}}Store valid for the lifetime of the given transaction.
func {{.NameWithPrefix "New"}}StoreWithTx(tx *genji.Tx) *{{$structName}}Store {
	schema := record.Schema{
		Fields: []field.Field{
		{{- range .Fields}}
			{{- if eq .Type "string"}}
			{Name: "{{.Name}}", Type: field.String},
			{{- else if eq .Type "int64"}}
			{Name: "{{.Name}}", Type: field.Int64},
			{{- end}}
		{{- end}}
		},
	}

	return &{{$structName}}Store{Store: genji.NewStoreWithTx(tx, "{{$structName}}", &schema)}
}

// Insert a record in the table and return the primary key.
{{- if eq .Pk.Name ""}}
func ({{$fl}} *{{$structName}}Store) Insert(record *{{$structName}}) (rowid []byte, err error) {
	return {{$fl}}.Store.Insert(record)
}
{{- else }}
func ({{$fl}} *{{$structName}}Store) Insert(record *{{$structName}}) (err error) {
	_, err = {{$fl}}.Store.Insert(record)
	return err
}
{{- end}}

// Get a record using its primary key.
{{- if eq .Pk.Name ""}}
func ({{$fl}} *{{$structName}}Store) Get(rowid []byte) (*{{$structName}}, error) {
{{- else}}
	{{- if eq .Pk.Type "string"}}
func ({{$fl}} *{{$structName}}Store) Get(pk string) (*{{$structName}}, error) {
	{{- else if eq .Pk.Type "int64"}}
func ({{$fl}} *{{$structName}}Store) Get(pk int64) (*{{$structName}}, error) {
	{{- end}}
{{- end}}
	var record {{$structName}}

	{{- if ne .Pk.Name ""}}
		{{- if eq .Pk.Type "string"}}
			rowid := []byte(pk)
		{{- else if eq .Pk.Type "int64"}}
			rowid := field.EncodeInt64(pk)
		{{end}}
	{{- end}}

	return &record, {{$fl}}.Store.Get(rowid, &record)
}

{{- if ne .Pk.Name ""}}
// Delete a record using its primary key.
	{{- if eq .Pk.Type "string"}}
func ({{$fl}} *{{$structName}}Store) Delete(pk string) error {
	rowid := []byte(pk)
	{{- else if eq .Pk.Type "int64"}}
func ({{$fl}} *{{$structName}}Store) Delete(pk int64) error {
	rowid := field.EncodeInt64(pk)
	{{- end}}
	return {{$fl}}.Store.Delete(rowid)
}
{{- end}}

// List records from the specified offset. If the limit is equal to -1, it returns all records after the selected offset.
func ({{$fl}} *{{$structName}}Store) List(offset, limit int) ([]{{$structName}}, error) {
	size := limit
	if size == -1 {
		size = 0
	}
	list := make([]{{$structName}}, 0, size)
	err := {{$fl}}.Store.List(offset, limit, func(rowid []byte, r record.Record) error {
		var record {{$structName}}
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}
		list = append(list, record)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}

{{ if eq .Pk.Name ""}}
func ({{$fl}} *{{$structName}}Store) Replace(rowid []byte, record *{{$structName}}) error {
{{- else}}
	{{- if eq .Pk.Type "string"}}
func ({{$fl}} *{{$structName}}Store) Replace(pk string, record *{{$structName}}) error {
	rowid := []byte(pk)
	if record.{{ .Pk.Name }} == "" && record.{{ .Pk.Name }} != pk {
		record.{{ .Pk.Name }} = pk
	}

	{{- else if eq .Pk.Type "int64"}}
func ({{$fl}} *{{$structName}}Store) Replace(pk int64, record *{{$structName}}) error {
	rowid := field.EncodeInt64(pk)
	if record.{{ .Pk.Name }} == 0 && record.{{ .Pk.Name }} != pk {
		record.{{ .Pk.Name }} = pk
	}
	{{- end}}
{{- end}}
	return {{$fl}}.Store.Replace(rowid, record)
}

// {{$structName}}QuerySelector provides helpers for selecting fields from the {{$structName}} structure.
type {{$structName}}QuerySelector struct{
{{- range $i, $a := .Fields }}
	{{- if eq .Type "string"}}
		{{$a.Name}} query.StrField
	{{- else if eq .Type "int64"}}
		{{$a.Name}} query.Int64Field
	{{- end}}
{{- end}}
}

{{- if .IsExported }}
// New{{$structName}}QuerySelector creates a {{$structName}}QuerySelector.
func New{{$structName}}QuerySelector() {{$structName}}QuerySelector {
{{- else}}
// new{{$structName}}QuerySelector creates a {{$structName}}QuerySelector.
func new{{.ExportedName}}QuerySelector() {{$structName}}QuerySelector {
{{- end}}
	return {{$structName}}QuerySelector{
		{{- range $i, $a := .Fields }}
			{{- if eq .Type "string"}}
				{{$a.Name}}: query.NewStrField("{{$a.Name}}"),
			{{- else if eq .Type "int64"}}
				{{$a.Name}}: query.NewInt64Field("{{$a.Name}}"),
			{{- end}}
		{{- end}}
	}
}

// Table returns a query.TableSelector for {{$structName}}.
func (*{{$structName}}QuerySelector) Table() query.TableSelector {
	return query.Table("{{$structName}}")
}

// All returns a list of all selectors for {{$structName}}.
func (s *{{$structName}}QuerySelector) All() []query.FieldSelector {
	return []query.FieldSelector{
		{{- range $i, $a := .Fields }}
		s.{{$a.Name}},
		{{- end}}
	}
}

// {{$structName}}Result can be used to store the result of queries.
// Selected fields must map the {{$structName}} fields.
type {{$structName}}Result []{{$structName}}

// ScanTable iterates over table.Reader and stores all the records in the slice.
func ({{$fl}} *{{$structName}}Result) ScanTable(tr table.Reader) error {
	return tr.Iterate(func(_ []byte, r record.Record) error {
		var record {{$structName}}
		err := record.ScanRecord(r)
		if err != nil {
			return err
		}

		*{{$fl}} = append(*{{$fl}}, record)
		return nil
	})
}
`

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
	if !s.IsExported() {
		return s.Name
	}

	return s.Unexport(s.Name)
}

func (s *recordContext) ExportedName() string {
	if s.IsExported() {
		return s.Name
	}

	return s.Export(s.Name)
}

func (s *recordContext) NameWithPrefix(prefix string) string {
	n := prefix + s.ExportedName()
	if s.IsExported() {
		return s.Export(n)
	}

	return s.Unexport(n)
}

func (s *recordContext) Export(n string) string {
	name := []byte(n)
	name[0] = byte(unicode.ToUpper(rune(n[0])))
	return string(name)
}

func (s *recordContext) Unexport(n string) string {
	name := []byte(n)
	name[0] = byte(unicode.ToLower(rune(n[0])))
	return string(name)
}

// GenerateRecords parses the given ast, looks for the targets structs
// and generates complementary code to the given writer.
func GenerateRecords(w io.Writer, f *ast.File, targets ...string) error {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "package %s\n", f.Name.Name)

	fmt.Fprintf(&buf, `
	import (
		"errors"

		"github.com/asdine/genji"
		"github.com/asdine/genji/field"
		"github.com/asdine/genji/query"
		"github.com/asdine/genji/record"
		"github.com/asdine/genji/table"
	)
	`)

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
	output, err := format.Source(buf.Bytes())
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

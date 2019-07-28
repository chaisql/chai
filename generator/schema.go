package generator

const tableSchemaTmpl = `
{{ define "table-schema" }}

{{ template "table-schema-Struct" . }}
{{ template "table-schema-New" . }}
{{ template "table-schema-Init" . }}
{{ template "table-schema-Table" . }}
{{ template "table-schema-TableName" . }}
{{ template "table-schema-Indexes" . }}
{{ template "table-schema-All" . }}
{{ end }}
`

const tableSchemaStructTmpl = `
{{ define "table-schema-Struct" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// {{$structName}}TableSchema provides provides information about the {{.TableName}} table.
type {{$structName}}TableSchema struct{
{{- range $i, $a := .Fields }}
	{{$a.Name}} query.{{.Type}}FieldSelector
{{- end}}
}
{{ end }}
`

const tableSchemaNewTmpl = `
{{ define "table-schema-New" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
{{- if .IsExported }}
// New{{$structName}}TableSchema creates a {{$structName}}TableSchema.
func New{{$structName}}TableSchema() {{$structName}}TableSchema {
{{- else}}
// new{{$structName}}TableSchema creates a {{$structName}}TableSchema.
func new{{.ExportedName}}TableSchema() {{$structName}}TableSchema {
{{- end}}
	return {{$structName}}TableSchema {
		{{- range $i, $a := .Fields }}
			{{$a.Name}}: query.{{.Type}}Field("{{$a.Name}}"),
		{{- end}}
	}
}
{{ end }}
`

const tableSchemaInitTmpl = `
{{ define "table-schema-Init" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// Init initializes the {{.TableName}} table by ensuring the table and its index are created.
func (s *{{$structName}}TableSchema) Init(tx *genji.Tx) error {
	return genji.InitTable(tx, s)
}
{{ end }}
`

const tableSchemaTableNameTmpl = `
{{ define "table-schema-TableName" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// TableName returns the name of the table.
func (s *{{$structName}}TableSchema) TableName() string {
	return "{{.TableName}}"
}
{{ end }}
`

const tableSchemaIndexesTmpl = `
{{ define "table-schema-Indexes" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
{{- if .HasIndexes }}
// Indexes returns the list of indexes of the {{.TableName}} table.
func (*{{$structName}}TableSchema) Indexes() []string {
	return []string{
		{{- range $i, $a := .Indexes }}
			"{{$a}}",
		{{- end}}
	}
}
{{- end }}
{{ end }}
`

const tableSchemaTableTmpl = `
{{ define "table-schema-Table" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// Table returns a query.TableSelector for {{$structName}}.
func (*{{$structName}}TableSchema) Table() query.TableSelector {
	return query.Table("{{.TableName}}")
}
{{ end }}
`

const tableSchemaAllTmpl = `
{{ define "table-schema-All" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// All returns a list of all selectors for {{$structName}}.
func (s *{{$structName}}TableSchema) All() []query.FieldSelector {
	return []query.FieldSelector{
		{{- range $i, $a := .Fields }}
		s.{{$a.Name}},
		{{- end}}
	}
}
{{ end }}
`

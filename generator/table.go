package generator

const tableTmpl = `
{{ define "table" }}

{{ template "table-Struct" . }}
{{ template "table-New" . }}
{{ template "table-Init" . }}
{{ template "table-SelectTable" . }}
{{ template "table-Insert" . }}
{{ template "table-TableName" . }}
{{ template "table-Indexes" . }}
{{ end }}
`

const tableStructTmpl = `
{{ define "table-Struct" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// {{$structName}}Table manages the {{.TableName}} table.
type {{$structName}}Table struct{
{{- range $i, $a := .Fields }}
	{{$a.Name}} query.{{.Type}}FieldSelector
{{- end}}
}
{{ end }}
`

const tableNewTmpl = `
{{ define "table-New" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
{{- if .IsExported }}
// New{{$structName}}Table creates a {{$structName}}Table.
func New{{$structName}}Table() *{{$structName}}Table {
{{- else}}
// new{{$structName}}Table creates a {{$structName}}Table.
func new{{.ExportedName}}Table() *{{$structName}}Table {
{{- end}}
	return &{{$structName}}Table {
		{{- range $i, $a := .Fields }}
			{{$a.Name}}: query.{{.Type}}Field("{{$a.Name}}"),
		{{- end}}
	}
}
{{ end }}
`

const tableInitTmpl = `
{{ define "table-Init" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// Init initializes the {{.TableName}} table by ensuring the table and its index are created.
func (t *{{$structName}}Table) Init(tx *genji.Tx) error {
	return genji.InitTable(tx, t)
}
{{ end }}
`

const tableSelectTableTmpl = `
{{ define "table-SelectTable" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// SelectTable implements the query.TableSelector interface. It gets the {{.TableName}} table from
// the transaction.
func (t *{{$structName}}Table) SelectTable(tx *genji.Tx) (*genji.Table, error) {
	return tx.Table(t.TableName())
}
{{ end }}
`

const tableInsertTmpl = `
{{ define "table-Insert" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// Insert is a shortcut that gets the {{.TableName}} table from the transaction and 
// inserts a {{$structName}} into it.
{{- if ne .Pk.Name ""}}
func (t *{{$structName}}Table) Insert(tx *genji.Tx, x *{{$structName}}) error {
	tb, err := t.SelectTable(tx)
	if err != nil {
		return err
	}

	_, err = tb.Insert(x)
	return err
}
{{- else}}
func (t *{{$structName}}Table) Insert(tx *genji.Tx, x *{{$structName}}) ([]byte, error) {
	tb, err := t.SelectTable(tx)
	if err != nil {
		return nil, err
	}

	return tb.Insert(x)
}
{{end}}
{{ end }}
`

const tableTableNameTmpl = `
{{ define "table-TableName" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// TableName returns the name of the table.
func (*{{$structName}}Table) TableName() string {
	return "{{.TableName}}"
}
{{ end }}
`

const tableIndexesTmpl = `
{{ define "table-Indexes" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
{{- if .HasIndexes }}
// Indexes returns the list of indexes of the {{.TableName}} table.
func (*{{$structName}}Table) Indexes() map[string]index.Options {
	return map[string]index.Options{
		{{- range $i, $a := .Indexes }}
			"{{$a.FieldName}}": index.Options{Unique: {{$a.Unique}}},
		{{- end}}
	}
}
{{- end }}
{{ end }}
`

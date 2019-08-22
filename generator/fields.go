package generator

const fieldsTmpl = `
{{ define "fields" }}

{{ template "fields-Struct" . }}
{{ template "fields-New" . }}
{{ end }}
`

const fieldsStructTmpl = `
{{ define "fields-Struct" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// {{$structName}}Fields describes the fields of the {{$structName}} record.
// It can be used to select fields during queries.
type {{$structName}}Fields struct{
{{- range $i, $a := .Fields }}
	{{$a.Name}} query.{{.Type}}FieldSelector
{{- end}}
}
{{ end }}
`

const fieldsNewTmpl = `
{{ define "fields-New" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// {{.NameWithPrefix "New"}}Fields creates a {{$structName}}Fields.
func {{.NameWithPrefix "New"}}Fields() *{{$structName}}Fields {
	return &{{$structName}}Fields {
		{{- range $i, $a := .Fields }}
			{{$a.Name}}: query.{{.Type}}Field("{{$a.Name}}"),
		{{- end}}
	}
}
{{ end }}
`

const indexesTmpl = `
{{ define "indexes" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
{{- if .HasIndexes }}
// {{.NameWithPrefix "New"}}Indexes creates a map containing the configuration for each index of the table.
func {{.NameWithPrefix "New"}}Indexes() map[string]index.Options {
	return map[string]index.Options{
		{{- range $i, $a := .Indexes }}
			"{{$a.FieldName}}": index.Options{Unique: {{$a.Unique}}},
		{{- end}}
	}
}
{{- end }}
{{ end }}
`

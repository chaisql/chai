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

package generator

import (
	"go/ast"
	"io"
)

const resultsTmpl = `
{{- define "results" }}
  {{- range .Results }}
	{{- template "record-ScanRecord" . }}
    {{- template "result" . }}
  {{- end }}
{{- end }}
`

const resultTmpl = `
{{ define "result" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

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
{{ end }}
`

// GenerateResults parses the given asts, looks for the targets structs
// and generates complementary code to the given writer.
func GenerateResults(w io.Writer, files []*ast.File, targets []string) error {
	return nil
}

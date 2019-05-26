package generator

const storeTmpl = `
{{ define "store" }}

{{ template "store-Struct" . }}
{{ template "store-New" . }}
{{ template "store-NewWithTx" . }}
{{ template "store-Insert" . }}
{{ template "store-Get" . }}
{{ template "store-Delete" . }}
{{ template "store-List" . }}
{{ template "store-Replace" . }}
{{ end }}
`

const storeStructTmpl = `
{{ define "store-Struct" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// {{$structName}}Store manages the table. It provides several typed helpers
// that simplify common operations.
type {{$structName}}Store struct {
	*genji.Store
}
{{ end }}
`

const storeNewTmpl = `
{{ define "store-New" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// {{.NameWithPrefix "New"}}Store creates a {{$structName}}Store.
func {{.NameWithPrefix "New"}}Store(db *genji.DB) *{{$structName}}Store {
	var schema *record.Schema
	{{- if .Schema}}
	schema = &record.Schema{
		Fields: []field.Field{
		{{- range .Fields}}
			{Name: "{{.Name}}", Type: field.{{.Type}}},
		{{- end}}
		},
	}
	{{- end}}

	var indexes []string
	{{- if .HasIndexes }}
		{{- range .Indexes }}
		indexes = append(indexes, "{{.}}")
		{{- end }}
	{{- end }}

	return &{{$structName}}Store{Store: genji.NewStore(db, "{{$structName}}", schema, indexes)}
}
{{ end }}
`

const storeNewWithTxTmpl = `
{{ define "store-NewWithTx" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// {{.NameWithPrefix "New"}}StoreWithTx creates a {{$structName}}Store valid for the lifetime of the given transaction.
func {{.NameWithPrefix "New"}}StoreWithTx(tx *genji.Tx) *{{$structName}}Store {
	var schema *record.Schema
	{{- if .Schema}}
	schema = &record.Schema{
		Fields: []field.Field{
		{{- range .Fields}}
			{Name: "{{.Name}}", Type: field.{{.Type}}},
		{{- end}}
		},
	}
	{{- end}}

	var indexes []string
	{{- if .HasIndexes }}
		{{ range .Indexes }}
		indexes = append(indexes, "{{.}}")
		{{- end }}
	{{- end }}

	return &{{$structName}}Store{Store: genji.NewStoreWithTx(tx, "{{$structName}}", schema, indexes)}
}
{{ end }}
`

const storeInsertTmpl = `
{{ define "store-Insert" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
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
{{ end }}
`

const storeGetTmpl = `
{{ define "store-Get" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// Get a record using its primary key.
{{- if eq .Pk.Name ""}}
func ({{$fl}} *{{$structName}}Store) Get(rowid []byte) (*{{$structName}}, error) {
{{- else}}
func ({{$fl}} *{{$structName}}Store) Get(pk {{.Pk.GoType}}) (*{{$structName}}, error) {
{{- end}}
	var record {{$structName}}

	{{- if ne .Pk.Name ""}}
		rowid := field.Encode{{.Pk.Type}}(pk)
	{{- end}}

	return &record, {{$fl}}.Store.Get(rowid, &record)
}
{{ end }}
`

const storeDeleteTmpl = `
{{ define "store-Delete" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}

// Delete a record using its primary key.
{{- if ne .Pk.Name ""}}
func ({{$fl}} *{{$structName}}Store) Delete(pk {{.Pk.GoType}}) error {
	rowid := field.Encode{{.Pk.Type}}(pk)
	return {{$fl}}.Store.Delete(rowid)
}
{{- else }}
func ({{$fl}} *{{$structName}}Store) Delete(rowid []byte) error {
	return {{$fl}}.Store.Delete(rowid)
}
{{- end}}
{{ end }}
`

const storeListTmpl = `
{{ define "store-List" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
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
{{ end }}
`

const storeReplaceTmpl = `
{{ define "store-Replace" }}
{{- $fl := .FirstLetter -}}
{{- $structName := .Name -}}
// Replace the selected record by the given one.
{{- if eq .Pk.Name ""}}
func ({{$fl}} *{{$structName}}Store) Replace(rowid []byte, record *{{$structName}}) error {
{{- else}}
func ({{$fl}} *{{$structName}}Store) Replace(pk {{.Pk.GoType}}, record *{{$structName}}) error {
	rowid := field.Encode{{.Pk.Type}}(pk)
	if record.{{ .Pk.Name }} != pk {
		record.{{ .Pk.Name }} = pk
	}
{{- end}}
	return {{$fl}}.Store.Replace(rowid, record)
}
{{ end }}
`

package generator

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"text/template"

	"golang.org/x/tools/imports"
)

const tmpl = `
{{ define "base" }}
package {{ .Pkg }}

import (
	{{- range .Imports }}
	"{{ . }}"
	{{- end }}
)

{{ template "records" . }}
{{ template "results" . }}

{{- end }}
`

var t *template.Template

func init() {
	templates := map[string]string{
		"records":              recordsTmpl,
		"record":               recordTmpl,
		"record-Field":         recordFieldTmpl,
		"record-Iterate":       recordIterateTmpl,
		"record-Pk":            recordPkTmpl,
		"record-ScanRecord":    recordScanRecordTmpl,
		"store":                storeTmpl,
		"store-Struct":         storeStructTmpl,
		"store-New":            storeNewTmpl,
		"store-NewWithTx":      storeNewWithTxTmpl,
		"store-Insert":         storeInsertTmpl,
		"store-Get":            storeGetTmpl,
		"store-Delete":         storeDeleteTmpl,
		"store-List":           storeListTmpl,
		"store-Replace":        storeReplaceTmpl,
		"query-Selector":       querySelectorTmpl,
		"query-SelectorStruct": querySelectorStructTmpl,
		"query-SelectorNew":    querySelectorNewTmpl,
		"query-SelectorTable":  querySelectorTableTmpl,
		"query-SelectorAll":    querySelectorAllTmpl,
		"results":              resultsTmpl,
		"result":               resultTmpl,
	}

	t = template.Must(template.New("main").Parse(tmpl))
	for k, v := range templates {
		t = template.Must(t.New(k).Parse(v))
	}
}

type Options struct {
	Sources []io.Reader
	Records []string
	Results []string
}

// Generate parses the given asts, looks for the targets structs
// and generates complementary code to the given writer.
func Generate(w io.Writer, opts Options) error {
	var gctx genContext

	srcs, err := readSources(opts.Sources)
	if err != nil {
		return err
	}

	err = gctx.readPackage(srcs)
	if err != nil {
		return err
	}

	err = gctx.readTargets(srcs, &opts)
	if err != nil {
		return err
	}

	gctx.selectImports()

	var buf bytes.Buffer

	// generate code
	err = t.ExecuteTemplate(&buf, "base", &gctx)
	if err != nil {
		return err
	}

	// format using goimports
	output, err := imports.Process("", buf.Bytes(), &imports.Options{
		TabWidth:   8,
		TabIndent:  true,
		Comments:   true,
		FormatOnly: true,
	})
	if err != nil {
		return err
	}

	_, err = w.Write(output)
	return err
}

func readSources(srcs []io.Reader) ([]*ast.File, error) {
	var buf bytes.Buffer
	afs := make([]*ast.File, len(srcs))

	for i, r := range srcs {
		buf.Reset()
		_, err := buf.ReadFrom(r)
		if err != nil {
			return nil, err
		}

		fset := token.NewFileSet()
		af, err := parser.ParseFile(fset, "", buf.String(), 0)
		if err != nil {
			return nil, err
		}
		afs[i] = af
	}

	return afs, nil
}

type genContext struct {
	Pkg     string
	Imports []string
	Records []recordContext
	Results []recordContext
}

func (g *genContext) readPackage(srcs []*ast.File) error {
	var pkg string

	for _, src := range srcs {
		if pkg != "" && pkg != src.Name.Name {
			return errors.New("input files must belong to the same package")
		}
		pkg = src.Name.Name
	}

	g.Pkg = pkg
	return nil
}

func (g *genContext) readTargets(srcs []*ast.File, opts *Options) error {
	g.Records = make([]recordContext, len(opts.Records))
	for i := range opts.Records {
		for _, src := range srcs {
			ok, err := g.Records[i].lookupRecord(src, opts.Records[i])
			if err != nil {
				return err
			}
			if ok {
				break
			}
		}
	}

	g.Results = make([]recordContext, len(opts.Results))
	for i := range opts.Results {
		for _, src := range srcs {
			ok, err := g.Results[i].lookupRecord(src, opts.Results[i])
			if err != nil {
				return err
			}
			if ok {
				break
			}
		}
	}

	return nil
}

func (g *genContext) selectImports() {
	m := make(map[string]int)

	if len(g.Records) > 0 {
		m["errors"]++
		m["github.com/asdine/genji"]++
		m["github.com/asdine/genji/field"]++
		m["github.com/asdine/genji/query"]++
		m["github.com/asdine/genji/record"]++
		m["github.com/asdine/genji/table"]++
	}

	if len(g.Results) > 0 {
		m["github.com/asdine/genji/record"]++
		m["github.com/asdine/genji/table"]++
	}

	g.Imports = make([]string, 0, len(m))
	for k := range m {
		g.Imports = append(g.Imports, k)
	}
}

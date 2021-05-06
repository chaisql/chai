package main

import (
	"bufio"
	"embed"
	"io"
	"path"
	"strings"
	"text/template"
)

//go:embed "test_template.go.tmpl"
var tmplFS embed.FS

type statement struct {
	Expr   []string
	Result []string
	Error  string
}

type test struct {
	Name       string
	Statements []*statement
}

type testSuite struct {
	Filename string
	Setup    []string
	Tests    []*test
}

func parse(r io.Reader, filename string, origName string) *testSuite {
	s := bufio.NewScanner(r)
	ts := testSuite{
		Filename: filename,
	}

	var curTest *test
	var curStmt *statement

	var readingResult bool
	var readingSetup bool

	for s.Scan() {
		line := s.Text()

		// keep result indentation intact
		if !readingResult {
			line = strings.TrimSpace(line)
		}

		switch {
		case line == "":
			if readingResult {
				// results are terminated by a blank line
				curStmt = nil
				readingResult = false
			}

		case strings.HasPrefix(line, "-- setup:"):
			readingSetup = true

		case strings.HasPrefix(line, "-- test:"):
			readingSetup = false

			// create a new test
			name := strings.TrimPrefix(line, "-- test: ")
			curTest = &test{
				Name: name,
			}
			ts.Tests = append(ts.Tests, curTest)
			curStmt = nil

		case strings.HasPrefix(line, "-- result:"):
			readingResult = true

		case strings.HasPrefix(line, "-- error:"):
			error := strings.TrimPrefix(line, "-- error: ")
			curStmt.Error = error
			curStmt = nil

		case strings.HasPrefix(line, "--"): // ignore normal comments

		default:
			if readingSetup {
				ts.Setup = append(ts.Setup, line)
			} else if readingResult {
				curStmt.Result = append(curStmt.Result, line)
			} else {
				if curStmt == nil {
					curStmt = new(statement)
					curTest.Statements = append(curTest.Statements, curStmt)
				}

				curStmt.Expr = append(curStmt.Expr, line)
			}
		}
	}

	return &ts
}

func generate(ts *testSuite, packageName string, w io.Writer) error {
	base := path.Base(ts.Filename)
	name := strings.TrimSuffix(base, path.Ext(ts.Filename))
	testName := strings.Title(name)

	tmpl := template.Must(template.ParseFS(tmplFS, "test_template.go.tmpl"))
	bindings := struct {
		Package  string
		TestName string
		Suite    *testSuite
	}{
		packageName,
		testName,
		ts,
	}

	return tmpl.Execute(w, bindings)
}

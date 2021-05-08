package main

import (
	"bufio"
	"embed"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/template"
)

//go:embed "test_template.go.tmpl"
var tmplFS embed.FS

type statement struct {
	Expr       []string
	Result     []string
	ErrorMatch string
	Fails      bool
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

func parse(r io.Reader, filename string) *testSuite {
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
			// ignore blank lines
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

		case strings.HasPrefix(line, "/* result:"):
			readingResult = true

		case strings.HasPrefix(line, "-- error:"):
			error := strings.TrimPrefix(line, "-- error:")
			error = strings.TrimSpace(error)
			if error == "" {
				// handle the case where error was used but without a message
				curStmt.Fails = true
			} else {
				curStmt.ErrorMatch = error
				curStmt.Fails = true
			}
			curStmt = nil

		case strings.HasPrefix(line, "--"): // ignore normal comments

		default:
			if readingSetup {
				ts.Setup = append(ts.Setup, line)
			} else if readingResult && strings.TrimSpace(line) == "*/" {
				readingResult = false
				curStmt = nil
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

// some sql query use backticks which can mess up the templating
func escapeBackticks(text string) string {
	return strings.ReplaceAll(text, "`", "`+\"`\"+`")
}

func camelize(str string) string {
	str = strings.ReplaceAll(str, "_", " ")
	str = strings.Title(str)
	str = strings.ReplaceAll(str, " ", "")

	return str
}

func generate(ts *testSuite, packageName string, w io.Writer) error {
	name := normalize(ts.Filename)

	funcMap := template.FuncMap{
		"escapeBackticks": escapeBackticks,
	}

	tmpl, err := template.New("test_template.go.tmpl").Funcs(funcMap).ParseFS(tmplFS, "test_template.go.tmpl")
	if err != nil {
		return err
	}

	bindings := struct {
		Package  string
		TestName string
		Suite    *testSuite
	}{
		packageName,
		camelize(name),
		ts,
	}

	return tmpl.Execute(w, bindings)
}

// normalize transforms "/foo/bar/my_foo_test.sql" into "my_foo"
func normalize(filepath string) string {
	base := path.Base(filepath)
	name := strings.TrimSuffix(base, path.Ext(filepath))
	return strings.TrimSuffix(name, "_test")
}

var packageName string

func init() {
	flag.StringVar(&packageName, "package", "", "package name for the generated files")
	flag.Usage = func() {
		fmt.Println("Usage: ./examplar -package=[NAME] input1 input2 ...")
	}
}

func main() {
	flag.Parse()

	if packageName == "" {
		flag.Usage()
		os.Exit(-1)
	}

	paths := os.Args[2:]
	if len(paths) < 1 {
		flag.Usage()
		os.Exit(-1)
	}

	for _, p := range paths {
		// use globs because when invoked from go generate, there will be no shell
		// to expand it for us
		gpaths, err := filepath.Glob(p)
		if err != nil {
			log.Fatal(err)
		}

		if len(gpaths) < 1 {
			log.Fatalf("%s does not exist", p)
		}

		for _, gp := range gpaths {
			f, err := os.Open(gp)
			if err != nil {
				panic(err)
			}

			ts := parse(f, gp)

			name := normalize(gp)

			out, err := os.OpenFile(name+"_test.go", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				panic(err)
			}

			err = generate(ts, packageName, out)
			if err != nil {
				panic(err)
			}
		}
	}
}

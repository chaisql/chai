package main

import (
	"bufio"
	"bytes"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"golang.org/x/tools/imports"
)

//go:embed "test_template.go.tmpl"
var tmplFS embed.FS

type statement struct {
	Expr       []string
	Result     []string
	ErrorMatch string
	Fails      bool
	Sorted     bool
}

type test struct {
	Name       string
	Statements []*statement
}

type suite struct {
	Name       string
	Statements []string
}

type testSuite struct {
	Filename string
	Setup    []string
	Suites   []suite
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
	var readingSuite bool
	var suiteIndex int = -1

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
		case strings.HasPrefix(line, "-- suite:"):
			readingSuite = true
			suiteIndex++
			ts.Suites = append(ts.Suites, suite{
				Name: strings.TrimPrefix(line, "-- suite: "),
			})
		case strings.HasPrefix(line, "-- test:"):
			readingSetup = false
			readingSuite = false

			// create a new test
			name := strings.TrimPrefix(line, "-- test: ")
			curTest = &test{
				Name: name,
			}
			ts.Tests = append(ts.Tests, curTest)
			curStmt = nil

		case strings.HasPrefix(line, "/* result:"):
			readingResult = true
		case strings.HasPrefix(line, "/* sorted-result:"):
			readingResult = true
			curStmt.Sorted = true
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
			if readingSuite {
				ts.Suites[suiteIndex].Statements = append(ts.Suites[suiteIndex].Statements, line)
			} else if readingSetup {
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

func concatSlices(s1, s2 []string) []string {
	return append(s1, s2...)
}

func dict(values ...interface{}) (map[string]interface{}, error) {
	if len(values)%2 != 0 {
		return nil, errors.New("invalid dict call")
	}
	dict := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("dict keys must be strings")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
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
		"concatSlices":    concatSlices,
		"dict":            dict,
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

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, bindings)
	if err != nil {
		return err
	}

	output, err := imports.Process("", buf.Bytes(), &imports.Options{
		TabWidth:  8,
		TabIndent: true,
		Comments:  true,
	})

	if err != nil {
		return fmt.Errorf("go imports error: %w", err)
	}

	_, err = w.Write(output)
	return err
}

// normalize transforms "/foo/bar/my_foo_test.sql" into "my_foo"
func normalize(filepath string) string {
	base := path.Base(filepath)
	name := strings.TrimSuffix(base, path.Ext(filepath))
	return strings.TrimSuffix(name, "_test")
}

var packageName string
var genDir bool
var outputDir string
var exclude string

func init() {
	flag.StringVar(&packageName, "package", "", "package name for the generated files")
	flag.BoolVar(&genDir, "gen-dir", false, "create source parent directory in this directory")
	flag.StringVar(&outputDir, "output-dir", "", "target directory")
	flag.StringVar(&exclude, "exclude", "", "exclude directory")
	flag.Usage = func() {
		fmt.Println("Usage: ./gensqltest -package=[NAME] input1 input2 ...")
	}
}

func main() {
	flag.Parse()

	if packageName == "" {
		flag.Usage()
		os.Exit(-1)
	}

	paths := flag.Args()
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

		if exclude != "" {
			gpaths, err = filterGlob(gpaths, exclude)
			if err != nil {
				log.Fatal(err)
			}
		}

		if len(gpaths) == 0 {
			log.Fatalf("%s is empty", p)
		}

		for _, gp := range gpaths {
			f, err := os.Open(gp)
			if err != nil {
				panic(err)
			}

			ts := parse(f, gp)

			name := normalize(gp)

			var outputPath string
			if genDir {
				parentDir := filepath.Dir(ts.Filename)
				outputPath = filepath.Join(outputDir, filepath.Base(parentDir))
			}

			if outputPath != "" {
				if _, err := os.Stat(outputPath); errors.Is(err, os.ErrNotExist) {
					err := os.MkdirAll(outputPath, os.ModePerm)
					if err != nil {
						log.Fatal(outputPath, " ", err)
					}
				}
			}

			out, err := os.OpenFile(filepath.Join(outputPath, name+"_test.go"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				panic(err)
			}

			err = generate(ts, packageName, out)
			if err != nil {
				panic(err)
			}

			err = out.Close()
			if err != nil {
				panic(err)
			}
		}
	}
}

func filterGlob(paths []string, excludeGlob string) ([]string, error) {
	toExclude, err := filepath.Glob(exclude)
	if err != nil {
		return nil, err
	}

	files := make(map[string]struct{})

	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, err
		}

		files[abs] = struct{}{}
	}

	for _, e := range toExclude {
		abs, err := filepath.Abs(e)
		if err != nil {
			return nil, err
		}
		delete(files, abs)
	}

	filtered := make([]string, 0, len(files))
	for f := range files {
		i := sort.SearchStrings(filtered, f)
		filtered = append(filtered, "")
		copy(filtered[i+1:], filtered[i:])
		filtered[i] = f
	}

	return filtered, nil
}

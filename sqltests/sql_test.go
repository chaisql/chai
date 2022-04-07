package sql_test

import (
	"bufio"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL(t *testing.T) {
	err := filepath.Walk(".", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			if info.Name() == "expr" {
				return fs.SkipDir
			}

			return nil
		}

		if filepath.Ext(info.Name()) != ".sql" {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		ts := parse(f, path)

		absPath, err := filepath.Abs(path)
		if err != nil {
			return err
		}

		t.Run(ts.Filename, func(t *testing.T) {
			setup := func(t *testing.T, db *genji.DB) {
				t.Helper()
				err := db.Exec(ts.Setup, "\n")
				assert.NoError(t, err)
			}

			if len(ts.Suites) > 0 {
				for _, suite := range ts.Suites {
					t.Run(suite.Name, func(t *testing.T) {
						var tests []*test

						for _, tt := range suite.Tests {
							if tt.Only {
								tests = []*test{tt}
								break
							}
						}

						if tests == nil {
							tests = suite.Tests
						}

						for _, test := range tests {
							t.Run(test.Name, func(t *testing.T) {
								db, err := genji.Open(":memory:")
								assert.NoError(t, err)
								defer db.Close()

								setup(t, db)

								// post setup
								if suite.PostSetup != "" {
									err = db.Exec(suite.PostSetup)
									assert.NoError(t, err)
								}

								if test.Fails {
									err := db.Exec(test.Expr)

									if test.ErrorMatch != "" {
										require.NotNilf(t, err, "%s:%d expected error, got nil", absPath, test.Line)
										require.Equal(t, test.ErrorMatch, err.Error(), "Source %s:%d", absPath, test.Line)
									} else {
										assert.Errorf(t, err, "\nSource:%s:%d expected\n%s\nto raise an error but got none", absPath, test.Line, test.Expr)
									}
								} else {
									res, err := db.Query(test.Expr)
									assert.NoError(t, err)
									defer res.Close()

									testutil.RequireStreamEqf(t, test.Result, res, test.Sorted, "Source: %s:%d", absPath, test.Line)
								}
							})
						}
					})
				}
			}
		})

		return nil
	})

	assert.NoError(t, err)
}

type test struct {
	Name       string
	Expr       string
	Result     string
	ErrorMatch string
	Fails      bool
	Sorted     bool
	Line       int
	Only       bool
}

type suite struct {
	Name      string
	PostSetup string
	Tests     []*test
}

type testSuite struct {
	Filename string
	Setup    string
	Suites   []suite
}

func parse(r io.Reader, filename string) *testSuite {
	s := bufio.NewScanner(r)
	ts := testSuite{
		Filename: filename,
	}

	var curTest *test

	var readingResult bool
	var readingSetup bool
	var readingSuite bool
	var readingCommentBlock bool
	var suiteIndex int = -1
	var only bool

	var lineCount = 0
	for s.Scan() {
		lineCount++
		line := s.Text()

		// keep result indentation intact
		if !readingResult {
			line = strings.TrimSpace(line)
		}

		switch {
		case line == "":
		// ignore blank lines
		case readingCommentBlock && strings.TrimSpace(line) == "*/":
			readingCommentBlock = false
		case readingCommentBlock:
			// ignore comment blocks
		case strings.HasPrefix(line, "-- setup:"):
			readingSetup = true
		case strings.HasPrefix(line, "-- suite:"):
			readingSuite = true
			suiteIndex++
			ts.Suites = append(ts.Suites, suite{
				Name: strings.TrimPrefix(line, "-- suite: "),
			})
		case strings.HasPrefix(line, "-- only:"):
			only = true
			fallthrough
		case strings.HasPrefix(line, "-- test:"):
			readingSetup = false
			readingSuite = false

			// create a new test
			name := strings.TrimPrefix(line, "-- test: ")
			curTest = &test{
				Name: name,
				Line: lineCount,
				Only: only,
			}
			only = false
			// if there are no suites, create one by default
			if suiteIndex == -1 {
				suiteIndex++
				ts.Suites = append(ts.Suites, suite{
					Name: "default",
				})
			}

			ts.Suites[suiteIndex].Tests = append(ts.Suites[suiteIndex].Tests, curTest)
		case strings.HasPrefix(line, "/* result:"), strings.HasPrefix(line, "/*result:"):
			readingResult = true
		case strings.HasPrefix(line, "/* sorted-result:"):
			readingResult = true
			curTest.Sorted = true
		case strings.HasPrefix(line, "-- error:"):
			error := strings.TrimPrefix(line, "-- error:")
			error = strings.TrimSpace(error)
			if error == "" {
				// handle the case where error was used but without a message
				curTest.Fails = true
			} else {
				curTest.ErrorMatch = error
				curTest.Fails = true
			}
			curTest = nil
		case strings.HasPrefix(line, "/*"): // ignore block comments
			readingCommentBlock = true
		case strings.HasPrefix(line, "--"):
			// ignore line comments
		case !readingResult && strings.TrimSpace(line) == "*/":
		default:
			if readingSuite {
				ts.Suites[suiteIndex].PostSetup += line + "\n"
			} else if readingSetup {
				ts.Setup += line + "\n"
			} else if readingResult && strings.TrimSpace(line) == "*/" {
				readingResult = false
				curTest = nil
			} else if readingResult {
				curTest.Result += line + "\n"
			} else {
				curTest.Expr += line + "\n"
			}
		}
	}

	return &ts
}

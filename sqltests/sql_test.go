package sql_test

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

var logger *log.Logger

func logF(format string, v ...any) {
	if logger != nil {
		logger.Printf(format, v...)
	}
}

func logLn(v ...any) {
	if logger != nil {
		logger.Println(v...)
	}
}

func TestSQL(t *testing.T) {
	if testing.Verbose() {
		logger = log.New(os.Stderr, "[SQL TESTS] ", 0)
	}

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

		ts, err := parse(f, path)
		if err != nil {
			return err
		}

		absPath, err := filepath.Abs(path)
		if err != nil {
			return err
		}

		t.Run(ts.Filename, func(t *testing.T) {
			setup := func(t *testing.T, db *sql.DB) {
				t.Helper()
				_, err := db.Exec(ts.Setup)
				require.NoError(t, err)
			}

			logF("Testing file %q with %d suites\n", absPath, len(ts.Suites))

			if len(ts.Suites) > 0 {
				for _, suite := range ts.Suites {
					t.Run(suite.Name, func(t *testing.T) {
						var tests []*test

						logLn("- Testing suite:", suite.Name)

						for _, tt := range suite.Tests {
							if tt.Only {
								tests = []*test{tt}
								break
							}
						}

						if tests == nil {
							tests = suite.Tests
						}

						logLn("- Running", len(tests), "tests")

						for _, test := range tests {
							t.Run(test.Name, func(t *testing.T) {
								db, err := sql.Open("chai", ":memory:")
								require.NoError(t, err)
								defer db.Close()

								setup(t, db)

								logLn("-- Running test:", test.Name)

								// post setup
								if suite.PostSetup != "" {
									_, err = db.Exec(suite.PostSetup)
									require.NoError(t, err)
								}

								if test.Fails {
									exec := func() error {
										_, err := db.Exec(test.Expr)
										return err
									}

									err := exec()
									if test.ErrorMatch != "" {
										require.NotNilf(t, err, "%s:%d expected error, got nil", absPath, test.Line)
										require.Equal(t, test.ErrorMatch, err.Error(), "Source %s:%d", absPath, test.Line)
									} else {
										require.Errorf(t, err, "\nSource:%s:%d expected\n%s\nto raise an error but got none", absPath, test.Line, test.Expr)
									}
								} else {
									rows, err := db.Query(test.Expr)
									require.NoError(t, err, "Source: %s:%d", absPath, test.Line)
									defer rows.Close()

									RequireRowsEqf(t, test.Result, rows, "Source: %s:%d", absPath, test.Line)
								}
							})
						}
					})
				}
			}
		})

		return nil
	})

	require.NoError(t, err)
}

type test struct {
	Name       string
	Expr       string
	Result     string
	ErrorMatch string
	Fails      bool
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

func parse(r io.Reader, filename string) (*testSuite, error) {
	s := bufio.NewScanner(r)
	ts := testSuite{
		Filename: filename,
	}

	var curTest *test

	absPath, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}

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

			// add test to each suite
			for i := range ts.Suites {
				ts.Suites[i].Tests = append(ts.Suites[i].Tests, curTest)
			}
		case strings.HasPrefix(line, "/* result:"), strings.HasPrefix(line, "/*result:"):
			readingResult = true
		case strings.HasPrefix(line, "-- error:"):
			if curTest == nil {
				return nil, fmt.Errorf("missing test directive. line: %q, file: %v:%d", line, absPath, lineCount)
			}
			errorString := strings.TrimPrefix(line, "-- error:")
			errorString = strings.TrimSpace(errorString)
			if errorString == "" {
				// handle the case where error was used but without a message
				curTest.Fails = true
			} else {
				curTest.ErrorMatch = errorString
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

	return &ts, nil
}

func RequireRowsEqf(t *testing.T, raw string, rows *sql.Rows, msg string, args ...any) {
	errMsg := append([]any{msg}, args...)
	t.Helper()
	r := testutil.ParseResultStream(raw)

	var want []row.Row

	for {
		v, err := r.Next()
		if err != nil {
			if perr, ok := err.(*parser.ParseError); ok {
				if perr.Found == "EOF" {
					break
				}
			} else if perr, ok := errors.Unwrap(err).(*parser.ParseError); ok {
				if perr.Found == "EOF" {
					break
				}
			}
		}
		require.NoError(t, err, errMsg...)

		want = append(want, v)
	}

	var got []row.Row

	cols, err := rows.Columns()
	require.NoError(t, err, errMsg...)

	for rows.Next() {
		vals := make([]any, len(cols))
		for i := range vals {
			vals[i] = new(types.ValueScanner)
		}
		err := rows.Scan(vals...)
		require.NoError(t, err, errMsg...)

		var cb row.ColumnBuffer

		for i := range vals {
			cb.Add(cols[i], vals[i].(*types.ValueScanner).V)
		}

		got = append(got, &cb)
	}

	if err := rows.Err(); err != nil {
		require.NoError(t, err, errMsg...)
	}

	var expected strings.Builder
	for i := range want {
		data, err := row.MarshalTextIndent(want[i], "\n", "  ")
		require.NoError(t, err, errMsg...)
		if i > 0 {
			expected.WriteString("\n")
		}

		expected.WriteString(string(data))
	}

	var actual strings.Builder
	for i := range got {
		data, err := row.MarshalTextIndent(got[i], "\n", "  ")
		require.NoError(t, err, errMsg...)
		if i > 0 {
			actual.WriteString("\n")
		}

		actual.WriteString(string(data))
	}

	if msg != "" {
		require.Equal(t, expected.String(), actual.String(), errMsg...)
	} else {
		require.Equal(t, expected.String(), actual.String())
	}
}

package genexprtests

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type statement struct {
	Expr     string
	ExprLine int
	Res      string
	ResLine  int
	Fail     bool
}

type test struct {
	Name       string
	Statements []*statement
}

type testSuite struct {
	Tests []*test
}

func Parse(r io.Reader) (*testSuite, error) {
	s := bufio.NewScanner(r)
	ts := testSuite{}

	var curTest *test
	var curStmt *statement
	lineNum := 0
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		lineNum++
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "-- test:"):
			name := strings.TrimPrefix(line, "-- test: ")
			curTest = &test{
				Name: name,
			}
			ts.Tests = append(ts.Tests, curTest)
		case strings.HasPrefix(line, "--"): // ignore normal comments
			continue
		case line[0] == '>':
			text := strings.TrimPrefix(line, "> ")
			curStmt = &statement{
				Expr:     text,
				ExprLine: lineNum,
			}
			curTest.Statements = append(curTest.Statements, curStmt)
		case line[0] == '!':
			text := strings.TrimPrefix(line, "! ")
			curStmt = &statement{
				Expr:     text,
				ExprLine: lineNum,
				Fail:     true,
			}
			curTest.Statements = append(curTest.Statements, curStmt)
		default:
			if curStmt.Fail {
				if line[0] != '\'' || line[len(line)-1] != '\'' {
					return nil, fmt.Errorf("error statement must be surrounded by ' in `%s`", line)
				}

				curStmt.Res = line[1 : len(line)-1]
			} else {
				curStmt.Res = line
			}
			curStmt.ResLine = lineNum
		}
	}

	return &ts, nil
}

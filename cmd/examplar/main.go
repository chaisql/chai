package main

import (
	"bufio"
	"io"
	"strings"
	"text/template"
)

const comment = "---"

type State int

const (
	ILLEGAL State = iota
	SETUP
	TEARDOWN
	TEST
	ASSERT_EQ
)

type example struct {
	name       string
	statements []*Statement
}

type examplar struct {
	name     string
	setup    []string
	teardown []string
	examples []*example
}

type Statement struct {
	Code        string
	EqAssertion string
}

func parse(r io.Reader, name string) (*examplar, error) {
	ex := examplar{name: name}
	var state State

	s := bufio.NewScanner(r)
	s.Split(bufio.ScanLines)

	for s.Scan() {
		line := s.Text()
		// fmt.Println("state ", state, "| ", line)

		if line == "" {
			continue
		}

		if strings.HasPrefix(line, comment) {
			c := strings.TrimPrefix(line, comment)
			c = strings.TrimLeft(c, " ")

			switch {
			case strings.HasPrefix(c, "setup:"):
				state = SETUP
			case strings.HasPrefix(c, "teardown:"):
				state = TEARDOWN
			case strings.HasPrefix(c, "test:"):
				if state != TEST {
					name := strings.TrimPrefix(c, "test:")
					name = strings.TrimSpace(name)
					ex.examples = append(ex.examples, &example{name: name})
				}
				state = TEST
			case strings.HasPrefix(c, "`"):
				state = ASSERT_EQ
				expected := strings.TrimPrefix(c, "`")
				expected = strings.TrimSuffix(expected, "`")

				t := ex.examples[len(ex.examples)-1]
				stmt := t.statements[len(t.statements)-1]
				stmt.EqAssertion = expected
			default:
				state = ILLEGAL
			}
		} else {
			switch state {
			case SETUP:
				ex.setup = append(ex.setup, line)
			case TEARDOWN:
				ex.teardown = append(ex.teardown, line)
			case TEST:
				t := ex.examples[len(ex.examples)-1]
				t.statements = append(t.statements, &Statement{
					Code: line,
				})
			case ASSERT_EQ:
			case ILLEGAL:
				panic(line)
			}
		}
	}

	if err := s.Err(); err != nil {
		return nil, err
	}

	return &ex, nil
}

func normalizeTestName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Title(name)
	return strings.ReplaceAll(name, " ", "")
}

func generate(ex *examplar, w io.Writer) error {
	tmpl := template.Must(template.ParseFiles("test_template.go.tmpl"))

	bindings := struct {
		Package    string
		TestName   string
		Setup      string
		Teardown   string
		Statements []*Statement
	}{
		"integration_test",
		normalizeTestName(ex.examples[0].name),
		strings.Join(ex.setup, "\n"),
		strings.Join(ex.teardown, "\n"),
		ex.examples[0].statements,
	}

	return tmpl.Execute(w, bindings)
}

func main() {
}

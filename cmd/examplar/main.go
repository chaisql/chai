package main

import (
	"bufio"
	"io"
	"regexp"
	"strings"
	"text/template"
)

const commentPrefix = "---"

type Tag int

const (
	UNKNOWN Tag = iota
	SETUP
	TEARDOWN
	TEST
)

// Test is a list of statements.
type Test struct {
	name       string
	statements []*Statement
}

// Statement is a pair composed of a line of code and an expectation on its result when evaluated.
type Statement struct {
	Code        string
	Expectation string
}

// Examplar represents a group of tests and can optionally include setup code and teardown code.
type Examplar struct {
	name     string
	setup    []string
	teardown []string
	examples []*Test
}

// HasSetup returns true if setup code is provided.
func (ex *Examplar) HasSetup() bool {
	return len(ex.setup) > 0
}

// HasSetup returns true if teardown code is provided.
func (ex *Examplar) HasTeardown() bool {
	return len(ex.teardown) > 0
}

func (ex *Examplar) appendTest(name string) {
	ex.examples = append(ex.examples, &Test{
		name: name,
	})
}

func (ex *Examplar) currentTest() *Test {
	return ex.examples[len(ex.examples)-1]
}

type stateFn func(*Scanner) stateFn

type Scanner struct {
	line string
	ex   *Examplar
}

func initialState(s *Scanner) stateFn {
	if tag, data := parseTag(s.line); tag != UNKNOWN {
		switch tag {
		case SETUP:
			return setupState
		case TEARDOWN:
			return teardownState
		case TEST:
			s.ex.appendTest(data)
			return testState
		}
	}

	return initialState
}

func setupState(s *Scanner) stateFn {
	if tag, data := parseTag(s.line); tag != UNKNOWN {
		switch tag {
		case SETUP:
			return errorState
		case TEARDOWN:
			if s.ex.HasTeardown() {
				return errorState
			} else {
				return teardownState
			}
		case TEST:
			s.ex.appendTest(data)
			return testState
		}
	}

	s.ex.setup = append(s.ex.setup, s.line)
	return setupState
}

func teardownState(s *Scanner) stateFn {
	if tag, data := parseTag(s.line); tag != UNKNOWN {
		switch tag {
		case SETUP:
			if s.ex.HasSetup() {
				return errorState
			} else {
				return setupState
			}
		case TEARDOWN:
			return errorState
		case TEST:
			s.ex.appendTest(data)
			return testState
		}
	}

	s.ex.teardown = append(s.ex.teardown, s.line)
	return teardownState
}

func testState(s *Scanner) stateFn {
	if tag, data := parseTag(s.line); tag != UNKNOWN {
		switch tag {
		case SETUP:
			return errorState
		case TEARDOWN:
			return errorState
		case TEST:
			s.ex.appendTest(data)
			return testState
		}
	}

	test := s.ex.currentTest()

	if hasMultilineAssertionTag(s.line) {
		return multilineAssertionState
	}

	if assertion := parseSingleAssertion(s.line); len(assertion) > 0 {
		stmt := test.statements[len(test.statements)-1]
		stmt.Expectation = assertion
		return testState
	}

	test.statements = append(test.statements, &Statement{
		Code: s.line,
	})

	return testState
}

func multilineAssertionState(s *Scanner) stateFn {
	re := regexp.MustCompile(`^\s*` + commentPrefix + `\s*(.*)`)
	matches := re.FindStringSubmatch(s.line)

	if matches == nil {
		return multilineAssertionState
	}

	code := strings.TrimRight(matches[1], " \t")

	if code == "```" {
		return testState
	}

	test := s.ex.currentTest()
	test.statements[len(test.statements)-1].Expectation += code + "\n"
	return multilineAssertionState
}

func errorState(s *Scanner) stateFn {
	panic(s.line)
}

func (s *Scanner) Run(io *bufio.Scanner) *Examplar {
	s.ex = &Examplar{}

	for state := initialState; io.Scan(); {
		s.line = io.Text()
		s.line = strings.TrimSpace(s.line)
		if s.line == "" {
			continue
		}
		state = state(s)
	}

	return s.ex
}

func parseTag(line string) (Tag, string) {
	re := regexp.MustCompile(`^\s*` + commentPrefix + `\s*(\w+):\s*(.*)`)
	matches := re.FindStringSubmatch(line)
	if matches == nil {
		return UNKNOWN, ""
	}

	var tag Tag
	switch strings.ToLower(matches[1]) {
	case "setup":
		tag = SETUP
	case "teardown":
		tag = TEARDOWN
	case "test":
		tag = TEST
	default:
		return UNKNOWN, ""
	}

	return tag, matches[2]
}

func parseSingleAssertion(line string) string {
	re := regexp.MustCompile(`^\s*` + commentPrefix + `\s*` + "`" + `([^` + "`" + `]+)`)
	matches := re.FindStringSubmatch(line)
	if matches == nil {
		return ""
	}
	return matches[1]
}

func hasMultilineAssertionTag(line string) bool {
	re := regexp.MustCompile(`^\s*` + commentPrefix + `\s*` + "```" + `(\w*)`)
	matches := re.FindStringSubmatch(line)
	return matches != nil
}

func Parse(r io.Reader, name string) (*Examplar, error) {
	scanner := &Scanner{}

	ex := scanner.Run(bufio.NewScanner(r))
	ex.name = name

	return ex, nil
}

func normalizeTestName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Title(name)
	return strings.ReplaceAll(name, " ", "")
}

func Generate(ex *Examplar, w io.Writer) error {
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

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

type Line struct {
	Num  int
	Text string
}

// Test is a list of statements.
type Test struct {
	Name       string
	Num        int
	Statements []*Statement
}

// Statement is a pair composed of a line of code and an expectation on its result when evaluated.
type Statement struct {
	Code        Line
	Expectation []Line
}

func (s Statement) expectationText() string {
	var text string
	for _, e := range s.Expectation {
		text += e.Text + "\n"
	}

	return text
}

// Examplar represents a group of tests and can optionally include setup code and teardown code.
type Examplar struct {
	Name     string
	setup    []Line
	teardown []Line
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

func (ex *Examplar) appendTest(name string, num int) {
	ex.examples = append(ex.examples, &Test{
		Name: name,
		Num:  num,
	})
}

func (ex *Examplar) currentTest() *Test {
	return ex.examples[len(ex.examples)-1]
}

func (ex *Examplar) currentStatement() *Statement {
	test := ex.currentTest()
	return test.Statements[len(test.Statements)-1]
}

func (ex *Examplar) currentExpectation() *[]Line {
	return &ex.currentStatement().Expectation
}

type stateFn func(*Scanner) stateFn

type Scanner struct {
	line string
	num  int
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
			s.ex.appendTest(data, s.num)
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
			s.ex.appendTest(data, s.num)
			return testState
		}
	}

	s.ex.setup = append(s.ex.setup, Line{s.num, s.line})
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
			s.ex.appendTest(data, s.num)
			return testState
		}
	}

	s.ex.teardown = append(s.ex.teardown, Line{s.num, s.line})
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
			s.ex.appendTest(data, s.num)
			return testState
		}
	}

	test := s.ex.currentTest()

	if hasMultilineAssertionTag(s.line) {
		return multilineAssertionState
	}

	if assertion := parseSingleAssertion(s.line); len(assertion) > 0 {
		exp := s.ex.currentExpectation()
		*exp = []Line{{s.num, assertion}}
		return testState
	}

	test.Statements = append(test.Statements, &Statement{
		Code: Line{s.num, s.line},
	})

	return testState
}

// TODO check that all lines are sharing the same amount of space, if yes
// trim them, so everything is aligned perfectly in the resulting test file.
func multilineAssertionState(s *Scanner) stateFn {
	re := regexp.MustCompile(`^\s*` + commentPrefix + `(.*)`)
	matches := re.FindStringSubmatch(s.line)

	if matches == nil {
		return multilineAssertionState
	}

	code := strings.TrimRight(matches[1], " \t")

	if strings.TrimSpace(matches[1]) == "```" {
		return testState
	}

	exp := s.ex.currentExpectation()
	*exp = append(*exp, Line{s.num, code})

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
		s.num++

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
	ex.Name = name

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
		Package  string
		TestName string
		Setup    []Line
		Teardown []Line
		Tests    []*Test
	}{
		"main",
		normalizeTestName("Foo Bar"),
		ex.setup,
		ex.teardown,
		ex.examples,
	}

	return tmpl.Execute(w, bindings)
}

func main() {
}

package main

import (
	"bufio"
	"regexp"
	"strings"
)

const commentPrefix = "---"

type Tag int

const (
	UNKNOWN Tag = iota
	SETUP
	TEARDOWN
	TEST
)

type stateFn func(*Scanner) stateFn

type Scanner struct {
	line string
	num  int
	ex   *Examplar

	curTest *Test
	curStmt *Statement
}

func initialState(s *Scanner) stateFn {
	if tag, data := parseTag(s.line); tag != UNKNOWN {
		switch tag {
		case SETUP:
			return setupState
		case TEARDOWN:
			return teardownState
		case TEST:
			s.curTest = s.ex.appendTest(data, s.num)
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
			s.curTest = s.ex.appendTest(data, s.num)
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
			s.curTest = s.ex.appendTest(data, s.num)
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
			s.curTest = s.ex.appendTest(data, s.num)
			return testState
		}
	}

	if s.curStmt == nil {
		stmt := &Statement{}
		s.curTest.Statements = append(s.curTest.Statements, stmt)
		s.curStmt = stmt
	}

	if hasMultilineAssertionTag(s.line) {
		return multilineAssertionState
	}

	if assertion := parseSingleAssertion(s.line); len(assertion) > 0 {
		exp := &s.curStmt.Expectation
		*exp = []Line{{s.num, assertion}}

		// current statement is now finished
		s.curStmt = nil
		return testState
	}

	s.curStmt.Code = append(s.curStmt.Code, Line{s.num, s.line})

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

	// final triple backtick, go back to testState
	if strings.TrimSpace(matches[1]) == "```" {
		s.curStmt = nil
		return testState
	}

	exp := &s.curStmt.Expectation
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

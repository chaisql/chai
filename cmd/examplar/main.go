package main

import (
	"bufio"
	"io"
	"strings"
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
	statements []string
	assertions []string
}

type examplar struct {
	setup    []string
	teardown []string
	examples []*example
}

func parse(r io.Reader) (*examplar, error) {
	var ex examplar
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
				t.assertions = append(t.assertions, expected)

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
				t.statements = append(t.statements, line)
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

func main() {
}

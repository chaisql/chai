package main

import (
	"bufio"
	"io"
	"strings"
	"text/template"
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
	Code        []Line
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

func (ex *Examplar) appendTest(name string, num int) *Test {
	test := Test{
		Name: name,
		Num:  num,
	}
	ex.examples = append(ex.examples, &test)

	return &test
}

// func (ex *Examplar) currentTest() *Test {
// 	return ex.examples[len(ex.examples)-1]
// }

// func (ex *Examplar) currentStatement() *Statement {
// 	test := ex.currentTest()
// 	return test.Statements[len(test.Statements)-1]
// }

// func (ex *Examplar) currentExpectation() *[]Line {
// 	return &ex.currentStatement().Expectation
// }

// Parse reads annotated textual data and transforms it into a
// structured representation. Only annotations are parsed, the
// textual data itself is irrelevant to this function.
//
// It will panic if an error is encountered.
func Parse(r io.Reader, name string) *Examplar {
	scanner := &Scanner{}

	ex := scanner.Run(bufio.NewScanner(r))
	ex.Name = name

	return ex
}

func normalizeTestName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Title(name)
	return strings.ReplaceAll(name, " ", "")
}

// Generate takes a structured representation of the original textual data in order
// to write a valid go test file.
func Generate(ex *Examplar, packageName string, w io.Writer) error {
	tmpl := template.Must(template.ParseFS(tmplFS, "test_template.go.tmpl"))

	bindings := struct {
		Package  string
		TestName string
		Setup    []Line
		Teardown []Line
		Tests    []*Test
	}{
		packageName,
		normalizeTestName(ex.Name),
		ex.setup,
		ex.teardown,
		ex.examples,
	}

	return tmpl.Execute(w, bindings)
}

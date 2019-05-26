package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/asdine/genji/generator"
	"github.com/pkg/errors"
)

type stringFlags []string

func (i *stringFlags) String() string {
	return "list of strings"
}

func (i *stringFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	var files, schemaless, schemaful, results stringFlags
	var output string

	flag.Var(&files, "f", "path of the files to parse")
	flag.Var(&schemaless, "s", "name of the source structure, will generate a schemaless table")
	flag.Var(&schemaful, "S", "name of the source structure, will generate a schemaful table")
	flag.Var(&results, "res", "name of the result structure, optional")
	flag.StringVar(&output, "o", "", "name of the generated file, optional")

	flag.Parse()

	if len(files) == 0 || (len(schemaless) == 0 && len(schemaful) == 0) {
		exitRecordUsage()
	}

	err := generate(files, schemaless, schemaful, results, output)
	if err != nil {
		fail("%v\n", err)
	}
}

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(2)
}

func exitRecordUsage() {
	flag.Usage()
	os.Exit(2)
}

func generate(files []string, schemaless, schemaful []string, results []string, output string) error {
	if !areGoFiles(files) {
		return errors.New("input files must be Go files")
	}

	sources := make([]io.Reader, len(files))

	for i, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			return errors.Wrap(err, "failed to read file")
		}

		sources[i] = f
	}

	structs := make([]generator.Struct, len(schemaful)+len(schemaless))
	for i, name := range schemaless {
		structs[i].Name = name
	}
	for i, name := range schemaful {
		structs[i].Name = name
		structs[i].Schema = true
	}

	var buf bytes.Buffer
	err := generator.Generate(&buf, generator.Config{
		Sources: sources,
		Structs: structs,
		Results: results,
	})
	if err != nil {
		return err
	}

	if output == "" {
		suffix := filepath.Ext(files[0])
		base := strings.TrimSuffix(files[0], suffix)
		if strings.HasSuffix(base, "_test") {
			base = strings.TrimSuffix(base, "_test")
			suffix = "_test" + suffix
		}
		output = base + ".genji" + suffix
	}

	err = ioutil.WriteFile(output, buf.Bytes(), 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to generate file at location %s", output)
	}

	return nil
}

func areGoFiles(files []string) bool {
	for _, f := range files {
		if filepath.Ext(f) != ".go" {
			return false
		}
	}

	return true
}

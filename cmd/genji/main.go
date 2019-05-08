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
	var files, records, results stringFlags
	var output string

	flag.Var(&files, "f", "path of the files to parse")
	flag.Var(&records, "rec", "name of the record structure")
	flag.Var(&results, "res", "name of the result structure, optional")
	flag.StringVar(&output, "o", "", "name of the generated file, optional")

	flag.Parse()

	if len(files) == 0 || len(records) == 0 {
		exitRecordUsage()
	}

	err := generate(files, records, results, output)
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

func generate(files []string, records []string, results []string, output string) error {
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

	var buf bytes.Buffer
	err := generator.Generate(&buf, generator.Config{
		Sources: sources,
		Records: records,
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

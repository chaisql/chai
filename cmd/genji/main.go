package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/asdine/genji/generator"
	"github.com/pkg/errors"
)

func main() {
	f := flag.String("f", "", "path of the file to parse")
	t := flag.String("t", "", "comma separated list of targeted struct names")

	flag.CommandLine.Usage = exitRecordUsage
	flag.CommandLine.Parse(os.Args[1:])

	if *f == "" || *t == "" {
		exitRecordUsage()
	}

	err := generate(*f, *t)
	if err != nil {
		fail("%v\n", err)
	}
}

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(2)
}

func exitRecordUsage() {
	fmt.Fprintf(os.Stderr, "Usage: genji [options]\n\nOptions:\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func generate(f, t string) error {
	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, f, nil, 0)
	if err != nil {
		return errors.Wrap(err, "failed to parse file")
	}

	var buf bytes.Buffer
	err = generator.GenerateRecords(&buf, af, strings.Split(t, ",")...)
	if err != nil {
		return err
	}

	suffix := filepath.Ext(f)
	base := strings.TrimSuffix(f, suffix)
	if strings.HasSuffix(base, "_test") {
		base = strings.TrimSuffix(base, "_test")
		suffix = "_test" + suffix
	}
	genPath := base + ".genji" + suffix

	err = ioutil.WriteFile(genPath, buf.Bytes(), 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to generate file at location %s", genPath)
	}

	return nil
}

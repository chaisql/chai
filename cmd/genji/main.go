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
)

func main() {
	f := flag.String("f", "", "path of the file to parse")
	t := flag.String("t", "", "name of the targeted type")

	flag.Parse()

	if *f == "" || *t == "" {
		flag.Usage()
		os.Exit(2)
	}

	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, *f, nil, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open file: %v\n", err)
		os.Exit(2)
	}

	var buf bytes.Buffer
	err = generator.Generate(af, *t, &buf)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	suffix := filepath.Ext(*f)
	base := strings.TrimSuffix(*f, suffix)
	if strings.HasSuffix(base, "_test") {
		base = strings.TrimSuffix(base, "_test")
		suffix = "_test" + suffix
	}
	genPath := base + ".genji" + suffix

	err = ioutil.WriteFile(genPath, buf.Bytes(), 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate file at location %s: %v\n", genPath, err)
		os.Exit(2)
	}
}

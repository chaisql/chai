package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/asdine/genji/generator"
)

const usage = `Usage:
	genji <command> [arguments]

The commands are:

	record		generate a record from a struct
`

func main() {
	if len(os.Args) < 2 {
		exitUsage()
	}

	switch os.Args[1] {
	case "record":
		recordCmd()
	default:
		exitUsage()
	}
}

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(2)
}

func exitUsage() {
	fail(usage)
}

func exitRecordUsage() {
	fmt.Fprintf(os.Stderr, "Usage: genji record [options]\n\nOptions:\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func recordCmd() {
	f := flag.String("f", "", "path of the file to parse")
	t := flag.String("t", "", "comma separated list of targeted struct names")

	if len(os.Args) < 3 {
		exitRecordUsage()
	}

	flag.CommandLine.Usage = exitRecordUsage
	flag.CommandLine.Parse(os.Args[2:])

	if *f == "" || *t == "" {
		exitRecordUsage()
	}

	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, *f, nil, 0)
	if err != nil {
		fail("failed to open file: %v\n", err)
	}

	var buf bytes.Buffer
	err = generator.GenerateRecords(&buf, af, strings.Split(*t, ",")...)
	if err != nil {
		fail(err.Error() + "\n")
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
		fail("failed to generate file at location %s: %v\n", genPath, err)
	}

	err = exec.Command("gofmt", "-w", genPath).Run()
	if err != nil {
		fail("gofmt failed with the following error: %s\n", err)
	}
}

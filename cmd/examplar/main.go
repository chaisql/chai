package main

import (
	"embed"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

//go:embed "test_template.go.tmpl"
var tmplFS embed.FS

var packageName string

func init() {
	flag.StringVar(&packageName, "package", "", "package name for the generated files")
	flag.Usage = func() {
		fmt.Println("Usage: ./examplar -package=[NAME] input1 input2 ...")
	}
}

func main() {
	flag.Parse()

	if packageName == "" {
		flag.Usage()
		os.Exit(-1)
	}

	paths := os.Args[2:]
	if len(paths) < 1 {
		flag.Usage()
		os.Exit(-1)
	}

	for _, p := range paths {
		// use globs because when invoked from go generate, there will be no shell
		// to expand it for us
		gpaths, err := filepath.Glob(p)
		if err != nil {
			log.Fatal(err)
		}

		if len(gpaths) < 1 {
			log.Fatalf("%s does not exist", p)
		}

		for _, gp := range gpaths {
			err := genFile(gp, packageName)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func genFile(p string, packageName string) error {
	in, err := os.Open(p)
	if err != nil {
		return err
	}
	defer in.Close()

	base := path.Base(p)
	name := strings.TrimSuffix(base, path.Ext(p))

	ex := Parse(in, name, base)

	out, err := os.OpenFile(name+"_test.go", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer out.Close()

	err = Generate(ex, packageName, out)
	if err != nil {
		return err
	}

	return nil
}

package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/asdine/genji/cmd/genji/generator"
	"github.com/pkg/errors"
)

func generate(files []string, structs []string, output string) error {
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

	list := make([]generator.Struct, len(structs))
	for i, name := range structs {
		list[i].Name = name
	}

	var buf bytes.Buffer
	err := generator.Generate(&buf, generator.Config{
		Sources: sources,
		Structs: list,
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

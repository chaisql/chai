package fuzz

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestFuzzParseQuery(t *testing.T) { testFuzz(t, FuzzParseQuery) }

func TestFuncName(t *testing.T) {
	got := funcName(funcName)
	expected := "funcName"
	if got != expected {
		t.Errorf("funcName(%s): expected %q, got %q", expected, expected, got)
	}
}

// funcName returns the name of a function f.
// It is used to infer the workdir of a fuzz function.
func funcName(f interface{}) string {
	v := reflect.ValueOf(f)
	pc := v.Pointer()
	fn := runtime.FuncForPC(pc)
	name := fn.Name()
	return name[strings.LastIndex(name, ".")+1:]
}

// testFuzz runs fuzz function once for each input in corpus.
// It assumes the directory structure from github.com/thepudds/fzgo.
// That is, testdata/fuzz/<FuzzName>/corpus stores the corpus.
func testFuzz(t *testing.T, fuzz func([]byte) int) {
	t.Helper()

	name := funcName(fuzz)

	workdir := filepath.Join("testdata", "fuzz", name)
	corpus := filepath.Join(workdir, "corpus")
	crashers := filepath.Join(workdir, "crashers")

	walkFn := func(path string, info os.FileInfo, err error) error {
		t.Helper()

		if err != nil {
			// Do nothing if the root directory does not exist.
			// Note that we expect testdata to be immutable
			// while running tests (but not fuzzing).
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		// Don’t descend into subdirectories.
		if info.IsDir() && path != corpus && path != crashers {
			return filepath.SkipDir
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		// Skip debug info in crashers directory.
		if path == crashers {
			if strings.HasSuffix(path, ".output") || strings.HasSuffix(path, ".quoted") {
				return nil
			}
		}

		t.Run(info.Name(), func(t *testing.T) {
			t.Helper()

			data, err := ioutil.ReadFile(path)
			if err != nil {
				t.Errorf("read test data: %v", err)
				return
			}

			fuzz(data)
		})

		return nil
	}

	roots := []string{corpus, crashers}
	for _, root := range roots {
		err := filepath.Walk(root, walkFn)
		if err != nil {
			t.Errorf("walk %q: %v", root, err)
		}
	}
}

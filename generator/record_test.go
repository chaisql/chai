//go:generate go run ../cmd/genji/main.go record -f record_test.go -t RecordTest

package generator

import (
	"bytes"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/generator/testdata"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

var update = flag.String("update", "", "update .golden files by name")

func TestGenerateRecord(t *testing.T) {
	t.Run("Golden", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"basic"},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fset := token.NewFileSet()
				f, err := parser.ParseFile(fset, "testdata/"+test.name+".go", nil, 0)
				require.NoError(t, err)

				var buf bytes.Buffer
				err = GenerateRecord(f, strings.Title(test.name), &buf)
				require.NoError(t, err)

				gp := "testdata/" + test.name + ".generated.golden.go"
				if *update == "basic" {
					t.Logf("%s: golden file updated", gp)
					require.NoError(t, ioutil.WriteFile(gp, buf.Bytes(), 0644))
				}

				g, err := ioutil.ReadFile(gp)
				require.NoError(t, err)

				require.Equal(t, string(g), buf.String())
			})
		}
	})

	t.Run("Unsupported fields", func(t *testing.T) {
		tests := []struct {
			Label     string
			FieldLine string
		}{
			{"Int", "F int"},
			{"Slice", "F []string"},
			{"Embedded", "F"},
		}

		for _, test := range tests {
			t.Run(test.Label, func(t *testing.T) {
				src := `
					package user
				
					type User struct {
						Name string
						Age int64
						` + test.FieldLine + `
					}
				`

				fset := token.NewFileSet()
				f, err := parser.ParseFile(fset, "", src, 0)
				require.NoError(t, err)

				var buf bytes.Buffer
				err = GenerateRecord(f, "User", &buf)
				require.Error(t, err)
			})
		}
	})

	t.Run("Not found", func(t *testing.T) {
		src := `
			package user
		`

		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, 0)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = GenerateRecord(f, "User", &buf)
		require.Error(t, err)
	})

	// this test ensures the generator only generates code for
	// top level types.
	t.Run("Top level only", func(t *testing.T) {
		src := `
			package s
		
			func foo() {
				type S struct {
					X,Y,Z string
				}

				var s S
			}
		`

		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, 0)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = GenerateRecord(f, "S", &buf)
		require.Error(t, err)
	})
}

func TestGeneratedRecord(t *testing.T) {
	r := testdata.Basic{
		A: "A", B: 10, C: 11, D: 12,
	}

	require.Implements(t, (*record.Record)(nil), &r)

	tests := []struct {
		name string
		typ  field.Type
		data []byte
	}{
		{"A", field.String, []byte("A")},
		{"B", field.Int64, field.EncodeInt64(r.B)},
		{"C", field.Int64, field.EncodeInt64(r.C)},
		{"D", field.Int64, field.EncodeInt64(r.D)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, err := r.Field(test.name)
			require.NoError(t, err)
			require.Equal(t, test.name, f.Name)
			require.Equal(t, test.typ, f.Type)
			require.Equal(t, test.data, f.Data)
		})
	}

	c := r.Cursor()
	for i := 0; i < 4; i++ {
		t.Run(fmt.Sprintf("Field-%d", i), func(t *testing.T) {
			require.True(t, c.Next())
			f := c.Field()
			require.NotEmpty(t, f)
			require.Equal(t, tests[i].name, f.Name)
			require.Equal(t, tests[i].typ, f.Type)
			require.Equal(t, tests[i].data, f.Data)
		})
	}

	require.False(t, c.Next())
}

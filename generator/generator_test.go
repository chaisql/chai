package generator

//go:generate go test -run=TestGenerate -update

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/generator/testdata"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update .golden file")

func TestGenerate(t *testing.T) {
	t.Run("Golden", func(t *testing.T) {
		structs := []Struct{
			{"Basic"},
			{"basic"},
			{"Pk"},
			{"Indexed"},
			{"MultipleTags"},
		}

		results := []string{
			"Sample",
		}

		f, err := os.Open("testdata/structs.go")
		require.NoError(t, err)

		var buf bytes.Buffer
		err = Generate(&buf, Config{
			Sources: []io.Reader{f},
			Structs: structs,
			Results: results,
		})
		require.NoError(t, err)

		gp := "testdata/structs.generated.golden.go"
		if *update {
			require.NoError(t, ioutil.WriteFile(gp, buf.Bytes(), 0644))
			t.Logf("%s: golden file updated", gp)
		}

		g, err := ioutil.ReadFile(gp)
		require.NoError(t, err)

		require.Equal(t, string(g), buf.String())
	})

	t.Run("Unsupported fields", func(t *testing.T) {
		tests := []struct {
			Label     string
			FieldLine string
		}{
			{"Slice", "F []string"},
			{"Maps", "F map[int]string"},
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

				var buf bytes.Buffer
				err := Generate(&buf, Config{
					Sources: []io.Reader{strings.NewReader(src)},
					Structs: []Struct{{"User"}},
				})
				require.Error(t, err)
			})
		}
	})

	t.Run("Not found", func(t *testing.T) {
		src := `
			package user
		`

		var buf bytes.Buffer
		err := Generate(&buf, Config{
			Sources: []io.Reader{strings.NewReader(src)},
			Structs: []Struct{{"User"}},
		})
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
		var buf bytes.Buffer
		err := Generate(&buf, Config{
			Sources: []io.Reader{strings.NewReader(src)},
			Structs: []Struct{{"S"}},
		})
		require.Error(t, err)
	})
}

func TestGeneratedRecords(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		r := testdata.Basic{
			A: "A", B: 10, C: 11, D: 12,
		}

		require.Implements(t, (*record.Record)(nil), &r)

		tests := []struct {
			name string
			typ  field.Type
			data []byte
		}{
			{"A", field.String, field.EncodeString(r.A)},
			{"B", field.Int, field.EncodeInt(r.B)},
			{"C", field.Int32, field.EncodeInt32(r.C)},
			{"D", field.Int32, field.EncodeInt32(r.D)},
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

		var i int
		err := r.Iterate(func(f field.Field) error {
			t.Run(fmt.Sprintf("Field-%d", i), func(t *testing.T) {
				require.NotEmpty(t, f)
				require.Equal(t, tests[i].name, f.Name)
				require.Equal(t, tests[i].typ, f.Type)
				require.Equal(t, tests[i].data, f.Data)
			})
			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 4, i)
	})

	t.Run("Pk", func(t *testing.T) {
		r := testdata.Pk{
			A: "A", B: 10,
		}

		require.Implements(t, (*record.Record)(nil), &r)
		require.Implements(t, (*table.PrimaryKeyer)(nil), &r)

		pk, err := r.PrimaryKey()
		require.NoError(t, err)
		require.Equal(t, field.EncodeInt64(10), pk)
	})

	t.Run("Result", func(t *testing.T) {
		r := testdata.Sample{
			A: "A", B: 10,
		}

		require.Implements(t, (*record.Scanner)(nil), &r)

		var res testdata.SampleResult
		var list []record.Record
		for i := 0; i < 5; i++ {
			list = append(list, record.FieldBuffer([]field.Field{
				field.NewString("A", strconv.Itoa(i+1)),
				field.NewInt64("B", int64(i+1)),
			}))
		}

		err := res.ScanTable(table.NewStreamFromRecords(list...))
		require.NoError(t, err)
		require.Len(t, res, 5)
	})
}

package generator

import (
	"bytes"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io/ioutil"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/generator/testdata"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update .golden file")

func TestGenerateRecord(t *testing.T) {
	t.Run("Golden", func(t *testing.T) {
		targets := []string{
			"Basic",
			"unexportedBasic",
			"Pk",
		}

		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "testdata/structs.go", nil, 0)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = GenerateRecords(&buf, f, targets...)
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
				err = GenerateRecords(&buf, f, "User")
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
		err = GenerateRecords(&buf, f, "User")
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
		err = GenerateRecords(&buf, f, "S")
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

		t.Run("Init", func(t *testing.T) {
			ng := memory.NewEngine()
			db := genji.New(ng)

			err := db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicTable(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				// verify table was created
				tab, err := tx.Table("Basic")
				require.NoError(t, err)
				require.NotNil(t, tab)

				// calling Init again should not fail
				return tb.Init()
			})
			require.NoError(t, err)
		})

		t.Run("Insert", func(t *testing.T) {
			ng := memory.NewEngine()
			db := genji.New(ng)

			err := db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicTable(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				record := testdata.Basic{
					A: "A",
				}
				rowid, err := tb.Insert(&record)
				require.NoError(t, err)
				require.NotNil(t, rowid)
				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("Pk", func(t *testing.T) {
		r := testdata.Pk{
			A: "A", B: 10,
		}

		require.Implements(t, (*record.Record)(nil), &r)
		require.Implements(t, (*table.Pker)(nil), &r)

		pk, err := r.Pk()
		require.NoError(t, err)
		require.Equal(t, field.EncodeInt64(10), pk)

		t.Run("Insert", func(t *testing.T) {
			ng := memory.NewEngine()
			db := genji.New(ng)

			err := db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewPkTable(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				record := testdata.Pk{
					A: "A",
					B: 10,
				}
				err := tb.Insert(&record)
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
		})
	})
}

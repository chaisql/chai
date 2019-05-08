package generator

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

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/generator/testdata"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update .golden file")

func TestGenerate(t *testing.T) {
	t.Run("Golden", func(t *testing.T) {
		records := []string{
			"Basic",
			"basic",
			"Pk",
		}

		results := []string{
			"Sample",
		}

		f, err := os.Open("testdata/structs.go")
		require.NoError(t, err)

		var buf bytes.Buffer
		err = Generate(&buf, Options{
			Sources: []io.Reader{f},
			Records: records,
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

				var buf bytes.Buffer
				err := Generate(&buf, Options{
					Sources: []io.Reader{strings.NewReader(src)},
					Records: []string{"User"},
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
		err := Generate(&buf, Options{
			Sources: []io.Reader{strings.NewReader(src)},
			Records: []string{"User"},
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
		err := Generate(&buf, Options{
			Sources: []io.Reader{strings.NewReader(src)},
			Records: []string{"S"},
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
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicStoreWithTx(tx)
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
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicStoreWithTx(tx)
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

		t.Run("Get", func(t *testing.T) {
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicStoreWithTx(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				record1 := testdata.Basic{
					A: "A",
					B: 1,
					C: 2,
					D: 3,
				}
				rowid, err := tb.Insert(&record1)
				require.NoError(t, err)

				record2, err := tb.Get(rowid)
				require.NoError(t, err)
				require.Equal(t, record1, *record2)
				return nil
			})
			require.NoError(t, err)
		})

		t.Run("Delete", func(t *testing.T) {
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicStoreWithTx(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				record1 := testdata.Basic{
					A: "A",
					B: 1,
					C: 2,
					D: 3,
				}
				rowid, err := tb.Insert(&record1)
				require.NoError(t, err)

				err = tb.Delete(rowid)
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
		})

		t.Run("List", func(t *testing.T) {
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicStoreWithTx(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				for i := int64(0); i < 10; i++ {
					_, err = tb.Insert(&testdata.Basic{
						B: i,
					})
					require.NoError(t, err)
				}

				list, err := tb.List(0, 3)
				require.NoError(t, err)
				require.Len(t, list, 3)
				require.EqualValues(t, 0, list[0].B)
				require.EqualValues(t, 1, list[1].B)
				require.EqualValues(t, 2, list[2].B)

				list, err = tb.List(8, 5)
				require.NoError(t, err)
				require.Len(t, list, 2)
				require.EqualValues(t, 8, list[0].B)
				require.EqualValues(t, 9, list[1].B)

				list, err = tb.List(7, -1)
				require.NoError(t, err)
				require.Len(t, list, 3)
				require.EqualValues(t, 7, list[0].B)
				require.EqualValues(t, 8, list[1].B)
				require.EqualValues(t, 9, list[2].B)
				return nil
			})
			require.NoError(t, err)
		})

		t.Run("Replace", func(t *testing.T) {
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewBasicStoreWithTx(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				record1 := testdata.Basic{
					A: "A",
					B: 1,
					C: 2,
					D: 3,
				}
				rowid, err := tb.Insert(&record1)
				require.NoError(t, err)

				record2 := testdata.Basic{
					A: "AA",
					B: 11,
					C: 22,
					D: 33,
				}

				err = tb.Replace(rowid, &record2)
				require.NoError(t, err)

				rec, err := tb.Get(rowid)
				require.Equal(t, record2, *rec)

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
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewPkStoreWithTx(tx)
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

		t.Run("Get", func(t *testing.T) {
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb := testdata.NewPkStoreWithTx(tx)
				require.NoError(t, err)

				err = tb.Init()
				require.NoError(t, err)

				record1 := testdata.Pk{
					A: "A",
					B: 10,
				}
				err := tb.Insert(&record1)
				require.NoError(t, err)

				record2, err := tb.Get(record1.B)
				require.NoError(t, err)

				require.Equal(t, record1, *record2)
				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("Result", func(t *testing.T) {
		r := testdata.Sample{
			A: "A", B: 10,
		}

		require.Implements(t, (*record.Scanner)(nil), &r)

		var res testdata.SampleResult
		var buf table.RecordBuffer
		for i := 0; i < 5; i++ {
			_, err := buf.Insert(record.FieldBuffer([]field.Field{
				field.NewString("A", strconv.Itoa(i+1)),
				field.NewInt64("B", int64(i+1)),
			}))
			require.NoError(t, err)
		}

		err := res.ScanTable(&buf)
		require.NoError(t, err)
		require.Len(t, res, 5)
	})
}

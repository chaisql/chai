package shell

import (
	"strings"
	"testing"

	"github.com/genjidb/genji"
	"github.com/stretchr/testify/require"
)

func TestRunTablesCmd(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{
			"Table",
			".tables",
			false,
		},
		{
			"Table with options",
			".tables test",
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			if err := displayTableIndex(db, test.in); (err != nil) != test.wantErr {
				require.Errorf(t, err, "", test.wantErr)
			}
		})
	}
}

func TestIndexesCmd(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		wantErr bool
	}{
		{
			".Indexes",
			strings.Fields(".indexes"),
			false,
		},
		{
			"Indexes with table name",
			strings.Fields(".indexes test"),
			false,
		},
		{
			"Indexes with nonexistent table name",
			strings.Fields(".indexes foo"),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
					`)
			require.NoError(t, err)
			if err := runIndexesCmd(db, test.in); (err != nil) != test.wantErr {
				require.Errorf(t, err, "", test.wantErr)
			}
		})
	}
}

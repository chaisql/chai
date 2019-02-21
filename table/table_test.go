package table_test

import (
	"testing"

	"github.com/asdine/genji/table"
	"github.com/asdine/genji/table/tabletest"
)

func TestRecordBuffer(t *testing.T) {
	tabletest.TestSuite(t, func() (table.Table, func()) {
		var rb table.RecordBuffer
		return &rb, func() {}
	})
}

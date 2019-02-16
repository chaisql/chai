package memory

import (
	"errors"

	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type tableTx struct {
	*table.RecordBuffer

	tx *transaction
}

func (t *tableTx) Insert(r record.Record) (rowid []byte, err error) {
	if !t.tx.writable {
		return nil, errors.New("can't insert record in read-only transaction")
	}

	rowid, err = t.RecordBuffer.Insert(r)
	if err != nil {
		return nil, err
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.RecordBuffer.Delete(rowid)
	})

	return rowid, nil
}

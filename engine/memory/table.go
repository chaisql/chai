package memory

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type tableTx struct {
	*table.RecordBuffer

	tx *transaction
}

func (t *tableTx) Insert(r record.Record) (rowid []byte, err error) {
	if !t.tx.writable {
		return nil, engine.ErrTransactionReadOnly
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

func (t *tableTx) Delete(rowid []byte) error {
	if !t.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	r, err := t.RecordBuffer.Record(rowid)
	if err != nil {
		return err
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.RecordBuffer.Set(rowid, r)
	})

	return t.RecordBuffer.Delete(rowid)
}

func (t *tableTx) Replace(rowid []byte, r record.Record) error {
	if !t.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	old, err := t.RecordBuffer.Record(rowid)
	if err != nil {
		return err
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.RecordBuffer.Set(rowid, old)
	})

	return t.RecordBuffer.Replace(rowid, r)
}

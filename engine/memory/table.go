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

func (t *tableTx) Insert(r record.Record) (recordID []byte, err error) {
	if !t.tx.writable {
		return nil, engine.ErrTransactionReadOnly
	}

	recordID, err = t.RecordBuffer.Insert(r)
	if err != nil {
		return nil, err
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.RecordBuffer.Delete(recordID)
	})

	return recordID, nil
}

func (t *tableTx) Delete(recordID []byte) error {
	if !t.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	r, err := t.RecordBuffer.Record(recordID)
	if err != nil {
		return err
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.RecordBuffer.Set(recordID, r)
	})

	return t.RecordBuffer.Delete(recordID)
}

func (t *tableTx) Replace(recordID []byte, r record.Record) error {
	if !t.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	old, err := t.RecordBuffer.Record(recordID)
	if err != nil {
		return err
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.RecordBuffer.Set(recordID, old)
	})

	return t.RecordBuffer.Replace(recordID, r)
}

func (t *tableTx) Truncate() error {
	if !t.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	old := t.RecordBuffer
	t.RecordBuffer = new(table.RecordBuffer)

	t.tx.undos = append(t.tx.undos, func() {
		t.RecordBuffer = old
	})

	return nil
}

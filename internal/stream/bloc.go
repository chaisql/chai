package stream

import (
	"encoding/binary"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/kelindar/column"
	"github.com/valyala/bytebufferpool"
)

type BytesBloc struct {
	schema *database.TableInfo

	data   *bytebufferpool.ByteBuffer
	cursor int
	count  int

	// reused objects
	encodedObj database.EncodedObject
	row        database.BasicRow
	buf        []byte
}

func NewBytesBloc(schema *database.TableInfo) *BytesBloc {
	bb := BytesBloc{
		schema: schema,

		data: bytebufferpool.Get(),
	}

	return &bb
}

func (b *BytesBloc) Add(key *tree.Key, record []byte) error {
	if key == nil || len(key.Encoded) == 0 {
		panic("key is empty or not encoded")
	}

	// write key length
	if cap(b.buf) < binary.MaxVarintLen64 {
		b.buf = make([]byte, binary.MaxVarintLen64)
	} else {
		b.buf = b.buf[:binary.MaxVarintLen64]
	}
	n := binary.PutUvarint(b.buf, uint64(len(key.Encoded)))
	_, err := b.data.Write(b.buf[:n])
	if err != nil {
		return err
	}

	// write key
	_, err = b.data.Write(key.Encoded)
	if err != nil {
		return err
	}

	// write record length
	n = binary.PutUvarint(b.buf, uint64(len(record)))
	_, err = b.data.Write(b.buf[:n])
	if err != nil {
		return err
	}

	// write record
	_, err = b.data.Write(record)
	if err != nil {
		return err
	}

	b.count++
	return nil
}

func (b *BytesBloc) Next() database.Row {
	if b.cursor >= b.data.Len() {
		return nil
	}

	data := b.data.Bytes()

	// read key length
	length, offset := binary.Uvarint(data[b.cursor:])
	b.cursor += offset

	// read key
	key := data[b.cursor : b.cursor+int(length)]
	b.cursor += int(length)

	// read record length
	length, offset = binary.Uvarint(data[b.cursor:])
	b.cursor += offset

	// read record
	record := data[b.cursor : b.cursor+int(length)]
	b.cursor += int(length)

	b.encodedObj.ResetWith(&b.schema.FieldConstraints, record)
	b.cursor += int(length)

	b.row.ResetWith(b.schema.TableName, tree.NewEncodedKey(key), &b.encodedObj)

	return &b.row
}

func (b *BytesBloc) Reset() {
	b.cursor = 0
	b.count = 0

	b.data.Reset()
}

func (b *BytesBloc) Len() int {
	return b.count
}

func (b *BytesBloc) Close() error {
	bytebufferpool.Put(b.data)
	return nil
}

type RowBloc struct {
	rows   []database.Row
	cursor int
}

func NewRowBloc() *RowBloc {
	return &RowBloc{}
}

func (b *RowBloc) Add(r database.Row) {
	b.rows = append(b.rows, r)
}

func (b *RowBloc) Next() database.Row {
	if b.cursor >= len(b.rows) {
		return nil
	}

	r := b.rows[b.cursor]
	b.cursor++

	return r
}

func (b *RowBloc) Reset() {
	b.cursor = 0
	b.rows = b.rows[:0]
}

func (b *RowBloc) Len() int {
	return len(b.rows)
}

func (b *RowBloc) Close() error {
	return nil
}

type ColumnBlock struct {
	columns column.Collection
}

func (c *ColumnBlock) CreateColumn(name string, typ types.ValueType) error {
	switch typ {
	case types.BooleanValue:
		return c.columns.CreateColumn(name, column.ForBool())
	case types.IntegerValue:
		return c.columns.CreateColumn(name, column.ForInt())
	case types.DoubleValue:
		return c.columns.CreateColumn(name, column.ForFloat64())
	case types.TextValue:
		return c.columns.CreateColumn(name, column.ForString())
	case types.BlobValue:
		return c.columns.CreateColumn(name, column.ForRecord(func() *blobColumn {
			return new(blobColumn)
		}))
	case types.ArrayValue:
		return c.columns.CreateColumn(name, column.ForRecord(func() *blobColumn {
			return new(blobColumn)
		}))
	case types.ObjectValue:

	}

	c.columns.CreateColumn(name)
}

type blobColumn []byte

func (b blobColumn) MarshalBinary() ([]byte, error) {
	return b, nil
}

func (l *blobColumn) UnmarshalBinary(b []byte) error {
	*l = b
	return nil
}

type arrayColumn types.Array

func (b arrayColumn) MarshalBinary() ([]byte, error) {
	return b, nil
}

func (l *arrayColumn) UnmarshalBinary(b []byte) error {
	*l = b
	return nil
}

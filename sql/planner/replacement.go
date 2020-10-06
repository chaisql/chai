package planner

import (
	"context"
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/sql/query/expr"
)

// replaceBufferSize is the size of the buffer used to replace documents.
const replaceBufferSize = 100

type replacementNode struct {
	node

	tableName string
	table     *database.Table
	codec     encoding.Codec
}

var _ operationNode = (*replacementNode)(nil)

// NewReplacementNode creates a node that stores every document of a stream
// in their respective table and primary keys.
func NewReplacementNode(n Node, tableName string) Node {
	return &replacementNode{
		node: node{
			op:   Replacement,
			left: n,
		},
		tableName: tableName,
	}
}

func (n *replacementNode) Bind(ctx context.Context, tx *database.Transaction, params []expr.Param) (err error) {
	n.table, err = tx.GetTable(ctx, n.tableName)
	return
}

// toResult replaces matching documents by batches of replaceBufferSize documents.
// Some engines can't create more than one iterator per read-write transaction (https://github.com/dgraph-io/badger/issues/1093).
// To deal with these limitations, Run will iterate on a limited number of documents, copy the keys
// to a buffer and replace them after the iteration is complete, and it will do that until there is no document
// left to replace.
// Increasing replaceBufferSize will occasionate less key searches (O(log n) for most engines) but will take more memory.
func (n *replacementNode) toStream(ctx context.Context, st document.Stream) (document.Stream, error) {
	// replace store implementation by a resumable store, temporarily.
	rit := resumableIterator{
		store: n.table.Store,
		codec: n.codec,
	}

	st = st.Limit(replaceBufferSize)

	keys := make([][]byte, replaceBufferSize)
	docs := make([]document.FieldBuffer, replaceBufferSize)

	var err error
	for {
		var i int

		err = st.Iterate(ctx, func(d document.Document) error {
			rk, ok := d.(document.Keyer)
			if !ok || rk == nil {
				return errors.New("attempt to replace document without key")
			}

			docs[i].Reset()
			err := docs[i].Copy(d)
			if err != nil {
				return err
			}

			// copy the key and reuse the buffer
			keys[i] = append(keys[i][0:0], rk.Key()...)
			i++

			return nil
		})

		for j := 0; j < i; j++ {
			err = n.table.Replace(ctx, keys[j], docs[j])
			if err != nil {
				return document.Stream{}, err
			}
		}

		if i < replaceBufferSize {
			break
		}

		rit.curKey = keys[i-1]
	}

	return document.Stream{}, err
}

func (n *replacementNode) String() string {
	return fmt.Sprintf("Replace(%s)", n.tableName)
}

// storeFromKey implements an engine.Store which iterates from a certain key.
// it is used to resume iteration.
type resumableIterator struct {
	store engine.Store
	codec encoding.Codec

	curKey []byte
}

// AscendGreaterOrEqual uses key as pivot if pivot is nil
func (u *resumableIterator) Iterate(ctx context.Context, fn func(d document.Document) error) error {
	var d encodedDocumentWithKey
	var err error

	it := u.store.Iterator(engine.IteratorOptions{})
	defer it.Close()

	var buf []byte
	for it.Seek(ctx, u.curKey); it.Valid(); it.Next(ctx) {
		item := it.Item()

		d.key = item.Key()
		buf, err = item.ValueCopy(buf)
		if err != nil {
			return err
		}

		d.Document = u.codec.NewDocument(buf)
		err = fn(&d)
		if err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}

	return nil
}

type encodedDocumentWithKey struct {
	document.Document

	key []byte
}

func (e encodedDocumentWithKey) Key() []byte {
	return e.key
}

package tree

import (
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

// replaceBufferSize is the size of the buffer used to replace documents.
const replaceBufferSize = 100

type replacementNode struct {
	node

	tableName string
	table     *database.Table
}

var _ outputNode = (*replacementNode)(nil)

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

func (n *replacementNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	n.table, err = tx.GetTable(n.tableName)
	return
}

// toResult replaces matching documents by batches of replaceBufferSize documents.
// Some engines can't create more than one iterator per read-write transaction (https://github.com/dgraph-io/badger/issues/1093).
// To deal with these limitations, Run will iterate on a limited number of documents, copy the keys
// to a buffer and replace them after the iteration is complete, and it will do that until there is no document
// left to replace.
// Increasing replaceBufferSize will occasionate less key searches (O(log n) for most engines) but will take more memory.
func (n *replacementNode) toResult(st document.Stream) (res query.Result, err error) {
	// replace store implementation by a resumable store, temporarily.
	rit := resumableIterator{store: n.table.Store}

	st = st.Limit(replaceBufferSize)

	keys := make([][]byte, replaceBufferSize)
	docs := make([]document.FieldBuffer, replaceBufferSize)

	for {
		var i int

		err = st.Iterate(func(d document.Document) error {
			rk, ok := d.(document.Keyer)
			if !ok || rk == nil {
				return errors.New("attempt to replace document without key")
			}

			docs[i].Reset()
			err := docs[i].ScanDocument(d)
			if err != nil {
				return err
			}

			// copy the key and reuse the buffer
			keys[i] = append(keys[i][0:0], rk.Key()...)
			i++

			return nil
		})

		for j := 0; j < i; j++ {
			err = n.table.Replace(keys[j], docs[j])
			if err != nil {
				return res, err
			}
		}

		if i < replaceBufferSize {
			break
		}

		rit.curKey = keys[i-1]
	}

	return res, err
}

// storeFromKey implements an engine.Store which iterates from a certain key.
// it is used to resume iteration.
type resumableIterator struct {
	store engine.Store

	curKey []byte
}

// AscendGreaterOrEqual uses key as pivot if pivot is nil
func (u *resumableIterator) Iterate(fn func(d document.Document) error) error {
	var d encodedDocumentWithKey
	var err error

	it := u.store.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	for it.Seek(u.curKey); it.Valid(); it.Next() {
		item := it.Item()

		d.key = item.Key()
		d.EncodedDocument, err = item.ValueCopy(d.EncodedDocument)
		if err != nil {
			return err
		}

		err = fn(&d)
		if err != nil {
			return err
		}
	}

	return nil
}

type encodedDocumentWithKey struct {
	encoding.EncodedDocument

	key []byte
}

func (e encodedDocumentWithKey) Key() []byte {
	return e.key
}

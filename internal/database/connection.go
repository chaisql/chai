package database

import (
	"context"

	"github.com/cockroachdb/errors"
)

type Connection struct {
	db  *Database
	ctx context.Context
	tx  *Transaction
}

// BeginTx starts a new transaction with the given options.
// If opts is empty, it will use the default options.
// The returned transaction must be closed either by calling Rollback or Commit.
func (c *Connection) BeginTx(opts *TxOptions) (*Transaction, error) {
	if c.ctx.Err() != nil {
		return nil, errors.New("connection is closed")
	}

	if c.tx != nil {
		return nil, errors.New("cannot open a transaction within a transaction")
	}

	tx, err := c.db.beginTx(opts)
	if err != nil {
		return nil, err
	}

	c.tx = tx
	tx.conn = c
	tx.OnRollbackHooks = append(tx.OnRollbackHooks, c.releaseAttachedTx)
	tx.OnCommitHooks = append(tx.OnCommitHooks, c.releaseAttachedTx)

	return tx, nil
}

func (c *Connection) Reset() error {
	if c.tx != nil {
		return errors.New("cannot reset a connection with an attached transaction")
	}

	return nil
}

func (c *Connection) releaseAttachedTx() {
	if c.tx != nil {
		c.tx = nil
	}
}

// GetAttachedTx returns the transaction attached to the connection, if any.
// The returned transaction is not thread safe.
func (c *Connection) GetTx() *Transaction {
	return c.tx
}

func (c *Connection) Close() error {
	defer c.db.connectionWg.Done()

	if c.tx != nil {
		return c.tx.Rollback()
	}

	return nil
}

package database_test

import (
	"testing"
	"time"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil/assert"
)

// See issue https://github.com/chaisql/chai/issues/298
func TestConcurrentTransactionManagement(t *testing.T) {
	db, err := chai.Open(":memory:")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, db.Close())
	}()

	ch := make(chan struct{})
	done := make(chan struct{})

	go func() {
		// 1. Start transaction T1.
		tx, err := db.Begin(true)
		assert.NoError(t, err)

		// Start transaction T2.
		ch <- struct{}{}
		// Wait in case goroutine gets rescheduled.
		time.Sleep(time.Millisecond)

		// 3. Commit or rollback T1.
		assert.NoError(t, tx.Rollback())

		// Wait for T2 to finish and return.
		<-ch
		done <- struct{}{}
	}()

	go func() {
		<-ch // wait for T1 to start.

		// 2. Attempt to start transaction T2.
		// Waits for T1 to finish.
		tx, err := db.Begin(true)
		assert.NoError(t, err)
		assert.NoError(t, tx.Rollback())

		ch <- struct{}{}
	}()

	r := make(chan bool)
	go func() {
		t := time.NewTimer(time.Second)
		select {
		case <-t.C:
			r <- false
		case <-done:
			r <- true
		}
		if !t.Stop() {
			<-t.C
		}
	}()

	if ok := <-r; !ok {
		t.Fatal("deadlock")
	}
}

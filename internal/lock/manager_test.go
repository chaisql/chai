package lock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func queueLen(q *LockRequest) int {
	var i int
	for q != nil {
		i++
		q = q.Next
	}
	return i
}

func TestLockManagerLock(t *testing.T) {
	t.Run("lock twice", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, S)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = m.Lock(getCtx(t), 1, doc, S)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, m.locks[*doc].Queue.Count)
	})

	t.Run("same object: S", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, S)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = m.Lock(getCtx(t), 2, doc, S)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, queueLen(m.locks[*doc].Queue))
	})

	t.Run("same object: incompatible lock", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, S)
		require.NoError(t, err)
		require.True(t, ok)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ok, err = m.Lock(ctx, 2, doc, X)
		require.Error(t, err)
		require.False(t, ok)
		require.Equal(t, 1, queueLen(m.locks[*doc].Queue))
	})

	t.Run("convert: single lock in queue", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, S)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = m.Lock(getCtx(t), 1, doc, X)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, m.locks[*doc].Queue.Count)
	})

	t.Run("convert: multiple locks in queue, compatible", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, IS)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = m.Lock(getCtx(t), 2, doc, IS)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = m.Lock(getCtx(t), 3, doc, IX)
		require.NoError(t, err)
		require.True(t, ok)

		// convert tx 1 to IX
		ok, err = m.Lock(getCtx(t), 1, doc, IX)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 3, queueLen(m.locks[*doc].Queue))
		require.Equal(t, IX, m.locks[*doc].GroupMode)
	})

	t.Run("convert: multiple locks in queue, incompatible", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, IS)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = m.Lock(getCtx(t), 2, doc, IS)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = m.Lock(getCtx(t), 3, doc, IX)
		require.NoError(t, err)
		require.True(t, ok)

		// convert tx 1 to X
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ok, err = m.Lock(ctx, 1, doc, X)
		require.Error(t, err)
		require.False(t, ok)
		require.Equal(t, 3, queueLen(m.locks[*doc].Queue))
		require.Equal(t, IX, m.locks[*doc].GroupMode)
	})
}

func TestLockManagerUnlock(t *testing.T) {
	t.Run("empty manager", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok := m.Unlock(1, doc)
		require.True(t, ok)
	})

	t.Run("unknown lock", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, IS)
		require.NoError(t, err)
		require.True(t, ok)

		ok = m.Unlock(2, doc)
		require.True(t, ok)
	})

	t.Run("unlock", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, IS)
		require.NoError(t, err)
		require.True(t, ok)

		ok = m.Unlock(1, doc)
		require.True(t, ok)
	})

	t.Run("unlock should wake up waiting lock", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, S)
		require.NoError(t, err)
		require.True(t, ok)

		ch := make(chan struct{})
		go func() {
			defer close(ch)

			ok, err := m.Lock(context.TODO(), 2, doc, X)
			require.NoError(t, err)
			require.True(t, ok)
		}()

		time.Sleep(time.Millisecond)
		ok = m.Unlock(1, doc)
		require.True(t, ok)

		<-ch
	})

	t.Run("unlock should wake up next waiting lock", func(t *testing.T) {
		m := NewLockManager()

		doc := NewDocumentObject("t", []byte("a"))

		ok, err := m.Lock(getCtx(t), 1, doc, S)
		require.NoError(t, err)
		require.True(t, ok)

		ch1 := make(chan struct{})
		ch2 := make(chan struct{})

		go func() {
			defer close(ch1)

			ok, err := m.Lock(context.TODO(), 2, doc, X)
			require.NoError(t, err)
			require.True(t, ok)
		}()

		go func() {
			defer close(ch2)

			time.Sleep(time.Millisecond)
			ok, err := m.Lock(context.TODO(), 3, doc, X)
			require.NoError(t, err)
			require.True(t, ok)
		}()

		time.Sleep(10 * time.Millisecond)
		ok = m.Unlock(1, doc)
		require.True(t, ok)

		<-ch1

		ok = m.Unlock(2, doc)
		require.True(t, ok)

		<-ch2
	})
}

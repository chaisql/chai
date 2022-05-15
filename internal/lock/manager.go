package lock

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
)

// A LockManager is used to acquire locks on database objects.
// It is used by the transaction manager to ensure that
// transactions do not interfere with each other.
type LockManager struct {
	mu sync.Mutex

	locks map[Object]*LockHeader
}

// NewLockManager creates a lock manager.
func NewLockManager() *LockManager {
	var lm LockManager
	lm.locks = make(map[Object]*LockHeader)
	return &lm
}

func (lm *LockManager) HasLock(txid uint64, obj *Object, mode LockMode) bool {
	lm.mu.Lock()
	head, ok := lm.locks[*obj]
	if !ok {
		lm.mu.Unlock()
		return false
	}
	// A lock exists for this object.
	// Lock the queue header and unlock the map.
	head.mu.Lock()
	lm.mu.Unlock()

	for req := head.Queue; req != nil; req = req.Next {
		if req.Txid == txid {
			head.mu.Unlock()
			return req.Mode == mode
		}
	}

	head.mu.Unlock()
	return false
}

func (lm *LockManager) Lock(ctx context.Context, txid uint64, obj *Object, mode LockMode) (bool, error) {
	lm.mu.Lock()
	head, ok := lm.locks[*obj]
	if !ok {
		// No lock exists for this object.
		head = &LockHeader{
			Object:    obj,
			GroupMode: mode,
			Queue: &LockRequest{
				Status: LockGranted,
				Mode:   mode,
				Count:  1,
				Txid:   txid,
			},
		}
		head.Queue.Head = head
		lm.locks[*obj] = head
		lm.mu.Unlock()
		return true, nil
	}

	// A lock exists for this object.
	// Lock the queue header and unlock the map.
	head.mu.Lock()
	lm.mu.Unlock()

	// check if a lock request is already in the queue for this couple txid / obj
	var req, last *LockRequest
	for req = head.Queue; req != nil; req = req.Next {
		if req.Txid == txid {
			// A lock request is already in the queue for this transaction.
			break
		}
		last = req
	}

	if req == nil {
		// No lock request is already in the queue for this couple txid / obj.
		// Create a new request.
		req = &LockRequest{
			Head:  head,
			Mode:  mode,
			Count: 1,
			Txid:  txid,
		}

		// Add the request to the queue.
		if last != nil {
			last.Next = req
		} else {
			head.Queue = req
		}

		// Check if the lock is compatible with the current mode and if there
		// are no other requests in the queue.
		if !head.Waiting && head.GroupMode.IsCompatibleWith(mode) {
			// No need to wait for a lock: update the group mode and return
			// immediately.
			head.GroupMode = MaxMode(mode, head.GroupMode)
			req.Status = LockGranted
			head.mu.Unlock()
			return true, nil
		}

		// Wait for the lock.
		head.Waiting = true
		req.Status = LockWaiting
		req.WakeUp = make(chan struct{})
		head.mu.Unlock()

		select {
		case <-ctx.Done():
			lm.Unlock(txid, obj)
			return false, errors.Wrap(ctx.Err(), "lock timeout")
		case <-req.WakeUp:
			return true, nil
		}
	}

	// A lock request is already in the queue for this couple txid / obj.
	// Check if the lock is compatible with all locks of the granted group.
	compatible := true
	for other := head.Queue; other != nil && other.Status == LockGranted; other = other.Next {
		if other != req && !other.Mode.IsCompatibleWith(mode) {
			compatible = false
		}
	}
	if !compatible {
		// Wait for the lock.
		head.Waiting = true
		req.Status = LockConverting
		req.ConvertMode = mode
		req.WakeUp = make(chan struct{})
		head.mu.Unlock()

		select {
		case <-ctx.Done():
			// if the context gets canceled, the transaction will rollback,
			// and call unlock on all objects that were locked by this transaction
			return false, errors.Wrap(ctx.Err(), "lock timeout")
		case <-req.WakeUp:
			return true, nil
		}
	}

	// The lock is compatible with all locks of the granted group.
	// Update the counter, the group mode and return.
	req.Count++
	req.Mode = mode
	head.GroupMode = MaxMode(mode, head.GroupMode)
	head.mu.Unlock()
	return true, nil
}

func (lm *LockManager) Unlock(txid uint64, obj *Object) bool {
	lm.mu.Lock()
	head, ok := lm.locks[*obj]
	if !ok {
		lm.mu.Unlock()
		return true
	}
	head.mu.Lock()

	// get the lock owned by this transaction
	var req, prev *LockRequest
	for req = head.Queue; req != nil; req = req.Next {
		if req.Txid == txid {
			break
		}
		prev = req
	}
	// if there is no lock owned by this transaction on the given object, return
	if req == nil {
		head.mu.Unlock()
		lm.mu.Unlock()
		return true
	}

	// if this request is held multiple times by the same transaction,
	// decrement the count and return
	if req.Count > 1 {
		req.Count--
		head.mu.Unlock()
		lm.mu.Unlock()
		return true
	}

	// if this is the only request in the queue, remove the request
	// and the queue header.
	if head.Queue == req && req.Next == nil {
		head.mu.Unlock()
		delete(lm.locks, *obj)
		lm.mu.Unlock()
		return true
	}

	// remove the request from the queue
	if prev != nil {
		prev.Next = req.Next
	} else {
		head.Queue = req.Next
	}

	head.Waiting = false
	head.GroupMode = Free

	// wake up all compatible requests
	for req = head.Queue; req != nil; req = req.Next {
		// refresh the group mode with granted requests
		if req.Status == LockGranted {
			head.GroupMode = MaxMode(req.Mode, head.GroupMode)
			continue
		}

		// deal with converting requests before waiting requests
		if req.Status == LockConverting {
			// if a lock is converting, only wake up the request if the
			// new mode is compatible with every other member of the group.
			compatible := true
			for other := head.Queue; other != nil && other.Status == LockGranted; other = other.Next {
				if other == req {
					continue
				}

				if !other.Mode.IsCompatibleWith(req.ConvertMode) {
					compatible = false
					break
				}
			}
			if compatible {
				req.Status = LockGranted
				req.Count++
				head.GroupMode = MaxMode(req.Mode, head.GroupMode)
				close(req.WakeUp)
			} else {
				// stop here
				head.Waiting = true
				break
			}

			continue
		}

		// deal with waiting requests
		if req.Status == LockWaiting {
			// if the lock is compatible with the current mode, grant it
			if head.GroupMode.IsCompatibleWith(req.Mode) {
				req.Status = LockGranted
				head.GroupMode = MaxMode(req.Mode, head.GroupMode)
				close(req.WakeUp)
			} else {
				// stop here
				head.Waiting = true
				break
			}

			continue
		}
	}

	head.mu.Unlock()
	lm.mu.Unlock()

	return true
}

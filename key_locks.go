package kreconciler

import (
	"sync"
	"time"
)

// used to never run two items with the same lock key at once, across workers
type keyLocks struct {
	m      sync.Mutex
	parked map[string][]parkedItem // a key is taken while it is in the map
}

type parkedItem struct {
	item
	w     *worker
	since time.Time
}

func newKeyLocks() *keyLocks {
	return &keyLocks{parked: map[string][]parkedItem{}}
}

// take takes i's lock key, or parks i (on w's queue) behind the items already waiting for it.
func (k *keyLocks) take(w *worker, i item) bool {
	k.m.Lock()
	defer k.m.Unlock()
	waiting, taken := k.parked[i.lockKey]
	if taken {
		k.parked[i.lockKey] = append(waiting, parkedItem{item: i, w: w, since: time.Now()})
		return false
	}
	k.parked[i.lockKey] = nil
	return true
}

// release frees key, or hands it to the oldest item parked on it, which the caller must queue.
func (k *keyLocks) release(key string) (parkedItem, bool) {
	k.m.Lock()
	defer k.m.Unlock()
	waiting := k.parked[key]
	if len(waiting) == 0 {
		delete(k.parked, key)
		return parkedItem{}, false
	}
	k.parked[key] = waiting[1:]
	return waiting[0], true
}

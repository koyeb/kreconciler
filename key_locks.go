package kreconciler

import (
	"sync"
)

// used to never run two items with the same key at once, across queues
type keyLocks struct {
	m      sync.Mutex
	parked map[string][]parkedItem // a key is taken while it is in the map
}

type parkedItem struct {
	item
	w *worker
}

func newKeyLocks() *keyLocks {
	return &keyLocks{parked: map[string][]parkedItem{}}
}

// take takes i's key, or parks i (from w's queue) behind the items already waiting for it.
func (k *keyLocks) take(w *worker, i item) bool {
	k.m.Lock()
	defer k.m.Unlock()
	waiting, taken := k.parked[i.key]
	if taken {
		k.parked[i.key] = append(waiting, parkedItem{item: i, w: w})
		return false
	}
	k.parked[i.key] = nil
	return true
}

// release frees key, or hands it to the oldest item parked on it, which the caller must run.
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

package kreconciler

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parentKeyer keys "<parent>/<child>" ids by their parent.
func parentKeyer(_ context.Context, id string) (string, error) {
	parent, _, _ := strings.Cut(id, "/")
	return parent, nil
}

// runController runs a controller until scenario returns, scenario gets the controller and the enqueue function.
func runController(t *testing.T, conf Config, reconciler Reconciler, scenario func(c *controller, enqueue func(id string))) {
	obs := obsForTest(t)
	conf.Observability = obs.Observability()
	conf.LeaderElectionEnabled = false
	ctx, done := context.WithCancel(context.Background())
	var c *controller
	c = New(conf, reconciler, map[string]EventStream{
		"default": EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
			scenario(c, func(id string) { require.NoError(t, handler.Call(ctx, id)) })
			done()
			return nil
		}),
	}).(*controller)
	require.NoError(t, c.Run(ctx))
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond * 5)
	}
}

func TestLockKeyerSameKeyNeverRunsTwice(t *testing.T) {
	var m sync.Mutex
	running := map[string]int{}
	calls := map[string]int{}
	overlaps, maxRunning, total := 0, 0, 0
	reconciler := ReconcilerFunc(func(ctx context.Context, id string) Result {
		key, _ := parentKeyer(ctx, id)
		m.Lock()
		running[key]++
		total++
		if running[key] > 1 {
			overlaps++
		}
		maxRunning = max(maxRunning, total)
		calls[id]++
		m.Unlock()
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
		m.Lock()
		running[key]--
		total--
		m.Unlock()
		return Result{}
	})

	var ids []string
	for p := 0; p < 5; p++ {
		for c := 0; c < 10; c++ {
			ids = append(ids, fmt.Sprintf("p%d/c%d", p, c))
		}
	}
	conf := DefaultConfig()
	conf.WorkerCount = 8
	conf.LockKeyer = parentKeyer
	runController(t, conf, reconciler, func(_ *controller, enqueue func(string)) {
		for round := 0; round < 20; round++ {
			for _, i := range rand.Perm(len(ids)) {
				enqueue(ids[i])
			}
			time.Sleep(time.Millisecond)
		}
		eventually(t, 10*time.Second, func() bool {
			m.Lock()
			defer m.Unlock()
			return len(calls) == len(ids) && total == 0
		})
	})

	assert.Zero(t, overlaps, "two items with the same lock key ran at once")
	assert.Greater(t, maxRunning, 1, "items never ran concurrently: the test proves nothing")
}

func TestLockKeyerSlowKeyDoesNotDelayOthers(t *testing.T) {
	const slow = time.Second
	for _, keyed := range []bool{false, true} {
		t.Run(fmt.Sprintf("keyed=%t", keyed), func(t *testing.T) {
			var m sync.Mutex
			start := time.Now()
			finished := map[string]time.Duration{}
			reconciler := ReconcilerFunc(func(_ context.Context, id string) Result {
				if strings.HasPrefix(id, "slow/") {
					time.Sleep(slow)
				}
				m.Lock()
				finished[id] = time.Since(start)
				m.Unlock()
				return Result{}
			})

			conf := DefaultConfig()
			conf.WorkerCount = 4
			if keyed {
				conf.LockKeyer = parentKeyer
			}
			runController(t, conf, reconciler, func(_ *controller, enqueue func(string)) {
				start = time.Now()
				enqueue("slow/a")
				enqueue("slow/b")
				for i := 0; i < 20; i++ {
					enqueue(fmt.Sprintf("fast%d/x", i))
				}
				eventually(t, 5*time.Second, func() bool {
					m.Lock()
					defer m.Unlock()
					return len(finished) == 22
				})
			})

			var lastFast time.Duration
			for id, d := range finished {
				if !strings.HasPrefix(id, "slow/") {
					lastFast = max(lastFast, d)
				}
			}
			if keyed {
				assert.Less(t, lastFast, slow/2, "a fast item waited behind a slow one")
				assert.GreaterOrEqual(t, finished["slow/b"]-finished["slow/a"], slow, "slow/b ran while slow/a held their key")
			} else {
				// Hash routing: fast ids hashed to a slow id's worker wait for it.
				assert.GreaterOrEqual(t, lastFast, slow)
			}
		})
	}
}

func TestLockKeyerParkedItem(t *testing.T) {
	var m sync.Mutex
	calls := map[string]int{}
	unblock := make(chan struct{})
	reconciler := ReconcilerFunc(func(_ context.Context, id string) Result {
		m.Lock()
		calls[id]++
		n := calls[id]
		m.Unlock()
		switch id {
		case "k/holder":
			<-unblock
		case "k/delayed":
			if n == 1 {
				return Result{RequeueDelay: 100 * time.Millisecond}
			}
		case "k/failing":
			return Result{Error: fmt.Errorf("fail %d", n)}
		}
		return Result{}
	})
	callsOf := func(id string) int {
		m.Lock()
		defer m.Unlock()
		return calls[id]
	}

	ot := obsForTest(t)
	w, err := newWorker(ot.Observability(), 0, "default", 10, 3, 10, 10*time.Millisecond, 0, reconciler)
	require.NoError(t, err)
	require.NoError(t, w.shareQueue(newKeyLocks(), 2))
	ctx, done := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Run(ctx)
	}()

	require.NoError(t, w.Enqueue("k/holder", "k"))
	eventually(t, time.Second, func() bool { return callsOf("k/holder") == 1 })
	for _, id := range []string{"k/delayed", "k/failing", "k/delayed", "other"} {
		require.NoError(t, w.Enqueue(id, strings.Split(id, "/")[0]))
	}
	eventually(t, time.Second, func() bool { return callsOf("other") == 1 })
	assert.Zero(t, callsOf("k/delayed"), "ran while its key was held")
	assert.Equal(t, 2, w.objectLocks.Size(), "parked items keep their queue slot, so a duplicate enqueue is deduplicated")

	close(unblock)
	eventually(t, 2*time.Second, func() bool { return callsOf("k/delayed") == 2 && callsOf("k/failing") == 3 })
	time.Sleep(100 * time.Millisecond)
	done()
	wg.Wait()
	assert.Equal(t, map[string]int{"k/holder": 1, "k/delayed": 2, "k/failing": 3, "other": 1}, calls)
	assert.Empty(t, w.keyLocks.parked, "a key stays taken")
}

func TestLockKeyerSharedWorkers(t *testing.T) {
	reservedFirst := func(id int) string {
		if id < 2 {
			return "reserved"
		}
		return "default"
	}
	testCases := map[string]struct {
		keyer      func(ctx context.Context, id string) (string, error)
		nameHelper func(id int) string
		// the worker id each worker id shares, and that worker's loops
		sharesWith  []int
		concurrency []int
	}{
		"unset: one worker each": {
			sharesWith:  []int{0, 1, 2, 3},
			concurrency: []int{1, 1, 1, 1},
		},
		"unset with names: one worker each": {
			nameHelper:  reservedFirst,
			sharesWith:  []int{0, 1, 2, 3},
			concurrency: []int{1, 1, 1, 1},
		},
		"keyed: one shared worker": {
			keyer:       parentKeyer,
			sharesWith:  []int{0, 0, 0, 0},
			concurrency: []int{4, 4, 4, 4},
		},
		"keyed with names: one per name": {
			keyer:       parentKeyer,
			nameHelper:  reservedFirst,
			sharesWith:  []int{0, 0, 2, 2},
			concurrency: []int{2, 2, 2, 2},
		},
	}
	for n, tt := range testCases {
		t.Run(n, func(t *testing.T) {
			conf := DefaultConfig()
			conf.WorkerCount = 4
			conf.LockKeyer = tt.keyer
			conf.WorkerNameHelper = tt.nameHelper
			runController(t, conf, &countingHandler{}, func(c *controller, _ func(string)) {
				require.Len(t, c.workers, 4)
				for i, w := range c.workers {
					assert.Equal(t, tt.sharesWith[i], w.id)
					assert.Equal(t, tt.concurrency[i], w.concurrency)
					assert.Equal(t, conf.WorkerQueueSize*w.concurrency, w.capacity)
					assert.Equal(t, conf.DelayQueueSize*w.concurrency, w.delayQueue.capacity)
					assert.Equal(t, tt.keyer != nil, w.keyLocks != nil)
				}
			})
		})
	}
}

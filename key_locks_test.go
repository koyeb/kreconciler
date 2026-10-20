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

// parentRouter keys "<parent>/<child>" ids by their parent, and sends "<queue>:<parent>/<child>" ids to <queue>.
var parentRouter = RouterFunc(func(_ context.Context, id string) (Route, error) {
	queue, rest, found := strings.Cut(id, ":")
	if !found {
		queue, rest = "", id
	}
	parent, _, _ := strings.Cut(rest, "/")
	return Route{Queue: queue, Key: parent, Drop: parent == "drop"}, nil
})

// runController runs a controller until scenario returns, scenario gets the controller and the enqueue function.
func runController(t *testing.T, conf Config, reconciler Reconciler, scenario func(c *controller, enqueue func(id string))) {
	obs := obsForTest(t)
	conf.Observability = obs.Observability()
	conf.LeaderElectionEnabled = false
	ctx, done := context.WithCancel(context.Background())
	var c *controller
	c = New(conf, reconciler, map[string]EventStream{
		"default": EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
			defer done() // also when scenario fails: t.Fatal exits this goroutine
			scenario(c, func(id string) { require.NoError(t, handler.Call(ctx, id)) })
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

func TestSameKeyNeverRunsTwice(t *testing.T) {
	var m sync.Mutex
	running := map[string]int{}
	calls := map[string]int{}
	overlaps, maxRunning, total := 0, 0, 0
	reconciler := ReconcilerFunc(func(ctx context.Context, id string) Result {
		route, _ := parentRouter(ctx, id)
		m.Lock()
		running[route.Key]++
		total++
		if running[route.Key] > 1 {
			overlaps++
		}
		maxRunning = max(maxRunning, total)
		calls[id]++
		m.Unlock()
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
		m.Lock()
		running[route.Key]--
		total--
		m.Unlock()
		return Result{}
	})

	// The same parents in two queues: keys hold across queues.
	var ids []string
	for p := 0; p < 5; p++ {
		for c := 0; c < 10; c++ {
			ids = append(ids, fmt.Sprintf("p%d/c%d", p, c), fmt.Sprintf("reserved:p%d/c%d", p, c))
		}
	}
	conf := DefaultConfig()
	conf.WorkerCount = 8
	conf.Queues = map[string]int{"reserved": 3}
	conf.Router = parentRouter
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

	assert.Zero(t, overlaps, "two items with the same key ran at once")
	assert.Greater(t, maxRunning, 1, "items never ran concurrently: the test proves nothing")
}

func TestSlowKeyDoesNotDelayOthers(t *testing.T) {
	const slow = time.Second
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
	conf.Router = parentRouter
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
	assert.Less(t, lastFast, slow/2, "a fast item waited behind a slow one")
	assert.GreaterOrEqual(t, finished["slow/b"]-finished["slow/a"], slow, "slow/b ran while slow/a held their key")
}

func TestReservedQueue(t *testing.T) {
	var m sync.Mutex
	finished := map[string]bool{}
	unblock := make(chan struct{})
	reconciler := ReconcilerFunc(func(_ context.Context, id string) Result {
		if !strings.HasPrefix(id, "reserved:") {
			<-unblock
		}
		m.Lock()
		finished[id] = true
		m.Unlock()
		return Result{}
	})

	conf := DefaultConfig()
	conf.WorkerCount = 3
	conf.Queues = map[string]int{"reserved": 1}
	conf.Router = parentRouter
	runController(t, conf, reconciler, func(c *controller, enqueue func(string)) {
		release := sync.OnceFunc(func() { close(unblock) })
		defer release() // also when the test fails, or the controller never stops
		require.Len(t, c.queues, 2)
		assert.Equal(t, 2, c.queues[defaultQueue].count)
		assert.Equal(t, 1, c.queues["reserved"].count)
		assert.Equal(t, conf.WorkerQueueSize*2, c.queues[defaultQueue].capacity)
		assert.Equal(t, conf.DelayQueueSize*2, c.queues[defaultQueue].delayQueue.capacity)

		for i := 0; i < 5; i++ {
			enqueue(fmt.Sprintf("busy%d/x", i))
			enqueue(fmt.Sprintf("unknown:other%d/x", i)) // a queue not in Queues: the default one
		}
		enqueue("reserved:a/x")
		enqueue("drop/x")
		eventually(t, time.Second, func() bool {
			m.Lock()
			defer m.Unlock()
			return finished["reserved:a/x"]
		})
		assert.Equal(t, 8, c.queues[defaultQueue].objectLocks.Size(), "every default worker is busy")
		release()
		eventually(t, time.Second, func() bool {
			m.Lock()
			defer m.Unlock()
			return len(finished) == 11
		})
	})
	assert.False(t, finished["drop/x"])
}

func TestQueuesConfig(t *testing.T) {
	for n, queues := range map[string]map[string]int{
		"reserved name":   {defaultQueue: 1},
		"no worker":       {"a": 0},
		"no default left": {"a": 2, "b": 2},
	} {
		t.Run(n, func(t *testing.T) {
			conf := DefaultConfig()
			conf.WorkerCount = 4
			conf.Queues = queues
			conf.LeaderElectionEnabled = false
			obs := obsForTest(t)
			conf.Observability = obs.Observability()
			assert.Error(t, New(conf, &countingHandler{}, nil).Run(context.Background()))
		})
	}
}

func TestParkedItem(t *testing.T) {
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
	w, err := newWorker(ot.Observability(), 0, 2, "default", 10, 3, 10, 10*time.Millisecond, 0, reconciler, newKeyLocks())
	require.NoError(t, err)
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

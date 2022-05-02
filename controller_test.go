package kreconciler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingHandler struct {
	sync.Mutex
	calls map[string]int
}

func (h *countingHandler) Apply(ctx context.Context, id string) Result {
	h.Lock()
	defer h.Unlock()
	if h.calls == nil {
		h.calls = map[string]int{}
	}
	h.calls[id] += 1
	return Result{}
}

func (h *countingHandler) Calls() map[string]int {
	h.Lock()
	defer h.Unlock()
	res := make(map[string]int, len(h.calls))
	for k, v := range h.calls {
		res[k] = v
	}
	return res
}

func TestReconciler(t *testing.T) {
	testCases := map[string]struct {
		conf     Config
		scenario func(dome func()) EventStreamFunc
		assert   func(t *testing.T, c *controller, m map[string]int)
	}{
		"simple": {
			scenario: func(done func()) EventStreamFunc {
				return func(ctx context.Context, handler EventHandler) error {
					handler.Call(ctx, "a")
					handler.Call(ctx, "b")
					handler.Call(ctx, "c")
					time.Sleep(time.Millisecond * 20)
					handler.Call(ctx, "c")
					time.Sleep(time.Millisecond * 10)
					done()
					return nil
				}
			},
			assert: func(t *testing.T, c *controller, calls map[string]int) {
				assert.Equal(t, map[string]int{
					"a": 1,
					"b": 1,
					"c": 2,
				}, calls)
			},
		},
		"ignore_empty_event": {
			scenario: func(done func()) EventStreamFunc {
				return func(ctx context.Context, handler EventHandler) error {
					handler.Call(ctx, "a")
					handler.Call(ctx, "")
					handler.Call(ctx, "c")
					time.Sleep(time.Millisecond * 10)
					done()
					return nil
				}
			},
			assert: func(t *testing.T, c *controller, calls map[string]int) {
				assert.Equal(t, map[string]int{
					"a": 1,
					"c": 1,
				}, calls)
			},
		},
	}

	for n, tt := range testCases {
		t.Run(n, func(t *testing.T) {
			// Because inside a worker everything should look serial we can always test with many workers
			for _, count := range []int{1, 2, 4} {
				t.Run(fmt.Sprintf("With %d workers", count), func(t *testing.T) {
					obs := obsForTest(t)
					handler := countingHandler{}
					ctx, done := context.WithCancel(context.Background())

					conf := tt.conf
					if conf.WorkerQueueSize == 0 {
						conf = DefaultConfig()
					}
					conf.LeaderElectionEnabled = false
					conf.WorkerCount = count
					conf.Observability = obs.Observability()
					c := New(conf, &handler, map[string]EventStream{"default": tt.scenario(done)})
					require.NoError(t, c.Run(ctx))

					tt.assert(t, c.(*controller), handler.Calls())
				})
			}
		})
	}
}

func TestEventStreamError(t *testing.T) {
	obs := obsForTest(t)
	handler := &countingHandler{}

	conf := DefaultConfig()
	conf.Observability = obs.Observability()
	conf.LeaderElectionEnabled = false
	c := New(conf, handler, map[string]EventStream{
		"default": EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
			return errors.New("oops")
		}),
	})

	require.Error(t, c.Run(context.Background()))
}

func TestReconcilerWithLock(t *testing.T) {
	obs := obsForTest(t)
	handler := &countingHandler{}

	conf := DefaultConfig()
	conf.Observability = obs.Observability()
	ctx, done := context.WithCancel(context.Background())
	c := New(conf, handler, map[string]EventStream{
		"default": EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
			handler.Call(ctx, "a")
			handler.Call(ctx, "b")
			handler.Call(ctx, "c")
			time.Sleep(time.Millisecond * 20)
			handler.Call(ctx, "c")
			time.Sleep(time.Millisecond * 10)
			done()
			return nil
		}),
	})
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		c.Run(ctx)
	}()

	time.Sleep(time.Millisecond * 50)
	assert.Empty(t, handler.Calls())

	// Now we're leader
	c.BecomeLeader()

	wg.Wait()

	assert.Equal(t, map[string]int{
		"a": 1,
		"b": 1,
		"c": 2,
	}, handler.Calls())
}

func TestResyncLoopEventStream(t *testing.T) {
	obs := obsForTest(t)
	stream, err := ResyncLoopEventStream(obs.Observability(), time.Millisecond*50, func(ctx context.Context) ([]string, error) {
		return []string{"a", "b", "c"}, nil
	})
	assert.NoError(t, err)

	idChannel := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())

	go stream.Subscribe(ctx, EventHandlerFunc(func(_ context.Context, id string) error {
		idChannel <- id
		return nil
	}))
	for _, v := range []string{"a", "b", "c"} {
		rec := <-idChannel
		assert.Equal(t, v, rec)
	}
	assert.Empty(t, idChannel)

	time.Sleep(time.Millisecond * 60)
	for _, v := range []string{"a", "b", "c"} {
		rec := <-idChannel
		assert.Equal(t, v, rec)
	}
	assert.Empty(t, idChannel)

	time.Sleep(time.Millisecond * 120)
	for _, v := range []string{"a", "b", "c", "a", "b", "c"} {
		rec := <-idChannel
		assert.Equal(t, v, rec)
	}
	assert.Empty(t, idChannel)

	cancel()
	time.Sleep(time.Millisecond * 60)
	assert.Empty(t, idChannel)
}

func TestReconcilerWithLockNeverLeader(t *testing.T) {
	obs := obsForTest(t)

	handler := &countingHandler{}

	conf := DefaultConfig()
	conf.Observability = obs.Observability()
	ctx, done := context.WithCancel(context.Background())
	c := New(conf, handler, map[string]EventStream{
		"default": EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
			handler.Call(ctx, "a")
			return nil
		}),
	})
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		c.Run(ctx)
	}()

	time.Sleep(time.Millisecond * 50)
	assert.Empty(t, handler.Calls())

	// Now finish
	done()
	wg.Wait()

	assert.Empty(t, handler.Calls())
}

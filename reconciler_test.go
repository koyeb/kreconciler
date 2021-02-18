package reconciler

import (
	"context"
	"fmt"
	"github.com/koyeb/api.koyeb.com/internal/pkg/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type handlerMock struct {
	mock.Mock
}

func (h *handlerMock) Handle(_ context.Context, id string) Result {
	res := h.Called(id)
	return res.Get(0).(Result)
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
					handler.Handle("a")
					handler.Handle("b")
					handler.Handle("c")
					time.Sleep(time.Millisecond * 200)
					handler.Handle("c")
					time.Sleep(time.Millisecond * 100)
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
	}

	for n, tt := range testCases {
		t.Run(n, func(t *testing.T) {
			// Because inside a worker everything should look serial we can always test with many workers
			for _, count := range []int{1, 2, 4} {
				t.Run(fmt.Sprintf("With %d workers", count), func(t *testing.T) {
					obs := observability.NewForTest(t)
					handlerCalls := map[string]int{}
					m := sync.Mutex{}
					handler := HandlerFunc(func(ctx context.Context, id string) Result {
						m.Lock()
						defer m.Unlock()
						handlerCalls[id] += 1
						return Result{}
					})
					ctx, done := context.WithCancel(context.Background())

					conf := tt.conf
					if conf.WorkerQueueSize == 0 {
						conf = DefaultConfig()
					}
					conf.WorkerHasher = DefaultHasher{Num: uint32(count)}
					c := New(obs, conf, handler, tt.scenario(done))
					require.NoError(t, c.Run(ctx))

					m.Lock()
					defer m.Unlock()
					tt.assert(t, c.(*controller), handlerCalls)
				})
			}
		})
	}

}

func TestResyncLoopEventStream(t *testing.T) {
	obs := observability.NewForTest(t)
	stream := ResyncLoopEventStream(obs, time.Millisecond*100, func(ctx context.Context) ([]string, error) {
		return []string{"a", "b", "c"}, nil
	})
	idChannel := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())

	go stream.Subscribe(ctx, EventHandlerFunc(func(id string) error {
		idChannel <- id
		return nil
	}))
	for _, v := range []string{"a", "b", "c"} {
		rec := <-idChannel
		assert.Equal(t, v, rec)
	}
	assert.Empty(t, idChannel)

	time.Sleep(time.Millisecond * 110)
	for _, v := range []string{"a", "b", "c"} {
		rec := <-idChannel
		assert.Equal(t, v, rec)
	}
	assert.Empty(t, idChannel)

	time.Sleep(time.Millisecond * 220)
	for _, v := range []string{"a", "b", "c", "a", "b", "c"} {
		rec := <-idChannel
		assert.Equal(t, v, rec)
	}
	assert.Empty(t, idChannel)

	cancel()
	time.Sleep(time.Millisecond * 100)
	assert.Empty(t, idChannel)
}

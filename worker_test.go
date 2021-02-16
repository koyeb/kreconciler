package reconciler

import (
	"context"
	"errors"
	"github.com/koyeb/api.koyeb.com/internal/pkg/observability"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type action struct {
	id          string
	expectedErr error
	sleepBefore time.Duration
}

func TestWorker(t *testing.T) {
	testCases := map[string]struct {
		capacity   int
		maxRetries int
		actions    []action
		mock       func(m *handlerMock)
		assert     func(t *testing.T, m *handlerMock)
	}{
		"simpleInserts": {
			capacity:   2,
			maxRetries: 0,
			actions: []action{
				{id: "a"},
				{id: "b"},
			},
			mock: func(m *handlerMock) {
				m.On("Handle", "a").Return(Result{})
				m.On("Handle", "b").Return(Result{})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Handle", 2)
			},
		},
		"insertSameNoDupe": {
			capacity:   2,
			maxRetries: 0,
			actions: []action{
				{id: "a"},
				{id: "a"},
			},
			mock: func(m *handlerMock) {
				m.On("Handle", "a").Return(Result{})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Handle", 1)
			},
		},
		"retriesDrop": {
			capacity:   2,
			maxRetries: 2,
			actions: []action{
				{id: "a"},
				{id: "a"},
			},
			mock: func(m *handlerMock) {
				m.On("Handle", "a").Return(Result{Error: errors.New("not good")})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Handle", 2)
			},
		},
		"atCapacityFails": {
			capacity: 2,
			actions: []action{
				{id: "a"},
				{id: "b"},
				{id: "c", expectedErr: QueueAtCapacityError},
			},
			mock: func(m *handlerMock) {
				m.On("Handle", "a").Return(Result{})
				m.On("Handle", "b").Return(Result{})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Handle", 2)
				m.AssertNotCalled(t, "Handle", "c")
			},
		},
	}

	for n, tt := range testCases {
		t.Run(n, func(t *testing.T) {
			obs := observability.NewForTest(t)
			ctx, done := context.WithCancel(context.Background())

			mockHandler := new(handlerMock)
			tt.mock(mockHandler)

			worker := newWorker(obs, 0, tt.capacity, tt.maxRetries, mockHandler)
			wg := sync.WaitGroup{}

			go func() {
				wg.Add(1)
				defer wg.Done()
				worker.Run(ctx)
			}()

			for _, action := range tt.actions {
				time.Sleep(action.sleepBefore)
				err := worker.Enqueue(action.id)
				assert.Equal(t, action.expectedErr, err)
			}
			time.Sleep(time.Millisecond * 200)
			done()
			wg.Wait()

			tt.assert(t, mockHandler)
		})
	}
}

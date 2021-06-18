package kreconciler

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel/codes"
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
		capacity    int
		maxTries    int
		maxDuration time.Duration
		actions     []action
		mock        func(m *handlerMock)
		assert      func(t *testing.T, m *handlerMock)
	}{
		"simpleInserts": {
			capacity: 2,
			maxTries: 0,
			actions: []action{
				{id: "a"},
				{id: "b"},
			},
			mock: func(m *handlerMock) {
				m.On("Apply", mock.Anything, "a").Return(Result{})
				m.On("Apply", mock.Anything, "b").Return(Result{})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Apply", 2)
			},
		},
		"insertSameNoDupe": {
			capacity: 2,
			maxTries: 0,
			actions: []action{
				{id: "a"},
				{id: "a"},
			},
			mock: func(m *handlerMock) {
				m.On("Apply", mock.Anything, "a").Return(Result{})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Apply", 1)
			},
		},
		"retriesDrop": {
			capacity: 2,
			maxTries: 2,
			actions: []action{
				{id: "a"},
				{id: "a"},
			},
			mock: func(m *handlerMock) {
				m.On("Apply", mock.Anything, "a").Return(Result{Error: errors.New("not good")})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Apply", 2)
			},
		},
		"atCapacityFails": {
			capacity: 2,
			actions: []action{
				{id: "a"},
				{id: "b"},
				{id: "c", expectedErr: queueAtCapacityError},
			},
			mock: func(m *handlerMock) {
				m.On("Apply", mock.Anything, "a").Return(Result{})
				m.On("Apply", mock.Anything, "b").Return(Result{})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Apply", 2)
				m.AssertNotCalled(t, "Apply", mock.Anything, "c")
			},
		},
		"takeTooLong": {
			capacity: 1,
			actions: []action{
				{id: "a"},
			},
			maxTries:    2,
			maxDuration: time.Millisecond * 100,
			mock: func(m *handlerMock) {
				m.On("Apply", mock.Anything, "a").After(time.Millisecond * 200).Run(func(args mock.Arguments) {
					assert.Equal(t, context.DeadlineExceeded, args.Get(0).(context.Context).Err())
				}).Return(Result{Error: context.DeadlineExceeded})
			},
			assert: func(t *testing.T, m *handlerMock) {
				m.AssertNumberOfCalls(t, "Apply", 2)
				m.AssertNotCalled(t, "Apply", "b")
				m.AssertNotCalled(t, "Apply", "c")
			},
		},
	}

	for n, tt := range testCases {
		t.Run(n, func(t *testing.T) {
			ctx, done := context.WithCancel(context.Background())

			mockHandler := new(handlerMock)
			tt.mock(mockHandler)

			ot := obsForTest(t)
			worker := newWorker(ot.Observability(), 0, tt.capacity, tt.maxTries, 10, time.Millisecond*100, tt.maxDuration, mockHandler)
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
			time.Sleep(time.Millisecond * 600)
			done()
			wg.Wait()

			tt.assert(t, mockHandler)
		})
	}
}

func TestTraceWorker(t *testing.T) {
	obs := obsForTest(t)

	ctx, done := context.WithCancel(context.Background())

	mockHandler := new(handlerMock)
	mockHandler.On("Apply", mock.Anything, "a").Return(Result{Error: errors.New("not good")})
	mockHandler.On("Apply", mock.Anything, "b").Return(Result{})
	mockHandler.On("Apply", mock.Anything, "c").Return(Result{RequeueDelay: 250 * time.Millisecond})

	worker := newWorker(obs.Observability(), 0, 10, 2, 10, time.Millisecond*100, 0, mockHandler)
	wg := sync.WaitGroup{}

	go func() {
		wg.Add(1)
		defer wg.Done()
		worker.Run(ctx)
	}()
	worker.Enqueue("a")
	worker.Enqueue("b")
	worker.Enqueue("c")

	time.Sleep(time.Second)
	done()
	wg.Wait()

	sr := obs.SpanRecorder().Completed()
	assert.Len(t, sr, 8) // 5 handle (2 retries) + 3 reconcile
	assert.Equal(t, "a", sr[0].Attributes()["id"].AsString())
	assert.Equal(t, "handle", sr[0].Name())
	assert.Equal(t, codes.Error, sr[0].StatusCode())
	assert.NotNil(t, sr[0].Attributes()["error.type"])

	assert.Equal(t, "b", sr[1].Attributes()["id"].AsString())
	assert.Equal(t, "handle", sr[1].Name())
	assert.Equal(t, codes.Ok, sr[1].StatusCode())

	assert.Equal(t, "b", sr[2].Attributes()["id"].AsString())
	assert.Equal(t, "reconcile", sr[2].Name())
	assert.Equal(t, codes.Ok, sr[2].StatusCode())

	assert.Equal(t, "c", sr[3].Attributes()["id"].AsString())
	assert.Equal(t, "handle", sr[3].Name())
	assert.Equal(t, codes.Ok, sr[3].StatusCode())

	assert.Equal(t, "a", sr[4].Attributes()["id"].AsString())
	assert.Equal(t, "handle", sr[4].Name())
	assert.Equal(t, codes.Error, sr[4].StatusCode())

	assert.Equal(t, "a", sr[5].Attributes()["id"].AsString())
	assert.Equal(t, "reconcile", sr[5].Name())
	assert.Equal(t, codes.Error, sr[5].StatusCode())
	assert.Equal(t, "Max try exceeded", sr[5].StatusMessage())

	assert.Equal(t, "c", sr[6].Attributes()["id"].AsString())
	assert.Equal(t, "handle", sr[6].Name())
	assert.Equal(t, codes.Ok, sr[6].StatusCode())

	assert.Equal(t, "c", sr[7].Attributes()["id"].AsString())
	assert.Equal(t, "reconcile", sr[7].Name())
	assert.Equal(t, codes.Error, sr[7].StatusCode())
}

type handlerMock struct {
	mock.Mock
}

func (h *handlerMock) Apply(ctx context.Context, id string) Result {
	res := h.Called(ctx, id)
	return res.Get(0).(Result)
}

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"github.com/koyeb/api.koyeb.com/internal/pkg/observability"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/trace"
	"runtime/debug"
	"sync"
	"time"
)

type handlerMock struct {
	mock.Mock
}

func (h *handlerMock) Handle(_ context.Context, id string) Result {
	res := h.Called(id)
	return res.Get(0).(Result)
}

type worker struct {
	observability.Wrapper
	sync.Mutex
	queue       chan item
	ticker      *time.Ticker
	delayQueue  *dq
	maxTries    int
	handler     Handler
	objectLocks objectLocks
	capacity    int
}

func NewTracerHandler(tracer trace.Tracer, delegate Handler) Handler {
	return HandlerFunc(func(ctx context.Context, id string) (result Result) {
		ctx, span := tracer.Start(ctx, "reconcile",
			trace.WithNewRoot(),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				label.String("id", id),
			),
		)
		defer func() {
			if result.Error != nil {
				span.RecordError(result.Error)
				span.SetStatus(codes.Error, "")
			} else {
				span.SetStatus(codes.Ok, "")
			}
			if result.RequeueDelay != 0 {
				span.SetAttributes(label.Int64("schedule.millis", result.RequeueDelay.Milliseconds()))
			}
			span.End()
		}()
		result = delegate.Handle(ctx, id)
		return result
	})
}

func newWorker(obs observability.Wrapper, id, capacity, maxRetries, delayQueueSize int, delayResolution time.Duration, handler Handler) *worker {
	obs = obs.NewChildWrapper(fmt.Sprintf("worker-%d", id))
	return &worker{
		Wrapper:     obs,
		queue:       make(chan item, capacity+1), // TO handle the inflight item schedule
		capacity:    capacity,
		maxTries:    maxRetries,
		delayQueue:  newQueue(delayQueueSize, delayResolution),
		objectLocks: newObjectLocks(capacity),
		handler:     NewTracerHandler(obs.Tracer(), NewPanicHandler(obs, handler)),
	}
}

func NewPanicHandler(obs observability.Wrapper, handler Handler) Handler {
	return HandlerFunc(func(ctx context.Context, id string) (r Result) {
		defer func() {
			if err := recover(); err != nil {
				obs.SLogWithContext(ctx).Errorw("Panicked inside an handler", "error", err, "stack", string(debug.Stack()))
				if e, ok := err.(error); ok {
					r = Result{Error: e}
				} else {
					r = Result{Error: errors.New(err.(string))}
				}
			}
		}()
		r = handler.Handle(ctx, id)
		return
	})
}

type item struct {
	ctx      context.Context
	tryCount int
	id       string
	maxTries int
}

var QueueAtCapacityError = errors.New("queue at capacity, retry later")

func (w *worker) Enqueue(id string) error {
	return w.enqueue(item{ctx: context.Background(), id: id, maxTries: w.maxTries})
}

func (w *worker) enqueue(i item) error {
	switch w.objectLocks.Take(i.id) {
	case alreadyPresent:
		w.SLog().Debug("Item already present in the queue, ignoring enqueue")
		return nil
	case queueOverflow:
		return QueueAtCapacityError
	default:
		w.queue <- i
		return nil
	}
}

func (w *worker) Run(ctx context.Context) {
	w.SLog().Info("worker started")
	defer w.SLog().Info("worker stopped")
	go w.delayQueue.run(ctx, func(_ time.Time, i interface{}) {
		itm := i.(item)
		w.SLog().Debugw("Reenqueuing item after delay", "object_id", itm.id)
		err := w.enqueue(itm)
		if err != nil {
			w.SLog().Errorw("Failed reenqueing delayed item", "error", err)
		}
	})
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-w.queue:
			w.objectLocks.Free(item.id)
			newCtx := item.ctx
			// process the object.
			w.SLog().Debugw("Get event for item", "object_id", item.id, "try", item.tryCount)
			res := w.handler.Handle(newCtx, item.id)
			// Retry if required based on the result.
			if res.Error != nil {
				w.SLog().Warnw("Failed reconcile loop", "object_id", item.id, "error", res.Error)
			}
			delay := res.GetRequeueDelay(w.delayQueue.resolution)
			if delay != 0 {
				item.tryCount += 1
				if item.maxTries != 0 && item.tryCount == item.maxTries {
					w.SLog().Errorw("Max retry exceeded, dropping item", "object_id", item.id)
				} else {
					w.SLog().Debugw("Delay item retry", "object_id", item.id)
					err := w.delayQueue.schedule(item, delay)
					if err != nil {
						w.SLog().Errorw("Error scheduling delay", "error", err)
					}
				}
			}
		}
	}
}

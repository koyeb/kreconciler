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

func newWorker(obs observability.Wrapper, id, capacity, maxRetries, delayQueueSize int, delayResolution time.Duration, handler Handler) *worker {
	obs = obs.NewChildWrapper(fmt.Sprintf("worker-%d", id))
	return &worker{
		Wrapper:     obs,
		queue:       make(chan item, capacity+1), // TO handle the inflight item schedule
		capacity:    capacity,
		maxTries:    maxRetries,
		delayQueue:  newQueue(delayQueueSize, delayResolution),
		objectLocks: newObjectLocks(capacity),
		handler:     NewPanicHandler(obs, handler),
	}
}

func NewPanicHandler(obs observability.Wrapper, handler Handler) Handler {
	return HandlerFunc(func(ctx context.Context, id string) (r Result) {
		defer func() {
			if err := recover(); err != nil {
				obs.SLogWithContext(ctx).Errorw("Panicked inside an handler", "error", err, "stack", string(debug.Stack()))
				span := trace.SpanFromContext(ctx)
				span.AddEvent("panic")
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
	ctx, _ := w.Tracer().Start(context.Background(), "reconcile",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			label.String("id", id),
		),
	)

	return w.enqueue(item{ctx: ctx, id: id, maxTries: w.maxTries})
}

func (w *worker) enqueue(i item) error {
	parentSpan := trace.SpanFromContext(i.ctx)
	switch w.objectLocks.Take(i.id) {
	case alreadyPresent:
		parentSpan.SetStatus(codes.Ok, "already_present")
		parentSpan.End()
		w.SLog().Debug("Item already present in the queue, ignoring enqueue")
		return nil
	case queueOverflow:
		parentSpan.SetStatus(codes.Error, "queue_full")
		parentSpan.End()
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
			parentSpan := trace.SpanFromContext(item.ctx)
			parentSpan.AddEvent("dequeue")
			// process the object.
			res := w.handle(item.ctx, item.id)
			delay := res.GetRequeueDelay(w.delayQueue.resolution)
			if delay != 0 {
				item.tryCount += 1
				if item.maxTries != 0 && item.tryCount == item.maxTries {
					parentSpan.SetStatus(codes.Error, "Max try exceeded")
					parentSpan.End()
					w.SLog().Errorw("Max retry exceeded, dropping item", "object_id", item.id)
				} else {
					parentSpan.AddEvent("enqueue_with_delay", trace.WithAttributes(label.Int64("schedule.millis", delay.Milliseconds()), label.Int("try_count", item.tryCount), label.Int("max_try", item.maxTries)))
					w.SLog().Debugw("Delay item retry", "object_id", item.id)
					err := w.delayQueue.schedule(item, delay)
					if err != nil {
						parentSpan.SetStatus(codes.Error, "Failed enqueuing with delay")
						parentSpan.RecordError(err)
						parentSpan.End()
						w.SLog().Errorw("Error scheduling delay", "error", err)
					}
				}
			} else {
				w.SLog().Infow("Done")
				parentSpan.SetStatus(codes.Ok, "")
				parentSpan.End()
			}
		}
	}
}

func (w *worker) handle(ctx context.Context, id string) Result {
	handleCtx, span := w.Tracer().Start(ctx, "handle",
		trace.WithAttributes(
			label.String("id", id),
		),
	)
	w.SLog().Debugw("Get event for item", "object_id", id)
	res := w.handler.Handle(handleCtx, id)
	// Retry if required based on the result.
	if res.Error != nil {
		span.RecordError(res.Error)
		span.SetStatus(codes.Error, "")
		w.SLog().Warnw("Failed reconcile loop", "object_id", id, "error", res.Error)
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
	return res
}

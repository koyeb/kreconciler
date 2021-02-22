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
	"sync"
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
				label.String("revision.id", id),
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
				span.SetAttributes(label.Int64("requeue.millis", result.RequeueDelay.Milliseconds()))
			}
			span.End()
		}()
		result = delegate.Handle(ctx, id)
		return result
	})
}

func newWorker(obs observability.Wrapper, id, capacity, maxRetries int, handler Handler) *worker {
	obs = obs.NewChildWrapper(fmt.Sprintf("worker-%d", id))
	return &worker{
		Wrapper:     obs,
		queue:       make(chan item, capacity+1), // TO handle the inflight item requeue
		capacity:    capacity,
		maxTries:    maxRetries,
		objectLocks: newObjectLocks(capacity),
		handler:     NewTracerHandler(obs.Tracer(), handler),
	}
}

type item struct {
	ctx      context.Context
	tryCount int
	id       string
}

var QueueAtCapacityError = errors.New("queue at capacity, retry later")

func (w *worker) Enqueue(id string) error {
	switch w.objectLocks.Take(id) {
	case alreadyPresent:
		return nil
	case queueOverflow:
		return QueueAtCapacityError
	default:
		w.queue <- item{ctx: context.Background(), id: id}
		return nil
	}
}

func (w *worker) Run(ctx context.Context) {
	w.SLog().Info("worker started")
	defer w.SLog().Info("worker stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-w.queue:
			w.objectLocks.Free(item.id)
			newCtx := item.ctx
			// process the object.
			w.SLog().Debugw("Get event for item", "id", item.id, "try", item.tryCount)
			res := w.handler.Handle(newCtx, item.id)
			// Retry if required based on the result.
			if res.Error != nil {
				w.SLog().Warnw("Failed reconcile loop", "object_id", item.id, "error", res.Error)
			}
			// TODO handle delay
			delay := res.GetRequeueDelay()
			if delay != 0 {
				item.tryCount += 1
				if w.maxTries != 0 && item.tryCount == w.maxTries {
					w.SLog().Errorw("Max retry exceeded, dropping item", "object_id", item.id)
				} else {
					switch w.objectLocks.Take(item.id) {
					case alreadyPresent:
						w.SLog().Debug("Item already present in the queue, ignoring enqueue")
					case queueOverflow:
						panic("Queue at capacity this shouldn't happen on a requeue!")
					default:
						w.queue <- item
					}
				}
			}
		}
	}
}

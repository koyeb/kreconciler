package reconciler

import (
	"context"
	"errors"
	"fmt"
	"github.com/koyeb/api.koyeb.com/internal/pkg/observability"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/unit"
	"runtime/debug"
	"sync"
	"time"
)

type metrics struct {
	queueSizeObserver        metric.Int64ValueObserver
	dequeue                  metric.BoundInt64Counter
	handleResultOk           metric.BoundInt64Counter
	handleResultDropMaxTry   metric.BoundInt64Counter
	handleResultErrorRequeue metric.BoundInt64Counter
	handleResultDelayRequeue metric.BoundInt64Counter
	delayWithoutError        metric.BoundInt64ValueRecorder
	delayWithError           metric.BoundInt64ValueRecorder
	handleLatencySuccess     metric.BoundInt64ValueRecorder
	handleLatencyError       metric.BoundInt64ValueRecorder
	enqueueOk                metric.BoundInt64Counter
	enqueueFull              metric.BoundInt64Counter
	enqueueAlreadyPresent    metric.BoundInt64Counter
	queueTime                metric.BoundInt64ValueRecorder
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
	metrics     *metrics
}

func newWorker(obs observability.Wrapper, id, capacity, maxTries, delayQueueSize int, delayResolution time.Duration, maxReconcileTime time.Duration, handler Handler) *worker {
	obs = obs.NewChildWrapper(fmt.Sprintf("worker-%d", id))
	w := &worker{
		Wrapper:     obs,
		queue:       make(chan item, capacity+1), // TO handle the inflight item schedule
		capacity:    capacity,
		maxTries:    maxTries,
		metrics:     &metrics{},
		delayQueue:  newQueue(delayQueueSize, delayResolution),
		objectLocks: newObjectLocks(capacity),
		handler:     NewPanicHandler(obs, NewHandlerWithTimeout(handler, maxReconcileTime)),
	}
	decorateMeter(w, id)

	return w
}

func decorateMeter(w *worker, id int) {
	meter := metric.Must(w.Meter())
	w.metrics.queueSizeObserver = meter.NewInt64ValueObserver("kreconciler_worker_queue_size", func(ctx context.Context, result metric.Int64ObserverResult) {
		result.Observe(int64(w.objectLocks.Size()), label.Int("workerId", id))
	},
		metric.WithUnit(unit.Dimensionless),
		metric.WithDescription("The number of outstanding items to reconcile"),
	)

	enqueue := meter.NewInt64Counter("kreconciler_enqueue",
		metric.WithUnit(unit.Dimensionless),
		metric.WithDescription("The number of times an item was added to the reconcile queue"),
	)
	w.metrics.enqueueOk = enqueue.Bind(label.Int("workerId", id), label.String("status", "ok"))
	w.metrics.enqueueFull = enqueue.Bind(label.Int("workerId", id), label.String("status", "queue_full"))
	w.metrics.enqueueAlreadyPresent = enqueue.Bind(label.Int("workerId", id), label.String("status", "already_present"))

	w.metrics.dequeue = meter.NewInt64Counter("kreconciler_dequeue",
		metric.WithUnit(unit.Dimensionless),
		metric.WithDescription("The number of times an item was remove from the reconcile queue (to be handled)"),
	).Bind(label.Int("workerId", id))

	handleResult := meter.NewInt64Counter("kreconciler_handle_result",
		metric.WithUnit(unit.Dimensionless),
		metric.WithDescription("The outcome of the call to handle"),
	)

	w.metrics.handleResultOk = handleResult.Bind(label.Int("workerId", id), label.String("result", "ok"))
	w.metrics.handleResultDelayRequeue = handleResult.Bind(label.Int("workerId", id), label.String("result", "delay_requeue"))
	w.metrics.handleResultErrorRequeue = handleResult.Bind(label.Int("workerId", id), label.String("result", "error_requeue"))
	w.metrics.handleResultDropMaxTry = handleResult.Bind(label.Int("workerId", id), label.String("result", "drop_max_tries"))

	delay := meter.NewInt64ValueRecorder("kreconciler_requeue_delay_millis",
		metric.WithUnit(unit.Milliseconds),
		metric.WithDescription("How long we are reenqueing item for"),
	)
	w.metrics.delayWithoutError = delay.Bind(label.Int("workerId", id), label.Bool("error", false))
	w.metrics.delayWithError = delay.Bind(label.Int("workerId", id), label.Bool("error", true))

	handleLatency := meter.NewInt64ValueRecorder("kreconciler_handle_millis",
		metric.WithUnit(unit.Milliseconds),
		metric.WithDescription("How long we're taking to process an item"),
	)
	w.metrics.handleLatencySuccess = handleLatency.Bind(label.Int("workerId", id), label.Bool("error", false))
	w.metrics.handleLatencyError = handleLatency.Bind(label.Int("workerId", id), label.Bool("error", true))
	w.metrics.queueTime = meter.NewInt64ValueRecorder("kreconciler_queue_millis",
		metric.WithUnit(unit.Milliseconds),
		metric.WithDescription("How long we spent in the queue"),
	).Bind(label.Int("workerId", id))
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

func NewHandlerWithTimeout(handler Handler, timeout time.Duration) Handler {
	return HandlerFunc(func(ctx context.Context, id string) Result {
		if timeout != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		return handler.Handle(ctx, id)
	})
}

type item struct {
	ctx              context.Context
	tryCount         int
	id               string
	maxTries         int
	firstEnqueueTime time.Time
	lastEnqueueTime  time.Time
}

var QueueAtCapacityError = errors.New("queue at capacity, retry later")

func (w *worker) Enqueue(id string) error {
	ctx, _ := w.Tracer().Start(context.Background(), "reconcile",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(),
		trace.WithAttributes(
			label.String("id", id),
		),
	)

	return w.enqueue(item{ctx: ctx, id: id, maxTries: w.maxTries, firstEnqueueTime: time.Now()})
}

func (w *worker) enqueue(i item) error {
	i.lastEnqueueTime = time.Now()
	parentSpan := trace.SpanFromContext(i.ctx)
	switch w.objectLocks.Take(i.id) {
	case alreadyPresent:
		w.metrics.enqueueAlreadyPresent.Add(i.ctx, 1)
		parentSpan.SetStatus(codes.Ok, "already_present")
		parentSpan.End()
		w.SLog().Debug("Item already present in the queue, ignoring enqueue")
		return nil
	case queueOverflow:
		w.metrics.enqueueFull.Add(i.ctx, 1)
		parentSpan.SetStatus(codes.Error, "queue_full")
		parentSpan.End()
		return QueueAtCapacityError
	default:
		w.metrics.enqueueOk.Add(i.ctx, 1)
		parentSpan.AddEvent("enqueue")
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
		case itm := <-w.queue:
			w.objectLocks.Free(itm.id)
			parentSpan := trace.SpanFromContext(itm.ctx)
			parentSpan.AddEvent("dequeue")
			w.metrics.dequeue.Add(ctx, 1)
			// process the object.
			res := w.handle(itm)
			delay := res.GetRequeueDelay(w.delayQueue.resolution)
			if delay != 0 {
				itm.tryCount += 1
				if itm.maxTries != 0 && itm.tryCount == itm.maxTries {
					parentSpan.SetStatus(codes.Error, "Max try exceeded")
					parentSpan.End()
					w.SLog().Errorw("Max retry exceeded, dropping item", "object_id", itm.id)
					w.metrics.handleResultDropMaxTry.Add(ctx, 1)
				} else {
					if res.Error != nil {
						w.metrics.handleResultErrorRequeue.Add(ctx, 1)
						w.metrics.delayWithError.Record(ctx, delay.Milliseconds())
					} else {
						w.metrics.handleResultDelayRequeue.Add(ctx, 1)
						w.metrics.delayWithoutError.Record(ctx, delay.Milliseconds())
					}
					parentSpan.AddEvent("enqueue_with_delay", trace.WithAttributes(label.Int64("schedule.millis", delay.Milliseconds()), label.Int("try_count", itm.tryCount), label.Int("max_try", itm.maxTries)))
					w.SLog().Debugw("Delay item retry", "object_id", itm.id)
					err := w.delayQueue.schedule(itm, delay)
					if err != nil {
						parentSpan.SetStatus(codes.Error, "Failed enqueuing with delay")
						parentSpan.RecordError(err)
						parentSpan.End()
						w.SLog().Errorw("Error scheduling delay", "error", err)
					}
				}
			} else {
				w.metrics.handleResultOk.Add(ctx, 1)
				w.SLog().Infow("Done")
				parentSpan.SetStatus(codes.Ok, "")
				parentSpan.End()
			}
		}
	}
}

func (w *worker) handle(i item) Result {
	handleCtx, span := w.Tracer().Start(i.ctx, "handle",
		trace.WithAttributes(
			label.String("id", i.id),
		),
	)
	w.SLog().Debugw("Get event for item", "object_id", i.id)
	start := time.Now()
	w.metrics.queueTime.Record(i.ctx, start.Sub(i.lastEnqueueTime).Milliseconds())
	res := w.handler.Handle(handleCtx, i.id)
	// Retry if required based on the result.
	if res.Error != nil {
		span.RecordError(res.Error)
		span.SetStatus(codes.Error, "")
		w.SLog().Warnw("Failed reconcile loop", "object_id", i.id, "error", res.Error)
		w.metrics.handleLatencyError.Record(i.ctx, time.Since(start).Milliseconds())
	} else {
		span.SetStatus(codes.Ok, "")
		w.metrics.handleLatencySuccess.Record(i.ctx, time.Since(start).Milliseconds())
	}
	span.End()
	return res
}

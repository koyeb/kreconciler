package kreconciler

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/asyncint64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/trace"
)

type metrics struct {
	queueSizeObserver     asyncint64.UpDownCounter
	dequeue               syncint64.Counter
	handleResult          syncint64.Counter
	delay                 syncint64.Histogram
	handleLatency         syncint64.Histogram
	enqueue               syncint64.Counter
	enqueueFull           syncint64.Counter
	enqueueAlreadyPresent syncint64.Counter
	queueTime             syncint64.Histogram
}

type worker struct {
	sync.Mutex
	id          int
	queue       chan item
	ticker      *time.Ticker
	delayQueue  *dq
	maxTries    int
	handler     Reconciler
	objectLocks objectLocks
	capacity    int
	metrics     *metrics
	Observability
}

func newWorker(obs Observability, id, capacity, maxTries, delayQueueSize int, delayResolution time.Duration, maxReconcileTime time.Duration, handler Reconciler) (*worker, error) {
	obs.Logger = obs.Logger.With("worker-id", id)
	w := &worker{
		id:            id,
		Observability: obs,
		queue:         make(chan item, capacity+1), // TO handle the inflight item schedule
		capacity:      capacity,
		maxTries:      maxTries,
		metrics:       &metrics{},
		delayQueue:    newQueue(delayQueueSize, delayResolution),
		objectLocks:   newObjectLocks(capacity),
		handler:       newPanicReconciler(obs, newReconcilerWithTimeout(handler, maxReconcileTime)),
	}
	err := decorateMeter(w, obs.Meter)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func attrWorkerId(id int) attribute.KeyValue {
	return attribute.Int("workerId", id)
}

func decorateMeter(w *worker, meter metric.Meter) error {
	queueSizeObserver, err := meter.AsyncInt64().UpDownCounter("kreconciler_worker_queue_size",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of outstanding items to reconcile"),
	)
	if err != nil {
		return err
	}
	w.metrics.queueSizeObserver = queueSizeObserver
	meter.RegisterCallback([]instrument.Asynchronous{w.metrics.queueSizeObserver}, func(ctx context.Context) {
		w.metrics.queueSizeObserver.Observe(ctx, int64(w.objectLocks.Size()), attrWorkerId(w.id))
	})

	enqueue, err := meter.SyncInt64().Counter("kreconciler_enqueue",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of times an item was added to the reconcile queue"),
	)
	if err != nil {
		return err
	}
	w.metrics.enqueue = enqueue

	w.metrics.dequeue, err = meter.SyncInt64().Counter("kreconciler_dequeue",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of times an item was removed from the reconcile queue (to be handled)"),
	)
	if err != nil {
		return err
	}

	w.metrics.handleResult, err = meter.SyncInt64().Counter("kreconciler_handle_result",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The outcome of the call to handle"),
	)
	if err != nil {
		return err
	}

	w.metrics.delay, err = meter.SyncInt64().Histogram("kreconciler_requeue_delay_millis",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("How long we are reenqueing item for"),
	)
	if err != nil {
		return err
	}

	w.metrics.handleLatency, err = meter.SyncInt64().Histogram("kreconciler_handle_millis",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("How long we're taking to process an item"),
	)
	if err != nil {
		return err
	}

	w.metrics.queueTime, err = meter.SyncInt64().Histogram("kreconciler_queue_millis",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("How long we spent in the queue"),
	)
	if err != nil {
		return err
	}

	return nil
}

func newPanicReconciler(obs Observability, delegate Reconciler) Reconciler {
	return ReconcilerFunc(func(ctx context.Context, id string) (r Result) {
		defer func() {
			if err := recover(); err != nil {
				l := obs.LoggerWithCtx(ctx)
				l.Error("Panicked inside an reconciler", "error", err, "stack", string(debug.Stack()))
				span := trace.SpanFromContext(ctx)
				span.AddEvent("panic")
				if e, ok := err.(error); ok {
					r = Result{Error: e}
				} else {
					r = Result{Error: errors.New(err.(string))}
				}
			}
		}()
		r = delegate.Apply(ctx, id)
		return
	})
}

func newReconcilerWithTimeout(delegate Reconciler, timeout time.Duration) Reconciler {
	if timeout == 0 {
		return delegate
	}
	return ReconcilerFunc(func(ctx context.Context, id string) Result {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
		return delegate.Apply(ctx, id)
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

var errQueueAtCapacityError = errors.New("queue at capacity, retry later")

func (w *worker) Enqueue(id string) error {
	ctx, _ := w.Observability.Start(context.Background(), "reconcile",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(),
		trace.WithAttributes(
			attribute.String("id", id),
		),
	)

	return w.enqueue(item{ctx: ctx, id: id, maxTries: w.maxTries, firstEnqueueTime: time.Now()})
}

func (w *worker) enqueue(i item) error {
	i.lastEnqueueTime = time.Now()
	parentSpan := trace.SpanFromContext(i.ctx)
	l := w.Observability.LoggerWithCtx(i.ctx)
	switch w.objectLocks.Take(i.id) {
	case errAlreadyPresent:
		w.metrics.enqueue.Add(i.ctx, 1, attrWorkerId(w.id), attribute.String("status", "already_present"))
		parentSpan.SetStatus(codes.Ok, "already_present")
		parentSpan.End()
		l.Debug("Item already present in the queue, ignoring enqueue", "object_id", i.id)
		return nil
	case errQueueOverflow:
		w.metrics.enqueue.Add(i.ctx, 1, attrWorkerId(w.id), attribute.String("status", "queue_full"))
		parentSpan.SetStatus(codes.Error, "queue_full")
		parentSpan.End()
		return errQueueAtCapacityError
	default:
		w.metrics.enqueue.Add(i.ctx, 1, attrWorkerId(w.id), attribute.String("status", "ok"))
		parentSpan.AddEvent("enqueue")
		w.queue <- i
		return nil
	}
}

func (w *worker) Run(ctx context.Context) {
	w.Info("worker started")
	defer w.Info("worker stopped")
	go w.delayQueue.run(ctx, func(_ time.Time, i interface{}) {
		itm := i.(item)
		l := w.Observability.LoggerWithCtx(ctx)
		l.Debug("Reenqueuing item after delay", "object_id", itm.id)
		err := w.enqueue(itm)
		if err != nil {
			l.Error("Failed reenqueing delayed item", "error", err)
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
			l := w.Observability.LoggerWithCtx(ctx)
			w.metrics.dequeue.Add(ctx, 1, attrWorkerId(w.id))
			// process the object.
			res := w.handle(itm)
			delay := res.RequeueDelayWithDefault(w.delayQueue.resolution)
			if delay != 0 {
				itm.tryCount += 1
				if itm.maxTries != 0 && itm.tryCount == itm.maxTries {
					parentSpan.SetStatus(codes.Error, "Max try exceeded")
					parentSpan.End()
					l.Error("Max retry exceeded, dropping item", "object_id", itm.id)
					w.metrics.handleResult.Add(ctx, 1, attrWorkerId(w.id), attribute.String("result", "drop_max_tries"))
				} else {
					if res.Error != nil {
						w.metrics.handleResult.Add(ctx, 1, attrWorkerId(w.id), attribute.String("result", "error_requeue"))
						w.metrics.delay.Record(ctx, delay.Milliseconds(), attrWorkerId(w.id), attribute.Bool("error", true))
					} else {
						w.metrics.handleResult.Add(ctx, 1, attrWorkerId(w.id), attribute.String("result", "delay_requeue"))
						w.metrics.delay.Record(ctx, delay.Milliseconds(), attrWorkerId(w.id), attribute.Bool("error", false))
					}
					parentSpan.AddEvent("enqueue_with_delay", trace.WithAttributes(attribute.Int64("schedule.millis", delay.Milliseconds()), attribute.Int("try_count", itm.tryCount), attribute.Int("max_try", itm.maxTries)))
					l.Debug("Delay item retry", "object_id", itm.id)
					err := w.delayQueue.schedule(itm, delay)
					if err != nil {
						parentSpan.SetStatus(codes.Error, "Failed enqueuing with delay")
						parentSpan.RecordError(err)
						parentSpan.End()
						l.Error("Error scheduling delay", "error", err)
					}
				}
			} else {
				w.metrics.handleResult.Add(ctx, 1, attrWorkerId(w.id), attribute.String("result", "ok"))
				l.Debug("Done", "object_id", itm.id)
				parentSpan.SetStatus(codes.Ok, "")
				parentSpan.End()
			}
		}
	}
}

func (w *worker) handle(i item) Result {
	handleCtx, span := w.Start(i.ctx, "handle",
		trace.WithAttributes(
			attribute.String("id", i.id),
		),
	)
	defer span.End()
	l := w.Observability.LoggerWithCtx(i.ctx)
	l.Debug("Get event for item", "object_id", i.id)
	start := time.Now()
	w.metrics.queueTime.Record(i.ctx, start.Sub(i.lastEnqueueTime).Milliseconds(), attrWorkerId(w.id))
	res := w.handler.Apply(handleCtx, i.id)
	// Retry if required based on the result.
	if res.Error != nil {
		span.RecordError(res.Error)
		span.SetStatus(codes.Error, "")
		l.Warn("Failed reconcile loop", "object_id", i.id, "error", res.Error)
		w.metrics.handleLatency.Record(i.ctx, time.Since(start).Milliseconds(), attrWorkerId(w.id), attribute.Bool("error", true))
	} else {
		span.SetStatus(codes.Ok, "")
		w.metrics.handleLatency.Record(i.ctx, time.Since(start).Milliseconds(), attrWorkerId(w.id), attribute.Bool("error", false))
	}
	return res
}

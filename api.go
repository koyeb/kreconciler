package kreconciler

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Config use to configure a controller.
type Config struct {
	// MaxItemRetries the number of times an item gets retried before dropping it
	MaxItemRetries int
	// WorkerQueueSize the number of outstanding reconciles a queue holds per worker serving it
	WorkerQueueSize int
	// WorkerCount the number of workers, including the ones reserved in Queues
	WorkerCount int
	// Router picks each item's queue and lock key
	Router Router
	// Queues reserves workers for named queues (name: worker count), the default queue gets the rest of WorkerCount.
	// The queue's name is the `workerName` attribute of its metrics, "default" for the default queue.
	Queues map[string]int
	// LeaderElectionEnabled whether or not we should use
	LeaderElectionEnabled bool
	// DelayResolution the lowest possible time for a delay retry
	DelayResolution time.Duration
	// DelayQueueSize the maximum number of items in the scheduled delay queue, per worker serving it
	DelayQueueSize int
	// MaxReconcileTime the maximum time a handle of an item should take
	MaxReconcileTime time.Duration
	// Observability configuration for logs, metrics and traces
	Observability Observability
}

// DefaultConfig a good set of configuration to get started
func DefaultConfig() Config {
	return Config{
		Observability:         DefaultObservability(),
		Router:                DefaultRouter,
		WorkerCount:           1,
		MaxItemRetries:        10,
		WorkerQueueSize:       2000,
		LeaderElectionEnabled: true,
		DelayResolution:       time.Millisecond * 250,
		DelayQueueSize:        1000,
		MaxReconcileTime:      time.Second * 10,
	}
}

// Reconciler is the core implementation of the control-loop.
type Reconciler interface {
	// Apply handle the item and potentially return an error
	Apply(ctx context.Context, id string) Result
}

// ReconcilerFunc see Reconciler
type ReconcilerFunc func(ctx context.Context, id string) Result

// Apply calls f(ctx, id).
func (f ReconcilerFunc) Apply(ctx context.Context, id string) Result {
	return f(ctx, id)
}

// Result a wrapper that is returned by a Reconciler.
type Result struct {
	// RequeueDelay the time to wait before requeing, ignored is Error is not nil
	RequeueDelay time.Duration
	// Error the error
	Error error
}

// RequeueDelayWithDefault returns the requeue delay and use the default delay if the error is not a Error.
func (r Result) RequeueDelayWithDefault(defaultDelay time.Duration) time.Duration {
	if r.Error != nil {
		er, ok := r.Error.(Error)
		if !ok {
			return defaultDelay
		}
		return er.RetryDelay()
	}
	return r.RequeueDelay
}

// Error an error that has a custom retry delay.
type Error interface {
	error
	// RetryDelay how long to wait before adding back in the queue
	RetryDelay() time.Duration
}

// Route where an item goes. Items wait in one FIFO per queue, and any free worker of the queue takes the oldest.
type Route struct {
	// Queue the queue the item waits in: a name in Config.Queues, else the default queue
	Queue string
	// Key two items with the same key never run at once, even in different queues. An item whose key is busy waits
	// without holding a worker, and runs right after the item holding it, before newer items. Empty is the item's id.
	Key string
	// Drop discard the item
	Drop bool
}

// Router decides where an item goes. It is called on each enqueue, but not on retries, which keep their route.
type Router interface {
	Route(ctx context.Context, id string) (Route, error)
}

// RouterFunc see Router
type RouterFunc func(ctx context.Context, id string) (Route, error)

// Route calls f(ctx, id).
func (f RouterFunc) Route(ctx context.Context, id string) (Route, error) {
	return f(ctx, id)
}

// DefaultRouter a Router which sends every item to the default queue, keyed by its id.
var DefaultRouter = RouterFunc(func(_ context.Context, _ string) (Route, error) {
	return Route{}, nil
})

// EventHandler called whenever an event is triggered
type EventHandler interface {
	Call(ctx context.Context, jobId string) error
}

// EventHandlerFunc see EventHandler
type EventHandlerFunc func(ctx context.Context, jobId string) error

// Call calls f(ctx, jobId).
func (f EventHandlerFunc) Call(ctx context.Context, jobId string) error {
	return f(ctx, jobId)
}

// MeteredEventHandler adds metrics any event reconciler
func MeteredEventHandler(meter metric.Meter, name string, child EventHandler) (EventHandler, error) {
	counter, err := meter.Int64Counter("kreconciler_stream_event_count")
	if err != nil {
		return nil, err
	}

	return EventHandlerFunc(func(ctx context.Context, jobId string) (err error) {
		defer func() {
			attributes := []attribute.KeyValue{attribute.String("stream", name)}

			if err != nil {
				attributes = append(attributes, attribute.Bool("error", true))
			} else {
				attributes = append(attributes, attribute.Bool("error", false))
			}

			counter.Add(ctx, 1, metric.WithAttributes(attributes...))
		}()
		err = child.Call(ctx, jobId)
		return
	}), nil
}

// EventStream calls `reconciler` whenever a new event is triggered.
// Examples of EventStreams are: "KafkaConsumers", "PubSub systems", "Nomad event stream".
// It's usually a way to signal that an external change happened and that we should rerun the control loop for the element with a given id.
type EventStream interface {
	Subscribe(ctx context.Context, handler EventHandler) error
}

// EventStreamFunc see EventStream
type EventStreamFunc func(ctx context.Context, handler EventHandler) error

// Subscribe calls f(ctx, handler)
func (f EventStreamFunc) Subscribe(ctx context.Context, handler EventHandler) error {
	return f(ctx, handler)
}

// NoopStream a stream that does nothing
var NoopStream = EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
	<-ctx.Done()
	return nil
})

// ResyncLoopEventStream an EventStream that calls `listFn` every `duration` interval.
// This is used for rerunning the control-loop for all entities periodically.
// Having one of these is recommended for any controller.
func ResyncLoopEventStream(obs Observability, duration time.Duration, listFn func(ctx context.Context) ([]string, error)) (EventStream, error) {
	count, err := obs.Meter.Int64Counter("kreconciler_stream_resync_item_count",
		metric.WithUnit("{count}"),
		metric.WithDescription("Increased by the number of items returned by the listFn"),
	)
	if err != nil {
		return nil, err
	}

	recorder, err := obs.Meter.Int64Histogram("kreconciler_stream_resync_millis",
		metric.WithUnit("ms"),
		metric.WithDescription("time spent calling the listFn"),
	)
	if err != nil {
		return nil, err
	}

	return EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
		ticker := time.NewTicker(duration)
		for {
			obs.Info("Running step of resync loop")
			start := time.Now()
			// Queue the objects to be handled.
			elts, err := listFn(ctx)
			if err != nil {
				recorder.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attribute.String("status", "error")))
				obs.Error("Failed resync loop call", "error", err)
				select {
				case <-ctx.Done():
					obs.Info("Finished resync loop")
					return nil
				default:
				}
				time.Sleep(time.Millisecond * 250)
				continue
			}
			obs.Info("Adding events", "count", len(elts))
			count.Add(ctx, int64(len(elts)))
			recorder.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attribute.String("status", "success")))
			for _, id := range elts {
				// Listed objects enqueue as present.
				err = handler.Call(ctx, id)
				if err != nil {
					obs.Warn("Failed handle in resync loop", "id", id, "error", err)
				}
			}

			select {
			case <-ctx.Done():
				obs.Info("Finished resync loop")
				return nil
			case <-ticker.C:
			}
		}
	}), nil
}

package kreconciler

import (
	"context"
	"hash/fnv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
)

// Config use to configure a controller.
type Config struct {
	// MaxItemRetries the number of times an item gets retried before dropping it
	MaxItemRetries int
	// WorkerQueueSize the size of the worker queue (outstanding reconciles)
	WorkerQueueSize int
	// WorkerHasher the function to assign work between workers
	WorkerHasher WorkerHasher
	// WorkerCount the number of workers
	WorkerCount int
	// LeaderElectionEnabled whether or not we should use
	LeaderElectionEnabled bool
	// DelayResolution the lowest possible time for a delay retry
	DelayResolution time.Duration
	// DelayQueueSize the maximum number of items in the scheduled delay queue
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
		WorkerHasher:          DefaultHasher,
		WorkerCount:           1,
		MaxItemRetries:        4,
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

// WorkerHasher specifies which of the control-loop workers should handle this specific item.
type WorkerHasher interface {
	// Route decide on which worker this item will go (return a value < 0 to drop this item), count is the number of items
	Route(ctx context.Context, id string, count int) (int, error)
}

// WorkerHasherFunc see WorkerHasher
type WorkerHasherFunc func(ctx context.Context, id string, count int) (int, error)

// Route calls f(ctx, id, count).
func (f WorkerHasherFunc) Route(ctx context.Context, id string, count int) (int, error) {
	return f(ctx, id, count)
}

// DefaultHasher a WorkerHasher which hashes the id and return `hash % count`.
var DefaultHasher = WorkerHasherFunc(func(_ context.Context, id string, count int) (int, error) {
	if count == 1 {
		return 0, nil
	}
	algorithm := fnv.New32a()
	algorithm.Write([]byte(id))
	return int(algorithm.Sum32() % uint32(count)), nil
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
	counter, err := meter.SyncInt64().Counter("kreconciler_stream_event_count")
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

			counter.Add(ctx, 1, attributes...)
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
	count, err := obs.Meter.SyncInt64().Counter("kreconciler_stream_resync_item_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("Increased by the number of items returned by the listFn"),
	)
	if err != nil {
		return nil, err
	}

	recorder, err := obs.Meter.SyncInt64().Histogram("kreconciler_stream_resync_millis",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("time spent calling the listFn"),
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
				recorder.Record(ctx, time.Since(start).Milliseconds(), attribute.String("status", "error"))
				obs.Error("Failed resync loop call", "error", err)
				time.Sleep(time.Millisecond * 250)
				continue
			}
			obs.Info("Adding events", "count", len(elts))
			count.Add(ctx, int64(len(elts)))
			recorder.Record(ctx, time.Since(start).Milliseconds(), attribute.String("status", "success"))
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

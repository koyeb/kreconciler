package reconciler

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/unit"
	"go.uber.org/zap"
	"hash/fnv"
	"time"
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
	Observability    Observability
}

func DefaultConfig() Config {
	l, _ := zap.NewProduction()
	return Config{
		Observability:         NewObservability(l.Sugar(), otel.GetMeterProvider(), otel.GetTracerProvider()),
		WorkerHasher:          DefaultHasher,
		WorkerCount:           1,
		MaxItemRetries:        10,
		WorkerQueueSize:       2000,
		LeaderElectionEnabled: true,
		DelayResolution:       time.Millisecond * 250,
		DelayQueueSize:        1000,
		MaxReconcileTime:      time.Second * 10,
	}
}

// Handler is the core implementation of the control-loop.
type Handler interface {
	// Handle handle the item and potentially return an error
	Handle(ctx context.Context, id string) Result
}

// HandlerFunc see Handler
type HandlerFunc func(ctx context.Context, id string) Result
func (f HandlerFunc) Handle(ctx context.Context, id string) Result {
	return f(ctx, id)
}

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
		} else {
			return er.RetryDelay()
		}
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

// Called whenever an event is triggered
type EventHandler interface {
	Handle(ctx context.Context, jobId string) error
}

// EventHandlerFunc see EventHandler
type EventHandlerFunc func(ctx context.Context, jobId string) error
func (f EventHandlerFunc) Handle(ctx context.Context, jobId string) error {
	return f(ctx, jobId)
}

// MeteredEventHandler adds metrics any event handler
func MeteredEventHandler(meter metric.Meter, name string, child EventHandler) EventHandler {
	counter := metric.Must(meter).NewInt64Counter("kreconciler_stream_event_count")
	errors := counter.Bind(label.Bool("error", true), label.String("stream", name))
	ok := counter.Bind(label.Bool("error", false), label.String("stream", name))
	return EventHandlerFunc(func(ctx context.Context, jobId string) (err error) {
		defer func() {
			if err != nil {
				errors.Add(ctx, 1)
			} else {
				ok.Add(ctx, 1)
			}
		}()
		err = child.Handle(ctx, jobId)
		return
	})
}

// EventStream calls `handler` whenever a new event is triggered.
// Examples of EventStreams are: "KafkaConsumers", "PubSub systems", "Nomad event stream".
// It's usually a way to signal that an external change happened and that we should rerun the control loop for the element with a given id.
type EventStream interface {
	Subscribe(ctx context.Context, handler EventHandler) error
}

// EventStreamFunc see EventStream
type EventStreamFunc func(ctx context.Context, handler EventHandler) error
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
func ResyncLoopEventStream(obs Observability, duration time.Duration, listFn func(ctx context.Context) ([]string, error)) EventStream {
	m := metric.Must(obs.Meter)
	count := m.NewInt64Counter("kreconciler_stream_resync_item_count",
		metric.WithUnit(unit.Dimensionless),
		metric.WithDescription("Increased by the number of items returned by the listFn"),
	)
	recorder := m.NewInt64ValueRecorder("kreconciler_stream_resync_millis",
		metric.WithUnit(unit.Milliseconds),
		metric.WithDescription("time spent calling the listFn"),
	)
	errorRecorder := recorder.Bind(label.String("status", "error"))
	successRecorder := recorder.Bind(label.String("status", "success"))
	return EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
		ticker := time.NewTicker(duration)
		for {
			obs.Info("Running step of resync loop")
			start := time.Now()
			// Queue the objects to be handled.
			elts, err := listFn(ctx)
			if err != nil {
				errorRecorder.Record(ctx, time.Since(start).Milliseconds())
				obs.Errorw("Failed resync loop call", "error", err)
				time.Sleep(time.Millisecond * 250)
				continue
			}
			obs.Infow("Adding events", "count", len(elts))
			count.Add(ctx, int64(len(elts)))
			successRecorder.Record(ctx, time.Since(start).Milliseconds())
			for _, id := range elts {
				// Listed objects enqueue as present.
				err = handler.Handle(ctx, id)
				if err != nil {
					obs.Warnw("Failed handle in resync loop", "id", id, "error", err)
				}
			}

			select {
			case <-ctx.Done():
				obs.Info("Finished resync loop")
				return nil
			case <-ticker.C:
			}
		}
	})
}

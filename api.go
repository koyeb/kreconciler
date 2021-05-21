package reconciler

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"hash/fnv"
	"time"
)

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

type Handler interface {
	// Handle handle the item and potentially return an error
	Handle(ctx context.Context, id string) Result
}

type HandlerFunc func(ctx context.Context, id string) Result

func (f HandlerFunc) Handle(ctx context.Context, id string) Result {
	return f(ctx, id)
}

type WorkerHasher interface {
	// Route decides on which worker this item will go (return a value < 0 to drop this item), count is the number of items
	Route(ctx context.Context, id string, count int) (int, error)
}
type WorkerHasherFunc func(ctx context.Context, id string, count int) (int, error)

func (f WorkerHasherFunc) Route(ctx context.Context, id string, count int) (int, error) {
	return f(ctx, id, count)
}

var DefaultHasher = WorkerHasherFunc(func(_ context.Context, id string, count int) (int, error) {
	if count == 1 {
		return 0, nil
	}
	algorithm := fnv.New32a()
	algorithm.Write([]byte(id))
	return int(algorithm.Sum32() % uint32(count)), nil
})

type Result struct {
	// RequeueDelay the time to wait before requeing, ignored is Error is not nil
	RequeueDelay time.Duration
	// Error the error
	Error error
}

func (r Result) GetRequeueDelay(defaultDelay time.Duration) time.Duration {
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

type Error interface {
	error
	// RetryDelay how long to wait before adding back in the queue
	RetryDelay() time.Duration
}

type EventHandlerFunc func(ctx context.Context, jobId string) error

func (f EventHandlerFunc) Handle(ctx context.Context, jobId string) error {
	return f(ctx, jobId)
}

// Called whenever an event is triggered
type EventHandler interface {
	Handle(ctx context.Context, jobId string) error
}

type EventStream interface {
	Subscribe(ctx context.Context, handler EventHandler) error
}
type EventStreamFunc func(ctx context.Context, handler EventHandler) error

func (f EventStreamFunc) Subscribe(ctx context.Context, handler EventHandler) error {
	return f(ctx, handler)
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

var NoopStream = EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
	<-ctx.Done()
	return nil
})

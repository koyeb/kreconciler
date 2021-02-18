package reconciler

import (
	"context"
	"hash/fnv"
	"time"
)

type Config struct {
	MaxItemRetries  int
	WorkerQueueSize int
	WorkerHasher    WorkerHasher
}

func DefaultConfig() Config {
	return Config{
		WorkerHasher:    DefaultHasher{Num: 1},
		MaxItemRetries:  10,
		WorkerQueueSize: 1000,
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
	// Route decides on which worker this item will go
	Route(id string) int
	// Count gives the total number of workers
	Count() int
}

type DefaultHasher struct {
	Num uint32
}

func (d DefaultHasher) Count() int {
	return int(d.Num)
}

func (d DefaultHasher) Route(id string) int {
	if d.Num == 1 {
		return 0
	}
	algorithm := fnv.New32a()
	algorithm.Write([]byte(id))
	return int(algorithm.Sum32() % d.Num)
}

type Result struct {
	// RequeueDelay the time to wait before requeing, ignored is Error is not nil
	RequeueDelay time.Duration
	// Error the error
	Error error
}

func (r Result) GetRequeueDelay() time.Duration {
	if r.Error != nil {
		er, ok := r.Error.(Error)
		if !ok {
			return time.Millisecond * 100
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

type EventHandlerFunc func(jobId string) error

func (f EventHandlerFunc) Handle(jobId string) error {
	return f(jobId)
}

// Called whenever an event is triggered
type EventHandler interface {
	Handle(jobId string) error
}

type EventStream interface {
	Subscribe(ctx context.Context, handler EventHandler) error
}
type EventStreamFunc func(ctx context.Context, handler EventHandler) error

func (f EventStreamFunc) Subscribe(ctx context.Context, handler EventHandler) error {
	return f(ctx, handler)
}

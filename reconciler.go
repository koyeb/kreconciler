package reconciler

import (
	"context"
	"github.com/koyeb/api.koyeb.com/internal/pkg/observability"
	"sync"
	"time"
)

type Controller interface {
	Run(ctx context.Context) error
	BecomeLeader()
}

type controller struct {
	observability.Wrapper
	cfg             Config
	workers         []*worker
	handler         Handler
	eventStreams    map[string]EventStream
	streamWaitGroup sync.WaitGroup
	workerWaitGroup sync.WaitGroup
	isLeader        chan struct{}
}

func (c *controller) BecomeLeader() {
	c.SLog().Infow("Signaling we're becoming leader")
	c.isLeader <- struct{}{}
}

func New(obs observability.Wrapper, config Config, handler Handler, streams map[string]EventStream) Controller {
	return &controller{
		Wrapper:      obs.NewChildWrapper("reconciler"),
		cfg:          config,
		handler:      handler,
		eventStreams: streams,
		isLeader:     make(chan struct{}, 1),
	}
}

func (c *controller) Run(ctx context.Context) error {
	if !c.cfg.LeaderElectionEnabled {
		c.isLeader <- struct{}{}
	}
	// Run workers.
	workersCtx, cancelWorkers := context.WithCancel(ctx)
	for i := 0; i < c.cfg.WorkerHasher.Count(); i++ {
		worker := newWorker(c, i, c.cfg.WorkerQueueSize, c.cfg.MaxItemRetries, c.cfg.DelayQueueSize, c.cfg.DelayResolution, c.handler)
		c.workers = append(c.workers, worker)
		go func() {
			c.workerWaitGroup.Add(1)
			defer c.workerWaitGroup.Done()
			worker.Run(workersCtx)
		}()
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	// Run streams subscribers
	c.SLog().Infow("Wait to become leader")
	select {
	case <-ctx.Done():
		c.SLog().Info("Context terminated without ever being leader, never start streams.")
	case <-c.isLeader:
		c.SLog().Infow("Became leader, starting reconciler")
		for name, stream := range c.eventStreams {
			stream := stream
			n := name
			go func() {
				c.streamWaitGroup.Add(1)
				defer c.streamWaitGroup.Done()
				err := stream.Subscribe(streamCtx, MeteredEventHandler(c.Meter(), n, EventHandlerFunc(c.enqueue)))
				if err != nil {
					c.SLog().Errorw("Failed subscribing to stream", "error", err)
				}
			}()
		}
		// Wait until it's finished
		<-ctx.Done()
		c.SLog().Infow("Context terminated after being a leader")
	}

	c.SLog().Infow("stopping controller...")
	c.SLog().Infow("stopping streams...")
	cancelStream()
	c.streamWaitGroup.Wait()
	c.SLog().Infow("stopped streams...")
	c.SLog().Infow("stopping workers...")
	cancelWorkers()
	c.workerWaitGroup.Wait()
	c.SLog().Infow("stopped workers...")
	c.SLog().Infow("stopped controller...")
	return nil
}

func (c *controller) enqueue(id string) error {
	// Simply discard items with empty ids
	if id == "" {
		return nil
	}
	workerId := c.cfg.WorkerHasher.Route(id)
	return c.workers[workerId].Enqueue(id)
}

func ResyncLoopEventStream(obs observability.Wrapper, duration time.Duration, listFn func(ctx context.Context) ([]string, error)) EventStream {
	return EventStreamFunc(func(ctx context.Context, handler EventHandler) error {
		ticker := time.NewTicker(duration)
		for {
			obs.SLog().Info("Running step of resync loop")
			// Queue the objects to be handled.
			elts, err := listFn(ctx)
			if err != nil {
				obs.SLog().Errorw("Failed resync loop call", "error", err)
				time.Sleep(time.Millisecond * 250)
				continue
			}
			obs.SLog().Infow("Adding events", "count", len(elts))
			for _, id := range elts {
				// Listed objects enqueue as present.
				err = handler.Handle(id)
				if err != nil {
					obs.SLog().Warnw("Failed handle in resync loop", "id", id, "error", err)
				}
			}

			select {
			case <-ctx.Done():
				obs.SLog().Info("Finished resync loop")
				return nil
			case <-ticker.C:
			}
		}
	})
}

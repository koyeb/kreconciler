package reconciler

import (
	"context"
	"sync"
)

type Controller interface {
	Run(ctx context.Context) error
	BecomeLeader()
}

type controller struct {
	Observability
	cfg             Config
	workers         []*worker
	handler         Handler
	eventStreams    map[string]EventStream
	streamWaitGroup sync.WaitGroup
	workerWaitGroup sync.WaitGroup
	isLeader        chan struct{}
}

func (c *controller) BecomeLeader() {
	c.Infow("Signaling we're becoming leader")
	c.isLeader <- struct{}{}
}

func New(config Config, handler Handler, streams map[string]EventStream) Controller {
	return &controller{
		Observability: config.Observability,
		cfg:           config,
		handler:       handler,
		eventStreams:  streams,
		isLeader:      make(chan struct{}, 1),
	}
}

func (c *controller) Run(ctx context.Context) error {
	if !c.cfg.LeaderElectionEnabled {
		c.isLeader <- struct{}{}
	}
	// Run workers.
	workersCtx, cancelWorkers := context.WithCancel(ctx)
	for i := 0; i < c.cfg.WorkerCount; i++ {
		worker := newWorker(c.Observability, i, c.cfg.WorkerQueueSize, c.cfg.MaxItemRetries, c.cfg.DelayQueueSize, c.cfg.DelayResolution, c.cfg.MaxReconcileTime, c.handler)
		c.workers = append(c.workers, worker)
		go func() {
			c.workerWaitGroup.Add(1)
			defer c.workerWaitGroup.Done()
			worker.Run(workersCtx)
		}()
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	// Run streams subscribers
	c.Infow("Wait to become leader")
	select {
	case <-ctx.Done():
		c.Info("Context terminated without ever being leader, never start streams.")
	case <-c.isLeader:
		c.Infow("Became leader, starting reconciler")
		for name, stream := range c.eventStreams {
			stream := stream
			n := name
			go func() {
				c.streamWaitGroup.Add(1)
				defer c.streamWaitGroup.Done()
				err := stream.Subscribe(streamCtx, MeteredEventHandler(c.Observability.Meter, n, EventHandlerFunc(c.enqueue)))
				if err != nil {
					c.Errorw("Failed subscribing to stream", "error", err)
				}
			}()
		}
		// Wait until it's finished
		<-ctx.Done()
		c.Infow("Context terminated after being a leader")
	}

	c.Infow("stopping controller...")
	c.Infow("stopping streams...")
	cancelStream()
	c.streamWaitGroup.Wait()
	c.Infow("stopped streams...")
	c.Infow("stopping workers...")
	cancelWorkers()
	c.workerWaitGroup.Wait()
	c.Infow("stopped workers...")
	c.Infow("stopped controller...")
	return nil
}

func (c *controller) enqueue(ctx context.Context, id string) error {
	// Simply discard items with empty ids
	if id == "" {
		return nil
	}
	workerId, err := c.cfg.WorkerHasher.Route(ctx, id, c.cfg.WorkerCount)
	if err != nil {
		return err
	}
	if workerId < 0 {
		c.Debugw("Dropping item", "id", id)
		return nil
	}
	return c.workers[workerId].Enqueue(id)
}

package kreconciler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
)

// Controller the core interface to define a control-loop
type Controller interface {
	// Run execute the control-loop until the context is cancelled
	Run(ctx context.Context) error
	// BecomeLeader notify that this controller is now leader and that it should start the control-loop
	BecomeLeader()
}

type controller struct {
	Observability
	cfg             Config
	queues          map[string]*worker
	reconciler      Reconciler
	eventStreams    map[string]EventStream
	streamWaitGroup sync.WaitGroup
	workerWaitGroup sync.WaitGroup
	isLeader        chan struct{}
}

func (c *controller) BecomeLeader() {
	c.Info("Signaling we're becoming leader")
	c.isLeader <- struct{}{}
}

// New create a new controller
func New(config Config, reconciler Reconciler, streams map[string]EventStream) Controller {
	return &controller{
		Observability: config.Observability,
		cfg:           config,
		reconciler:    reconciler,
		eventStreams:  streams,
		isLeader:      make(chan struct{}, 1),
	}
}

func (c *controller) Run(ctx context.Context) error {
	if !c.cfg.LeaderElectionEnabled {
		c.isLeader <- struct{}{}
	}
	counts, err := c.workerCounts()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	// Run workers.
	workersCtx, cancelWorkers := context.WithCancel(ctx)
	keyLocks := newKeyLocks()
	c.queues = make(map[string]*worker, len(counts))
	firstId := 0
	for _, name := range names {
		n := counts[name]
		worker, err := newWorker(c.Observability, firstId, n, name, c.cfg.WorkerQueueSize*n, c.cfg.MaxItemRetries, c.cfg.DelayQueueSize*n, c.cfg.DelayResolution, c.cfg.MaxReconcileTime, c.reconciler, keyLocks)
		if err != nil {
			cancelWorkers()
			return err
		}
		firstId += n

		c.queues[name] = worker
		c.workerWaitGroup.Add(1)
		go func() {
			defer c.workerWaitGroup.Done()
			worker.Run(workersCtx)
		}()
	}

	// Handle SIGHUP
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sig:
				c.dumpWorkersDelayQueue()
			}
		}
	}()

	errChan := make(chan error, 1)
	streamCtx, cancelStream := context.WithCancel(ctx)
	// Run streams subscribers
	c.Info("Wait to become leader")

	defer func() {
		c.Info("stopping controller...")
		c.Info("stopping streams...")
		cancelStream()
		c.streamWaitGroup.Wait()
		c.Info("stopped streams...")
		c.Info("stopping workers...")
		c.dumpWorkersDelayQueue()
		cancelWorkers()
		c.workerWaitGroup.Wait()
		c.Info("stopped workers...")
		c.Info("stopped controller...")
		close(errChan)
	}()

	select {
	case <-ctx.Done():
		c.Info("Context terminated without ever being leader, never start streams.")
	case <-c.isLeader:
		c.Info("Became leader, starting reconciler")
		for name, stream := range c.eventStreams {
			stream := stream
			n := name
			c.streamWaitGroup.Add(1)
			go func() {
				defer c.streamWaitGroup.Done()

				eventHandler, err := MeteredEventHandler(c.Observability.Meter, n, EventHandlerFunc(c.enqueue))
				if err != nil {
					c.Error("Failed creating MeteredEventHandlersubscribing to stream", "error", err, "stream", n)
					errChan <- err
					return
				}

				err = stream.Subscribe(streamCtx, eventHandler)
				if err != nil {
					c.Error("Failed subscribing to stream", "error", err, "stream", n)
					errChan <- err
				}
			}()
		}
		// Wait until it's finished
		select {
		case <-ctx.Done():
			c.Info("Context terminated after being a leader")
		case err := <-errChan:
			return err
		}
	}

	return nil
}

const defaultQueue = "default"

// workerCounts the number of workers of each queue.
func (c *controller) workerCounts() (map[string]int, error) {
	counts := map[string]int{defaultQueue: c.cfg.WorkerCount}
	for name, n := range c.cfg.Queues {
		if name == defaultQueue {
			return nil, fmt.Errorf("queue name %q is reserved", name)
		}
		if n <= 0 {
			return nil, fmt.Errorf("queue %q needs a positive worker count, got %d", name, n)
		}
		counts[name] = n
		counts[defaultQueue] -= n
	}
	if counts[defaultQueue] <= 0 {
		return nil, fmt.Errorf("no worker left for the default queue: WorkerCount %d, %d reserved in Queues", c.cfg.WorkerCount, c.cfg.WorkerCount-counts[defaultQueue])
	}
	return counts, nil
}

func (c *controller) enqueue(ctx context.Context, id string) error {
	// Simply discard items with empty ids
	if id == "" {
		return nil
	}
	route, err := c.cfg.Router.Route(ctx, id)
	if err != nil {
		return err
	}
	if route.Drop {
		c.Debug("Dropping item", "id", id)
		return nil
	}
	w, ok := c.queues[route.Queue]
	if !ok {
		w = c.queues[defaultQueue]
	}
	if route.Key == "" {
		route.Key = id
	}
	return w.Enqueue(id, route.Key)
}

func (c *controller) dumpWorkersDelayQueue() {
	for _, w := range c.queues {
		w.dumpDelayQueue()
	}
}

package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/koyeb/kreconciler"
)

func main() {
	// Run this for 1 min
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg := kreconciler.DefaultConfig()
	cfg.Observability.Logger = stdOutLogger{}

	// Disable leader election as this is just an example.
	cfg.LeaderElectionEnabled = false

	// This is just a set of fixed items to show case what can be done
	allItems := []string{"a", "b", "c", "d"}

	resync, err := kreconciler.ResyncLoopEventStream(cfg.Observability, time.Second*5, func(ctx context.Context) ([]string, error) {
		return allItems, nil
	})
	if err != nil {
		fmt.Printf("Could not initialize resync loop err='%v'", err)
		return
	}

	err = kreconciler.New(cfg, kreconciler.ReconcilerFunc(func(ctx context.Context, id string) kreconciler.Result {
		cfg.Observability.Info("Got reconcile call", "id", id)
		return kreconciler.Result{}
	}), map[string]kreconciler.EventStream{
		"resync":   resync,
		"random5s": tickEventStream(5*time.Second, allItems),
		"random1s": tickEventStream(time.Second, allItems),
	}).Run(ctx)
	fmt.Printf("Finished running err='%v'", err)
}

func tickEventStream(duration time.Duration, items []string) kreconciler.EventStream {
	return kreconciler.EventStreamFunc(func(ctx context.Context, handler kreconciler.EventHandler) error {
		t := time.Tick(duration)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t:
				handler.Call(ctx, items[rand.Int()%len(items)])
			}
		}
	})
}

type stdOutLogger struct {
	elts string
}

func (s stdOutLogger) With(keyValues ...interface{}) kreconciler.Logger {
	return stdOutLogger{elts: kv(s.elts, keyValues)}
}

func (s stdOutLogger) Debug(msg string, keyValues ...interface{}) {
	fmt.Printf("DEBUG %s %s\n", msg, kv("", keyValues))
}

func (s stdOutLogger) Info(msg string, keyValues ...interface{}) {
	fmt.Printf("INFO %s %s\n", msg, kv("", keyValues))
}

func (s stdOutLogger) Warn(msg string, keyValues ...interface{}) {
	fmt.Printf("WARN %s %s\n", msg, kv("", keyValues))
}

func (s stdOutLogger) Error(msg string, keyValues ...interface{}) {
	fmt.Printf("ERROR %s %s\n", msg, kv("", keyValues))
}

func kv(cur string, keyValues []interface{}) string {
	all := []string{}
	if cur != "" {
		all = append(all, cur)
	}
	for i := 0; i < len(keyValues); i += 2 {
		all = append(all, fmt.Sprintf("%s='%v'", keyValues[i], keyValues[i+1]))
	}
	return strings.Join(all, ",")
}

# kreconciler

[![Go Report Card](https://goreportcard.com/badge/github.com/koyeb/kreconciler)](https://goreportcard.com/report/github.com/koyeb/kreconciler)
[![Go Reference](https://pkg.go.dev/badge/github.com/koyeb/kreconciler/v2.svg)](https://pkg.go.dev/github.com/koyeb/kreconciler/v2)
[![Release](https://img.shields.io/github/release/koyeb/kreconciler.svg?style=flat-square)](https://github.com/koyeb/kreconciler/releases/latest)

A library to build control-loops for things other than Kubernetes.

## Principle

[Kubernetes operators](https://www.infoq.com/articles/kubernetes-operators-in-depth/) are amazing for building
reliable operational tooling.

Unfortunately as its name points out it is specific to Kubernetes.
This library brings a simple way to build `reconcilers` which is the core of an operator.
It runs a loop that for each event coming in will trigger the control-loop.

Its core goals are:

1. Remain simple, caching, resync are not meant to be builtins because they are hard to be generic.
2. Observability is important so it's instrumented with [opentelemetry](https://opentelemetry.io/).
3. Keep the number of dependencies low.

The reason why 2 is so important is that testing an operator is incredibly complicated.
It's therefore necessary to make execution of the reconciler as observable as possible to be able to figure out issues.

## Concurrency

Items wait in a FIFO queue, and any free worker takes the oldest one. `Config.Router` gives each item a key (its id
by default): two items with the same key never run at once, and an item whose key is busy waits without holding a
worker. Use a parent id as the key to serialize the items of a parent. `Config.Queues` reserves workers for named
queues, so that one kind of item never waits behind another.

## Who uses it

At Koyeb we've built reconcilers that helps keeping our models in sync with [Hashicorp Nomad](https://www.nomadproject.io/) and [Kuma](kuma.io/).


## Contributing

See [Contributing](CONTRIBUTING.md).

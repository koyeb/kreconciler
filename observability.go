package kreconciler

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Observability holds everything needed for instrumenting the reconciler code
type Observability struct {
	Logger
	metric.Meter
	trace.Tracer
}

// DefaultObservability uses noopLogger and otel.GetMeter and otel.GetTracer
func DefaultObservability() Observability {
	return NewObservability(NoopLogger{}, otel.GetMeterProvider(), otel.GetTracerProvider())
}

// LoggerWithCtx add the tracing context to the logger
func (o Observability) LoggerWithCtx(ctx context.Context) Logger {
	spanCtx := trace.SpanContextFromContext(ctx)
	return o.Logger.With("spanId", spanCtx.SpanID().String(), "traceId", spanCtx.TraceID().String())
}

// NewObservability create a new observability wraooer (usually easier to use DefaultObservability)
func NewObservability(l Logger, m metric.MeterProvider, t trace.TracerProvider) Observability {
	return Observability{Logger: l, Meter: m.Meter("kreconciler"), Tracer: t.Tracer("kreconciler")}
}

// Logger a wrapper for your logger implementation
type Logger interface {
	// With create a new logger with fixed keys
	With(keyValues ...interface{}) Logger
	// Debug log at debug level
	Debug(msg string, keyValues ...interface{})
	// Info log at debug level
	Info(msg string, keyValues ...interface{})
	// Warn log at debug level
	Warn(msg string, keyValues ...interface{})
	// Error log at debug level
	Error(msg string, keyValues ...interface{})
}

// NoopLogger a logger that does nothing
type NoopLogger struct{}

// With return self
func (n NoopLogger) With(keyValues ...interface{}) Logger {
	return n
}

// Debug noop
func (n NoopLogger) Debug(msg string, keyValues ...interface{}) {
}

// Info noop
func (n NoopLogger) Info(msg string, keyValues ...interface{}) {
}

// Warn noop
func (n NoopLogger) Warn(msg string, keyValues ...interface{}) {
}

// Error noop
func (n NoopLogger) Error(msg string, keyValues ...interface{}) {
}

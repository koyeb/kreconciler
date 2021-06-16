package reconciler

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
var DefaultObservability = NewObservability(NoopLogger{}, otel.GetMeterProvider(), otel.GetTracerProvider())

// LoggerWithCtx add the tracing context to the logger
func (o Observability) LoggerWithCtx(ctx context.Context) Logger {
	spanCtx := trace.SpanContextFromContext(ctx)
	return o.Logger.With("spanId", spanCtx.SpanID.String(), "traceId", spanCtx.TraceID.String())
}

func NewObservability(l Logger, m metric.MeterProvider, t trace.TracerProvider) Observability {
	return Observability{Logger: l, Meter: m.Meter("kreconciler"), Tracer: t.Tracer("kreconciler")}
}


type Logger interface {
	With(keyValues ...interface{}) Logger
	Debug(msg string, keyValues ...interface{})
	Info(msg string, keyValues ...interface{})
	Warn(msg string, keyValues ...interface{})
	Error(msg string, keyValues ...interface{})
}

type NoopLogger struct {}
func (n NoopLogger) With(keyValues ...interface{}) Logger {
	return n
}

func (n NoopLogger) Debug(msg string, keyValues ...interface{}) {
}

func (n NoopLogger) Info(msg string, keyValues ...interface{}) {
}

func (n NoopLogger) Warn(msg string, keyValues ...interface{}) {
}

func (n NoopLogger) Error(msg string, keyValues ...interface{}) {
}


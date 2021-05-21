package reconciler

import (
	"context"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Observability holds everything needed for instrumenting the reconciler code
type Observability struct {
	*zap.SugaredLogger
	metric.Meter
	trace.Tracer
}

// LoggerWithCtx add the tracing context to the logger
func (o Observability) LoggerWithCtx(ctx context.Context) *zap.SugaredLogger {
	spanCtx := trace.SpanContextFromContext(ctx)
	return o.SugaredLogger.With(zap.String("spanId", spanCtx.SpanID.String()), zap.String("traceId", spanCtx.TraceID.String()))
}

func NewObservability(l *zap.SugaredLogger, m metric.MeterProvider, t trace.TracerProvider) Observability {
	return Observability{SugaredLogger: l, Meter: m.Meter("kreconciler"), Tracer: t.Tracer("kreconciler")}
}

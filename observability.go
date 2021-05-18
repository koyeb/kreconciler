package reconciler

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func DecorateLogger(ctx context.Context, logger *zap.SugaredLogger) *zap.SugaredLogger {
	spanCtx := trace.SpanContextFromContext(ctx)
	return logger.With(zap.String("spanId", spanCtx.SpanID.String()), zap.String("traceId", spanCtx.TraceID.String()))
}

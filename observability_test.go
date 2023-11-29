package kreconciler

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type obsTest struct {
	log    Logger
	sr     *tracetest.SpanRecorder
	reader *metric.ManualReader
}

func (o *obsTest) SpanRecorder() *tracetest.SpanRecorder {
	return o.sr
}

func (o *obsTest) MetricReader() *metric.ManualReader {
	return o.reader
}

func (o *obsTest) Observability() Observability {
	o.reader = metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(o.reader))
	return Observability{
		Logger: o.log,
		Meter:  meterProvider.Meter("test"),
		Tracer: trace.NewTracerProvider(trace.WithSpanProcessor(o.sr)).Tracer("test"),
	}
}

type testLog struct {
	t    *testing.T
	args []string
}

func (l testLog) With(kv ...interface{}) Logger {
	args := []string{}
	for _, v := range l.args {
		args = append(args, v)
	}
	for i := 0; i < len(kv); i += 2 {
		args = append(args, fmt.Sprintf("%s=%v", kv[i], kv[i+1]))
	}

	return testLog{
		t:    l.t,
		args: args,
	}
}

func (l testLog) Info(msg string, kv ...interface{}) {
	l.t.Helper()
	l.t.Log("INFO", msg, kv)
}

func (l testLog) Debug(msg string, kv ...interface{}) {
	l.t.Helper()
	l.t.Log("DEBUG", msg, kv)
}

func (l testLog) Error(msg string, kv ...interface{}) {
	l.t.Helper()
	l.t.Log("ERROR", msg, kv)
}

func (l testLog) Warn(msg string, kv ...interface{}) {
	l.t.Helper()
	l.t.Log("WARN", msg, kv)
}

func obsForTest(t *testing.T) obsTest {
	sr := new(tracetest.SpanRecorder)
	return obsTest{
		log: testLog{t: t},
		sr:  sr,
	}
}

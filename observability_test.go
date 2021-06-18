package kreconciler

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/exporters/stdout"
	"go.opentelemetry.io/otel/oteltest"
	"go.opentelemetry.io/otel/sdk/metric/controller/basic"
	otelcontroller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"testing"
	"time"
)

type obsTest struct {
	log   Logger
	sr    *oteltest.StandardSpanRecorder
	contr *otelcontroller.Controller
}

func (o obsTest) SpanRecorder() *oteltest.StandardSpanRecorder {
	return o.sr
}

func (o obsTest) Observability() Observability {
	return Observability{
		Logger: o.log,
		Meter:  o.contr.MeterProvider().Meter("test"),
		Tracer: oteltest.NewTracerProvider(oteltest.WithSpanRecorder(o.sr)).Tracer("test"),
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
	l.t.Log("INFO", msg)
}
func (l testLog) Debug(msg string, kv ...interface{}) {
	l.t.Log("DEBUG", msg)
}
func (l testLog) Error(msg string, kv ...interface{}) {
	l.t.Log("ERROR", msg)
}
func (l testLog) Warn(msg string, kv ...interface{}) {
	l.t.Log("WARN", msg)
}

func obsForTest(t *testing.T) obsTest {
	sr := new(oteltest.StandardSpanRecorder)
	buf1 := bytes.Buffer{}
	contr, err := stdout.InstallNewPipeline([]stdout.Option{
		stdout.WithPrettyPrint(),
		stdout.WithWriter(&buf1),
	}, []basic.Option{
		basic.WithCollectPeriod(time.Second * 5),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		contr.Stop(context.Background())
	})
	return obsTest{
		log:   testLog{t: t},
		sr:    sr,
		contr: contr,
	}
}

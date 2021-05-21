package reconciler

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/exporters/stdout"
	"go.opentelemetry.io/otel/oteltest"
	"go.opentelemetry.io/otel/sdk/metric/controller/basic"
	otelcontroller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"testing"
	"time"
)

type obsTest struct {
	log   *zap.SugaredLogger
	sr    *oteltest.StandardSpanRecorder
	contr *otelcontroller.Controller
}

func (o obsTest) SpanRecorder() *oteltest.StandardSpanRecorder {
	return o.sr
}

func (o obsTest) Observability() Observability {
	return Observability{
		SugaredLogger: o.log,
		Meter:         o.contr.MeterProvider().Meter("test"),
		Tracer:        oteltest.NewTracerProvider(oteltest.WithSpanRecorder(o.sr)).Tracer("test"),
	}
}

func obsForTest(t *testing.T) obsTest {
	log := zaptest.NewLogger(t).Named("test")
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
		log.Sync()
		contr.Stop(context.Background())
	})
	return obsTest{
		log:   log.Sugar(),
		sr:    sr,
		contr: contr,
	}
}

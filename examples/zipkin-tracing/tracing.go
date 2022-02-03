package main

import (
	"fmt"

	"contrib.go.opencensus.io/exporter/zipkin"
	openzipkin "github.com/openzipkin/zipkin-go"
	zipkinHTTP "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/trace"
)

type TracingConfig struct {
	TracingSampleRate float64
	ZipKinServiceName string
	ZipkinServerAddr  string
}

func (c *TracingConfig) flags() []cli.Flag {
	return []cli.Flag{
		&cli.Float64Flag{
			Name:        "tracing-sample-rate",
			EnvVars:     []string{"TRACING_SAMPLE_RATE"},
			Destination: &c.TracingSampleRate,
			Value:       1,
		},
		&cli.StringFlag{
			Name:        "zipkin-service-name",
			EnvVars:     []string{"ZIPKIN_SERVICE_NAME"},
			Destination: &c.ZipKinServiceName,
		},
		&cli.StringFlag{
			Name:        "zipkin-server-addr",
			EnvVars:     []string{"ZIPKIN_SERVER_ADDR"},
			Destination: &c.ZipkinServerAddr,
		},
	}
}

func (c TracingConfig) Apply() error {
	// Global
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.ProbabilitySampler(c.TracingSampleRate)})

	// Zipkin
	localEndpoint, err := openzipkin.NewEndpoint(c.ZipKinServiceName, "")
	if err != nil {
		return fmt.Errorf("Failed to create the local zipkinEndpoint: %w", err)
	}
	reporter := zipkinHTTP.NewReporter(c.ZipkinServerAddr)
	ze := zipkin.NewExporter(reporter, localEndpoint)
	trace.RegisterExporter(ze)

	return nil
}

package main

import (
	"fmt"

	"contrib.go.opencensus.io/exporter/prometheus"
	tract "github.com/23caterpie/Tract"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
)

type MetricsConfig struct {
	PrometheusServiceName string
}

func (c *MetricsConfig) flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "prometheus-service-name",
			EnvVars:     []string{"PROMETHEUS_SERVICE_NAME"},
			Destination: &c.PrometheusServiceName,
		},
	}
}

func (c MetricsConfig) Apply() (*prometheus.Exporter, error) {
	// Global
	err := tract.RegisterDefaultViews()
	if err != nil {
		return nil, fmt.Errorf("error registering tract default views: %w", err)
	}

	// Prometheus
	prometheusExporter, err := prometheus.NewExporter(prometheus.Options{Namespace: c.PrometheusServiceName})
	if err != nil {
		return nil, fmt.Errorf("unable to get prometheus exporter: %w", err)
	}
	view.RegisterExporter(prometheusExporter)

	return prometheusExporter, nil
}

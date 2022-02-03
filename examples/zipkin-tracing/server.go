package main

import (
	"net/http"
	"net/http/pprof"

	"github.com/urfave/cli/v2"
)

type ServerConfig struct {
	Addr string
}

type ServiceServerConfig struct {
	ServerConfig
}

func (c *ServiceServerConfig) flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "sevice-server-addr",
			EnvVars:     []string{"SERVICE_SERVER_ADDR"},
			Destination: &c.ServerConfig.Addr,
		},
	}
}

func (c ServiceServerConfig) GetServer(metricsHandler http.Handler) *http.Server {
	mux := http.NewServeMux()
	// liveness
	mux.HandleFunc("/healthcheck", func(http.ResponseWriter, *http.Request) {})
	// metrics
	if metricsHandler != nil {
		mux.Handle("/metrics", metricsHandler)
	}
	// pprof
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	server := &http.Server{
		Addr:    c.Addr,
		Handler: mux,
	}
	return server
}

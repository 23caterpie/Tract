package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"golang.org/x/sys/unix"

	tract "github.com/23caterpie/Tract"
	"github.com/23caterpie/Tract/urfavtract"

	"github.com/urfave/cli/v2"
)

func main() {
	runner := newRunner()
	app := cli.App{
		Name:   "example-zipkin-tracing",
		Action: cli.ActionFunc(runner.action),
		Flags:  runner.flags(),
	}

	if err := app.Run(os.Args); err != nil {
		log.Println("Exited with error:", err.Error())
		os.Exit(1)
	}
	fmt.Println("DONE")
}

func newRunner() *runner {
	return &runner{
		kafkaConsumerConfig: newKafkaConsumerConfig(),
		parseWorkerConfig:   urfavtract.NewWorkerTractConfig("parse"),
		sqrtWorkerConfig:    urfavtract.NewWorkerTractConfig("sqrt"),
		writeWorkerConfig:   urfavtract.NewWorkerTractConfig("write"),
	}
}

type runner struct {
	kafkaConsumerConfig kafkaConsumerConfig
	parseWorkerConfig   urfavtract.WorkerTractConfig
	sqrtWorkerConfig    urfavtract.WorkerTractConfig
	writeWorkerConfig   urfavtract.WorkerTractConfig
	tracingConfig       TracingConfig
	metricsConfig       MetricsConfig
	serviceServerConfig ServiceServerConfig
}

func (r *runner) flags() []cli.Flag {
	flagLists := [][]cli.Flag{
		r.kafkaConsumerConfig.flags(),
		r.parseWorkerConfig.Flags(),
		r.sqrtWorkerConfig.Flags(),
		r.writeWorkerConfig.Flags(),
		r.tracingConfig.flags(),
		r.metricsConfig.flags(),
		r.serviceServerConfig.flags(),
	}
	flags := make([]cli.Flag, 0)
	for _, flagList := range flagLists {
		flags = append(flags, flagList...)
	}
	return flags
}

func (r *runner) action(*cli.Context) error {
	fmt.Printf("config: %+#v\n", r)

	// Setup Tracing
	err := r.tracingConfig.Apply()
	if err != nil {
		return fmt.Errorf("error applying tracing config: %w", err)
	}

	// Setup Metrics
	prometheusExporter, err := r.metricsConfig.Apply()
	if err != nil {
		return fmt.Errorf("error applying metrics config: %w", err)
	}

	// Setup HTTP Server
	server := r.serviceServerConfig.GetServer(prometheusExporter)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Println("error running http server")
		}
	}()

	// declaritively define the tract.
	myTract := tract.NewNamedLinker[request, parsedRequest, request](
		"process_messages",
		urfavtract.NewErrorWorkerFuncTract(r.parseWorkerConfig, parseRequest),
	).Link(tract.NewLinker[parsedRequest, processedRequest, request](
		urfavtract.NewBasicWorkerFuncTract(r.sqrtWorkerConfig, sqrtRequest),
	).Link(
		urfavtract.NewBasicWorkerFuncTract(r.writeWorkerConfig, func(req processedRequest) request {
			fmt.Printf("processed message :: parsed_number: %v, processed_number: %v\n", req.parsedNumber, req.processedNumber)
			return req.request
		}),
	))

	// TODO: this process could be simpler if the tract runner supported input/output factories.

	// Define the channel used to connect our kafka comsumer to the input of the tract.
	requests := make(chan request)

	// Init and start tract.
	tractStarter, err := myTract.Init(NewRequestLinks(requests))
	if err != nil {
		return fmt.Errorf("error initilizing tract: %w", err)
	}
	tractWaiter := tractStarter.Start()

	// Consume requests from kafka.
	consumeCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, unix.SIGTERM, unix.SIGINT)
	defer cancel()
	err = r.kafkaConsumerConfig.consume(consumeCtx, requests)
	if err != nil {
		return fmt.Errorf("error consuming: %w", err)
	}

	// Kafka consumer has stopped, wait for tract to finish.
	close(requests)
	tractWaiter.Wait()

	// Shutdown HTTP Server.
	err = server.Shutdown(context.Background())
	if err != nil {
		log.Println("error shutting down http server")
	}
	wg.Wait()
	return nil
}

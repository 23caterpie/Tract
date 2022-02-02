package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"golang.org/x/sys/unix"
	"os"

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
}

func newRunner() runner {
	return runner{
		kafkaConsumerConfig: newKafkaConsumerConfig(),
		// TODO: look into these not parsing.
		parseWorkerConfig:   urfavtract.NewWorkerTractConfig("parse"),
		sqrtWorkerConfig:    urfavtract.NewWorkerTractConfig("sqrt"),
		writeWorkerConfig:   urfavtract.NewWorkerTractConfig("write"),
	}
}

type runner struct {
	// TODO: add zipkin and promethius configs.
	kafkaConsumerConfig *kafkaConsumerConfig
	parseWorkerConfig   urfavtract.WorkerTractConfig
	sqrtWorkerConfig    urfavtract.WorkerTractConfig
	writeWorkerConfig   urfavtract.WorkerTractConfig
}

func (r *runner) flags() []cli.Flag {
	flagLists := [][]cli.Flag{
		r.kafkaConsumerConfig.flags(),
		r.parseWorkerConfig.Flags(),
		r.sqrtWorkerConfig.Flags(),
		r.writeWorkerConfig.Flags(),
	}
	flags := make([]cli.Flag, 0)
	for _, flagList := range flagLists {
		flags = append(flags, flagList...)
	}
	return flags
}

func (r runner) action(*cli.Context) error {
	fmt.Printf("config: %+#v\n", r)
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

	// Setup inputs and outputs of tract to use context from request.
	requests := make(chan request)
	input, output := tract.NewRequestWrapperLinks[request, request](
		tract.NewChannel(requests),
		newRequestOutput(),
	)
	input.BaseContext = func(req request) context.Context {
		return req.ctx
	}

	// Init and start tract.
	tractStarter, err := myTract.Init(input, output)
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
	return nil
}

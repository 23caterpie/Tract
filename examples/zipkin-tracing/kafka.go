package main

import (
	"context"
	"fmt"
	"log"

	tract "github.com/23caterpie/Tract"

	"github.com/Shopify/sarama"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/trace"
)

func newKafkaConsumerConfig() *kafkaConsumerConfig {
	return &kafkaConsumerConfig{
		config: sarama.NewConfig(),
	}
}

type kafkaConsumerConfig struct {
	brokers           cli.StringSlice
	consumerGroupName string
	config            *sarama.Config
	topics            cli.StringSlice
}

func (c *kafkaConsumerConfig) flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringSliceFlag{
			Name:        "kafka-brokers",
			EnvVars:     []string{"KAFKA_BROKERS"},
			Destination: &c.brokers,
			Required: true,
		},
		&cli.StringFlag{
			Name:        "kafka-consumer-group-name",
			EnvVars:     []string{"KAFKA_CONSUMER_GROUP_NAME"},
			Destination: &c.consumerGroupName,
			Required: true,
		},
		&cli.StringSliceFlag{
			Name:        "kafka-topics",
			EnvVars:     []string{"KAFKA_TOPICS"},
			Destination: &c.topics,
			Required: true,
		},
	}
}

func (c kafkaConsumerConfig) consume(ctx context.Context, requests chan<- request) error {
	consumerGroup, err := sarama.NewConsumerGroup(c.brokers.Value(), c.consumerGroupName, c.config)
	if err != nil {
		return fmt.Errorf("error making consumer group: %w", err)
	}

	var (
		handler = newConsumerGroupHandler(requests)
		topics  = c.topics.Value()
	)
	for {
		err = consumerGroup.Consume(ctx, topics, handler)
		if ctx.Err() != nil {
			// Stop reconnecting if the context was cancelled.
			return nil
		}
		if !isRetryableKafkaError(err) {
			log.Println("fatal error consuming:", err)
			return err
		} else {
			fmt.Println("retriable error consuming:", err)
		}
	}
}

func isRetryableKafkaError(e error) bool {
	switch e {
	case sarama.ErrInvalidMessage,
		sarama.ErrUnknownTopicOrPartition,
		sarama.ErrLeaderNotAvailable,
		sarama.ErrNotLeaderForPartition,
		sarama.ErrRequestTimedOut,
		sarama.ErrOffsetsLoadInProgress,
		sarama.ErrConsumerCoordinatorNotAvailable,
		sarama.ErrNotCoordinatorForConsumer,
		sarama.ErrNotEnoughReplicas,
		sarama.ErrNotEnoughReplicasAfterAppend,
		nil:
		return true
	}
	return false
}

type request struct {
	ctx     context.Context
	rawData []byte
	end     func()
}

func newConsumerGroupHandler(requests chan<- request) *consumerGroupHandler {
	return &consumerGroupHandler{
		requests: requests,
	}
}

type consumerGroupHandler struct {
	requests chan<- request
}

func (consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var ctx, span = trace.StartSpan(context.Background(), "base/kafka")

		span.AddAttributes(
			trace.StringAttribute("kafka.topic", msg.Topic),
			trace.Int64Attribute("kafka.partition", int64(msg.Partition)),
			trace.Int64Attribute("kafka.offset", msg.Offset),
		)

		sess.MarkMessage(msg, "")

		h.requests <- request{
			ctx:     ctx,
			rawData: msg.Value,
			end:     span.End,
		}
	}
	return nil
}

func newRequestOutput() tract.Output[request] {
	return requestOutput{}
}

type requestOutput struct{}

func (requestOutput) Put(req request) {
	req.end()
}

func (requestOutput) Close() {}
